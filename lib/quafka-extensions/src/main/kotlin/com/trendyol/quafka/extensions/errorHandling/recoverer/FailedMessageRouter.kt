package com.trendyol.quafka.extensions.errorHandling.recoverer

import com.trendyol.quafka.common.rethrowIfFatalOrCancelled
import com.trendyol.quafka.consumer.*
import com.trendyol.quafka.extensions.delaying.DelayHeaders.clearDelay
import com.trendyol.quafka.extensions.delaying.DelayHeaders.withDelay
import com.trendyol.quafka.extensions.delaying.DelayStrategy
import com.trendyol.quafka.logging.*
import com.trendyol.quafka.producer.*
import org.slf4j.Logger
import org.slf4j.event.Level
import kotlin.time.Duration

/**
 * A central handler for managing exceptions that occur during Kafka message consumption.
 *
 * The primary purpose of this class is to intercept failures during message processing, prevent message loss,
 * and apply configured recovery strategies such as retry, delayed retry, and dead-lettering.
 *
 * @param TKey The type of the Kafka message key.
 * @param TValue The type of the Kafka message value.
 * @property exceptionDetailsProvider Provides enriched error details from thrown exceptions
 * @property topicResolver Finds the relevant topic configuration for messages
 * @property danglingTopic Fallback topic for messages from unconfigured topics
 * @property quafkaProducer Kafka producer for sending messages to destinations
 * @property policyProvider Determines the retry policy based on message and exception
 * @property outgoingMessageModifier Allows modification of outgoing messages before sending
 */
class FailedMessageRouter<TKey, TValue>(
    private val exceptionDetailsProvider: ExceptionDetailsProvider,
    private val topicResolver: TopicResolver,
    private val danglingTopic: String,
    private val quafkaProducer: QuafkaProducer<TKey, TValue>,
    private val policyProvider: RetryPolicyProvider,
    private val outgoingMessageModifier: OutgoingMessageModifierDelegate<TKey, TValue>
) {
    private val logger: Logger = LoggerHelper.createLogger(clazz = FailedMessageRouter::class.java)

    suspend fun handle(
        incomingMessage: IncomingMessage<TKey, TValue>,
        consumerContext: ConsumerContext,
        exception: Throwable
    ) {
        exception.rethrowIfFatalOrCancelled()

        val exceptionDetails = exceptionDetailsProvider(exception)
        logger
            .atWarn()
            .enrichWithConsumerContext(consumerContext)
            .setCause(exception)
            .log(
                "Error occurred when processing message, error will be handled... | exception detail: {} | {}",
                exceptionDetails.detail,
                incomingMessage.toString(logLevel = Level.WARN, addValue = false, addHeaders = true)
            )

        when (val resolvedTopic = topicResolver.resolve(incomingMessage)) {
            is TopicResolver.Topic.ResolvedTopic -> handleKnownTopic(
                resolvedTopic.configuration,
                incomingMessage,
                consumerContext,
                exceptionDetails
            )

            TopicResolver.Topic.UnknownTopic -> forwardToDanglingTopic(
                incomingMessage,
                consumerContext,
                exceptionDetails
            )
        }
    }

    private suspend fun handleKnownTopic(
        topicConfiguration: TopicConfiguration<*>,
        incomingMessage: IncomingMessage<TKey, TValue>,
        consumerContext: ConsumerContext,
        exceptionDetails: ExceptionDetails
    ) {
        if (topicConfiguration.retry == TopicRetryStrategy.NoneStrategy) {
            forwardToErrorTopic(topicConfiguration, incomingMessage, consumerContext, exceptionDetails)
            return
        }

        when (val policy = policyProvider(incomingMessage, consumerContext, exceptionDetails, topicConfiguration)) {
            is RetryPolicy.InMemory, RetryPolicy.NoRetry -> forwardToErrorTopic(
                topicConfiguration,
                incomingMessage,
                consumerContext,
                exceptionDetails
            )

            is RetryPolicy.FullRetry -> handleRetryPolicy(
                topicConfiguration,
                incomingMessage,
                consumerContext,
                exceptionDetails,
                policy.nonBlockingConfig,
                policy.identifier
            )

            is RetryPolicy.NonBlocking -> handleRetryPolicy(
                topicConfiguration,
                incomingMessage,
                consumerContext,
                exceptionDetails,
                policy.config,
                policy.identifier
            )
        }
    }

    private suspend fun handleRetryPolicy(
        topicConfiguration: TopicConfiguration<*>,
        incomingMessage: IncomingMessage<TKey, TValue>,
        consumerContext: ConsumerContext,
        exceptionDetails: ExceptionDetails,
        nonBlockingRetryConfig: NonBlockingRetryConfig,
        policyIdentifier: PolicyIdentifier
    ) {
        val retryDecision = determineRetry(
            incomingMessage,
            consumerContext,
            topicConfiguration,
            nonBlockingRetryConfig,
            policyIdentifier,
            exceptionDetails
        )

        when (retryDecision) {
            RetryOutcome.Error -> forwardToErrorTopic(
                topicConfiguration,
                incomingMessage,
                consumerContext,
                exceptionDetails
            )

            is RetryOutcome.Retry -> forwardToRetryTopic(
                consumerContext,
                exceptionDetails,
                retryDecision,
                incomingMessage
            )

            is RetryOutcome.Forward -> forwardToDelayTopic(
                consumerContext,
                exceptionDetails,
                retryDecision,
                incomingMessage
            )
        }
    }

    private fun determineRetry(
        incomingMessage: IncomingMessage<*, *>,
        consumerContext: ConsumerContext,
        config: TopicConfiguration<*>,
        policy: NonBlockingRetryConfig,
        policyIdentifier: PolicyIdentifier,
        exceptionDetails: ExceptionDetails
    ): RetryOutcome {
        val attempts = calculateRetryAttempts(incomingMessage, policyIdentifier)
        if (attempts.nextAttempt > policy.maxAttempts) {
            return RetryOutcome.Error
        }
        val calculatedDelay = policy.calculateDelay(
            message = incomingMessage,
            consumerContext = consumerContext,
            exceptionDetails = exceptionDetails,
            attempt = attempts.nextOverall
        )
        return when (config.retry) {
            TopicRetryStrategy.NoneStrategy -> RetryOutcome.Error

            is TopicRetryStrategy.SingleTopicRetry ->
                applySingleTopicStrategy(config.retry, calculatedDelay, attempts, policyIdentifier)

            is TopicRetryStrategy.ExponentialBackoffMultiTopicRetry ->
                applyMultiTopicStrategy(config.retry, calculatedDelay, attempts, policyIdentifier, incomingMessage, consumerContext)

            is TopicRetryStrategy.ExponentialBackoffToSingleTopicRetry ->
                applyBackoffToSingleStrategy(config.retry, calculatedDelay, attempts, policyIdentifier, incomingMessage, consumerContext)
        }
    }

    /**
     * Calculates the current and next retry attempt numbers.
     * Per-policy attempts reset when the policy identifier changes.
     */
    private fun calculateRetryAttempts(
        incomingMessage: IncomingMessage<*, *>,
        policyIdentifier: PolicyIdentifier
    ): RetryAttempts {
        val currentIdentifier = incomingMessage.headers.getRetryPolicyIdentifier()
        val currentAttempt = if (policyIdentifier == currentIdentifier) {
            incomingMessage.headers.getRetryAttemptOrDefault()
        } else {
            0 // Reset per-policy counter if policy changed
        }

        val currentOverall = incomingMessage.headers.getOverallRetryAttemptOrDefault()

        return RetryAttempts(
            currentAttempt = currentAttempt,
            nextAttempt = currentAttempt + 1,
            currentOverall = currentOverall,
            nextOverall = currentOverall + 1
        )
    }

    private fun applySingleTopicStrategy(
        strategy: TopicRetryStrategy.SingleTopicRetry,
        delay: Duration,
        attempts: RetryAttempts,
        policyIdentifier: PolicyIdentifier
    ): RetryOutcome {
        if (attempts.nextOverall > strategy.maxOverallAttempts) {
            return RetryOutcome.Error
        }

        return RetryOutcome.Retry(
            retryTopic = strategy.retryTopic,
            delay = delay,
            nextAttempt = attempts.nextAttempt,
            overallAttempt = attempts.nextOverall,
            policyIdentifier = policyIdentifier
        )
    }

    private fun applyMultiTopicStrategy(
        strategy: TopicRetryStrategy.ExponentialBackoffMultiTopicRetry,
        delay: Duration,
        attempts: RetryAttempts,
        policyIdentifier: PolicyIdentifier,
        incomingMessage: IncomingMessage<*, *>,
        consumerContext: ConsumerContext
    ): RetryOutcome {
        if (attempts.nextOverall > strategy.maxOverallAttempts) {
            return RetryOutcome.Error
        }

        val bucket = findBucket(consumerContext, attempts, delay, incomingMessage, strategy)
        return RetryOutcome.Retry(
            retryTopic = bucket.topic,
            delay = delay,
            nextAttempt = attempts.nextAttempt,
            overallAttempt = attempts.nextOverall,
            policyIdentifier = policyIdentifier
        )
    }

    private fun applyBackoffToSingleStrategy(
        strategy: TopicRetryStrategy.ExponentialBackoffToSingleTopicRetry,
        delay: Duration,
        attempts: RetryAttempts,
        policyIdentifier: PolicyIdentifier,
        incomingMessage: IncomingMessage<*, *>,
        consumerContext: ConsumerContext
    ): RetryOutcome {
        if (attempts.nextOverall > strategy.maxOverallAttempts) {
            return RetryOutcome.Error
        }

        val bucket = findBucket(consumerContext, attempts, delay, incomingMessage, strategy)
        return RetryOutcome.Forward(
            delayTopic = bucket.topic,
            retryTopic = strategy.retryTopic,
            delay = delay,
            nextAttempt = attempts.nextAttempt,
            overallAttempt = attempts.nextOverall,
            policyIdentifier = policyIdentifier
        )
    }

    private fun findBucket(
        consumerContext: ConsumerContext,
        attempts: RetryAttempts,
        delay: Duration,
        incomingMessage: IncomingMessage<*, *>,
        strategy: TopicRetryStrategy.MultiTopicRetry
    ): TopicRetryStrategy.DelayTopicConfiguration {
        if (logger.isDebugEnabled) {
            logger
                .atDebug()
                .enrichWithConsumerContext(consumerContext)
                .log(
                    "Backoff delay | attempts: $attempts | delay: $delay | message: {} ",
                    incomingMessage.toString(Level.DEBUG, addValue = false, addHeaders = true)
                )
        }
        val bucket = strategy.findBucket(delay)
        if (bucket.maxDelay < delay) {
            logger
                .atWarn()
                .enrichWithConsumerContext(consumerContext)
                .log(
                    "Calculated delay ($delay) exceeds max bucket threshold (${bucket.maxDelay}). " +
                        "Using max bucket '${bucket.topic}' as fallback. | message = {} ",
                    incomingMessage.toString(Level.WARN, addValue = false, addHeaders = true)
                )
        }
        return bucket
    }

    private suspend fun forwardToDelayTopic(
        consumerContext: ConsumerContext,
        exceptionDetails: ExceptionDetails,
        action: RetryOutcome.Forward,
        incomingMessage: IncomingMessage<TKey, TValue>
    ) {
        logger
            .atWarn()
            .enrichWithConsumerContext(consumerContext)
            .setCause(exceptionDetails.exception)
            .log(
                "Error occurred when consuming retried kafka message, " +
                    "message will be delayed in {} then forward to {} delay topic. | exception detail: {} | overall attempt: {} | {}",
                action.delayTopic,
                action.retryTopic,
                exceptionDetails.detail,
                action.overallAttempt,
                incomingMessage.toString(Level.WARN, addHeaders = true)
            )

        val now = consumerContext.consumerOptions.timeProvider.now()
        val headers = incomingMessage.headers
            .toMutableList()
            .addException(exceptionDetails, now)
            .forwardingTopic(action.retryTopic)
            .addOriginTopicInformation(incomingMessage, now)
            .addPublishedAt(now)
            .addRetryAttempt(action.nextAttempt, action.overallAttempt, action.policyIdentifier)
            .clearDelay()
            .apply {
                if (action.delay > Duration.ZERO) {
                    this.withDelay(DelayStrategy.FOR_FIXED_DURATION, action.delay, now)
                }
            }

        val outgoingMessage = createAndSendMessage(
            topic = action.delayTopic,
            incomingMessage = incomingMessage,
            headers = headers,
            consumerContext = consumerContext,
            exceptionDetails = exceptionDetails
        )

        consumerContext.consumerOptions.eventBus.publish(
            RecovererEvent.DelayMessagePublished(
                message = incomingMessage,
                exceptionDetails = exceptionDetails,
                outgoingMessage = outgoingMessage,
                consumerContext = consumerContext,
                attempt = action.nextAttempt,
                overallAttempt = action.overallAttempt,
                policyIdentifier = action.policyIdentifier,
                delay = action.delay,
                forwardingTopic = action.retryTopic
            )
        )
    }

    /**
     * Forwards message directly to a retry topic with optional delay.
     */
    private suspend fun forwardToRetryTopic(
        consumerContext: ConsumerContext,
        exceptionDetails: ExceptionDetails,
        action: RetryOutcome.Retry,
        incomingMessage: IncomingMessage<TKey, TValue>
    ) {
        logger
            .atWarn()
            .enrichWithConsumerContext(consumerContext)
            .setCause(exceptionDetails.exception)
            .log(
                "Error occurred when consuming retried kafka message. | exception detail: {} | forwarding to: {} | overall attempt: {} | {}",
                exceptionDetails.detail,
                action.retryTopic,
                action.overallAttempt,
                incomingMessage.toString(Level.WARN, addHeaders = true)
            )

        val now = consumerContext.consumerOptions.timeProvider.now()
        val headers = incomingMessage.headers
            .toMutableList()
            .addException(exceptionDetails, now)
            .addOriginTopicInformation(incomingMessage, now)
            .addPublishedAt(now)
            .addRetryAttempt(action.nextAttempt, action.overallAttempt, action.policyIdentifier)
            .clearDelay()
            .apply {
                if (action.delay > Duration.ZERO) {
                    this.withDelay(DelayStrategy.FOR_FIXED_DURATION, action.delay, now)
                }
            }

        val outgoingMessage = createAndSendMessage(
            topic = action.retryTopic,
            incomingMessage = incomingMessage,
            headers = headers,
            consumerContext = consumerContext,
            exceptionDetails = exceptionDetails
        )

        consumerContext.consumerOptions.eventBus.publish(
            RecovererEvent.RetryMessagePublished(
                message = incomingMessage,
                exceptionDetails = exceptionDetails,
                outgoingMessage = outgoingMessage,
                consumerContext = consumerContext,
                attempt = action.nextAttempt,
                overallAttempt = action.overallAttempt,
                policyIdentifier = action.policyIdentifier,
                delay = action.delay
            )
        )
    }

    private suspend fun forwardToErrorTopic(
        topicConfiguration: TopicConfiguration<*>,
        incomingMessage: IncomingMessage<TKey, TValue>,
        consumerContext: ConsumerContext,
        exceptionDetails: ExceptionDetails
    ) {
        logger
            .atError()
            .enrichWithConsumerContext(consumerContext)
            .setCause(exceptionDetails.exception)
            .log(
                "Not retryable error occurred when consuming, forwarding to error topic: {} | exception detail: {} | {} ",
                topicConfiguration.deadLetterTopic,
                exceptionDetails.detail,
                incomingMessage.toString(Level.ERROR, addValue = true, addHeaders = true)
            )

        val now = consumerContext.consumerOptions.timeProvider.now()
        val headers = incomingMessage.headers
            .toMutableList()
            .addException(exceptionDetails, now)
            .addOriginTopicInformation(incomingMessage, now)
            .addPublishedAt(now)
            .clearDelay()

        val outgoingMessage = createAndSendMessage(
            topic = topicConfiguration.deadLetterTopic,
            incomingMessage = incomingMessage,
            headers = headers,
            consumerContext = consumerContext,
            exceptionDetails = exceptionDetails
        )

        consumerContext.consumerOptions.eventBus.publish(
            RecovererEvent.ErrorMessagePublished(
                message = incomingMessage,
                exceptionDetails = exceptionDetails,
                outgoingMessage = outgoingMessage,
                consumerContext = consumerContext
            )
        )
    }

    private suspend fun forwardToDanglingTopic(
        incomingMessage: IncomingMessage<TKey, TValue>,
        consumerContext: ConsumerContext,
        exceptionDetails: ExceptionDetails
    ) {
        logger
            .atError()
            .enrichWithConsumerContext(consumerContext)
            .setCause(exceptionDetails.exception)
            .log(
                "CRITICAL : Not retryable error occurred when consuming kafka message, forwarding to dangling topic because error topic not found. | exception detail = {} | topic = {} | message = {} ",
                exceptionDetails.detail,
                danglingTopic,
                incomingMessage.toString(Level.ERROR, addValue = false, addHeaders = true)
            )

        val now = consumerContext.consumerOptions.timeProvider.now()
        val headers = incomingMessage.headers
            .toMutableList()
            .addException(exceptionDetails, now)
            .addOriginTopicInformation(incomingMessage, now)
            .addPublishedAt(now)
            .clearDelay()

        val outgoingMessage = createAndSendMessage(
            topic = danglingTopic,
            incomingMessage = incomingMessage,
            headers = headers,
            consumerContext = consumerContext,
            exceptionDetails = exceptionDetails
        )

        consumerContext.consumerOptions.eventBus.publish(
            RecovererEvent.DanglingMessagePublished(
                message = incomingMessage,
                exceptionDetails = exceptionDetails,
                outgoingMessage = outgoingMessage,
                consumerContext = consumerContext
            )
        )
    }

    private suspend fun createAndSendMessage(
        topic: String,
        incomingMessage: IncomingMessage<TKey, TValue>,
        headers: List<org.apache.kafka.common.header.Header>,
        consumerContext: ConsumerContext,
        exceptionDetails: ExceptionDetails
    ): OutgoingMessage<TKey, TValue> {
        val outgoingMessage = outgoingMessageModifier(
            OutgoingMessage.create(
                topic = topic,
                partition = null,
                key = incomingMessage.key,
                value = incomingMessage.value,
                headers = headers
            ),
            incomingMessage,
            consumerContext,
            exceptionDetails
        )

        quafkaProducer.send(outgoingMessage)
        return outgoingMessage
    }

    private sealed class RetryOutcome {
        data object Error : RetryOutcome()

        data class Retry(
            val retryTopic: String,
            val delay: Duration,
            val nextAttempt: Int,
            val overallAttempt: Int,
            val policyIdentifier: PolicyIdentifier
        ) : RetryOutcome()

        data class Forward(
            val delayTopic: String,
            val retryTopic: String,
            val delay: Duration,
            val nextAttempt: Int,
            val overallAttempt: Int,
            val policyIdentifier: PolicyIdentifier
        ) : RetryOutcome()
    }

    private data class RetryAttempts(val currentAttempt: Int, val nextAttempt: Int, val currentOverall: Int, val nextOverall: Int)
}
