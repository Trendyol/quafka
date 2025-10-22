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
 * Instead of handling the error in-place, it enriches the message with relevant metadata (e.g., error details,
 * attempt counts) and forwards it to other Kafka topics. This approach enhances system resilience and
 * improves the traceability of failures.
 *
 * @param TKey The type of the Kafka message key.
 * @param TValue The type of the Kafka message value.
 * @property exceptionDetailsProvider A function that provides enriched error details from the thrown exception.
 * @property topicResolver A component that finds the relevant topic configuration for an incoming message.
 * @property danglingTopic A fallback topic for failed messages from unconfigured source topics.
 * @property quafkaProducer The Kafka producer client used to send the message to its next destination (e.g., retry or error topic).
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

        val exceptionReport = exceptionDetailsProvider(exception)
        logger
            .atWarn()
            .enrichWithConsumerContext(consumerContext)
            .setCause(exception)
            .log(
                "Error occurred when processing message, error will be handled... | exception detail: {} | {}",
                exceptionReport.detail,
                incomingMessage.toString(logLevel = Level.WARN, addValue = false, addHeaders = true)
            )
        when (val resolvedTopic = topicResolver.resolve(incomingMessage)) {
            is TopicResolver.Topic.ResolvedTopic -> handleKnownTopic(
                resolvedTopic.configuration,
                incomingMessage,
                consumerContext,
                exceptionReport
            )

            TopicResolver.Topic.UnknownTopic -> forwardToDanglingTopic(
                incomingMessage,
                consumerContext,
                exceptionReport
            )
        }
    }

    private suspend fun handleKnownTopic(
        topicConfiguration: TopicConfiguration,
        incomingMessage: IncomingMessage<TKey, TValue>,
        consumerContext: ConsumerContext,
        exceptionDetails: ExceptionDetails
    ) {
        if (topicConfiguration.retry == TopicConfiguration.TopicRetryStrategy.NoneStrategy) {
            forwardToErrorTopic(topicConfiguration, incomingMessage, consumerContext, exceptionDetails)
            return
        }

        when (val policy = policyProvider(incomingMessage, consumerContext, exceptionDetails)) {
            is RetryPolicy.FullRetry -> handleRetry(
                topicConfiguration,
                incomingMessage,
                consumerContext,
                exceptionDetails,
                policy.nonBlockingConfig,
                policy.identifier
            )

            is RetryPolicy.InMemoryOnly, RetryPolicy.NoRetry -> {
                forwardToErrorTopic(
                    topicConfiguration,
                    incomingMessage,
                    consumerContext,
                    exceptionDetails
                )
            }

            is RetryPolicy.NonBlockingOnly -> handleRetry(
                topicConfiguration,
                incomingMessage,
                consumerContext,
                exceptionDetails,
                policy.config,
                policy.identifier
            )
        }
    }

    private suspend fun handleRetry(
        topicConfiguration: TopicConfiguration,
        incomingMessage: IncomingMessage<TKey, TValue>,
        consumerContext: ConsumerContext,
        exceptionDetails: ExceptionDetails,
        nonBlockingRetryConfig: NonBlockingRetryConfig,
        policyIdentifier: String
    ) = when (
        val action =
            determineRetry(
                incomingMessage,
                consumerContext,
                topicConfiguration,
                nonBlockingRetryConfig,
                policyIdentifier,
                exceptionDetails
            )
    ) {
        RetryOutcome.Error -> forwardToErrorTopic(
            topicConfiguration,
            incomingMessage,
            consumerContext,
            exceptionDetails
        )

        is RetryOutcome.Retry ->
            forwardToRetryTopic(consumerContext, exceptionDetails, action, incomingMessage)

        is RetryOutcome.Forward -> forwardToDelayTopic(
            consumerContext,
            exceptionDetails,
            action,
            incomingMessage
        )
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
            .addRetryAttempt(action.nextAttempt, action.overallAttempt, action.retryIdentifier)
            .clearDelay()
            .apply {
                if (action.delay > Duration.ZERO) {
                    this.withDelay(DelayStrategy.FOR_FIXED_DURATION, action.delay, now)
                }
            }

        val outgoingMessage = this.outgoingMessageModifier(
            OutgoingMessage.create(
                topic = action.delayTopic,
                partition = null,
                key = incomingMessage.key,
                value = incomingMessage.value,
                headers = headers
            ),
            incomingMessage,
            consumerContext,
            exceptionDetails
        )
        quafkaProducer.send(
            outgoingMessage
        )
        consumerContext.consumerOptions.eventBus.publish(
            RecovererEvent.DelayMessagePublished(
                message = incomingMessage,
                exceptionDetails = exceptionDetails,
                outgoingMessage = outgoingMessage,
                consumerContext = consumerContext,
                attempt = action.nextAttempt,
                overallAttempt = action.overallAttempt,
                identifier = action.retryIdentifier,
                delay = action.delay,
                forwardingTopic = action.retryTopic
            )
        )
    }

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
            .addRetryAttempt(action.nextAttempt, action.overallAttempt, action.retryIdentifier)
            .clearDelay()
            .apply {
                if (action.delay > Duration.ZERO) {
                    this.withDelay(DelayStrategy.FOR_FIXED_DURATION, action.delay, now)
                }
            }

        val outgoingMessage = outgoingMessageModifier(
            OutgoingMessage.create(
                topic = action.retryTopic,
                partition = null,
                key = incomingMessage.key,
                value = incomingMessage.value,
                headers = headers
            ),
            incomingMessage,
            consumerContext,
            exceptionDetails
        )
        quafkaProducer.send(
            outgoingMessage
        )
        consumerContext.consumerOptions.eventBus.publish(
            RecovererEvent.RetryMessagePublished(
                message = incomingMessage,
                exceptionDetails = exceptionDetails,
                outgoingMessage = outgoingMessage,
                consumerContext = consumerContext,
                attempt = action.nextAttempt,
                overallAttempt = action.overallAttempt,
                identifier = action.retryIdentifier,
                delay = action.delay
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

        val outgoingMessage = outgoingMessageModifier(
            OutgoingMessage
                .create(
                    topic = danglingTopic,
                    partition = null,
                    key = incomingMessage.key,
                    value = incomingMessage.value,
                    headers = headers
                ),
            incomingMessage,
            consumerContext,
            exceptionDetails
        )
        quafkaProducer.send(
            outgoingMessage
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

    private suspend fun forwardToErrorTopic(
        topicConfiguration: TopicConfiguration,
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
        val outgoingMessage = outgoingMessageModifier(
            OutgoingMessage
                .create(
                    topic = topicConfiguration.deadLetterTopic,
                    partition = null,
                    key = incomingMessage.key,
                    value = incomingMessage.value,
                    headers = incomingMessage.headers
                        .toMutableList()
                        .addException(exceptionDetails, now)
                        .addOriginTopicInformation(incomingMessage, now)
                        .addPublishedAt(now)
                        .clearDelay()
                ),
            incomingMessage,
            consumerContext,
            exceptionDetails
        )
        quafkaProducer.send(
            outgoingMessage
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

    private sealed class RetryOutcome {
        data object Error : RetryOutcome()

        data class Retry(
            val retryTopic: String,
            val delay: Duration,
            val nextAttempt: Int,
            val overallAttempt: Int,
            val retryIdentifier: String
        ) : RetryOutcome()

        data class Forward(
            val delayTopic: String,
            val retryTopic: String,
            val delay: Duration,
            val nextAttempt: Int,
            val overallAttempt: Int,
            val retryIdentifier: String
        ) : RetryOutcome()
    }

    private fun determineRetry(
        incomingMessage: IncomingMessage<*, *>,
        consumerContext: ConsumerContext,
        config: TopicConfiguration,
        policy: NonBlockingRetryConfig,
        policyIdentifier: String,
        exceptionDetails: ExceptionDetails
    ): RetryOutcome {
        val currentIdentifier = incomingMessage.headers.getRetryIdentifier()
        val currentAttempt = if (policyIdentifier == currentIdentifier) {
            incomingMessage.headers.getRetryAttemptOrDefault()
        } else {
            0
        }
        val nextAttempt = currentAttempt + 1

        val currentOverall = incomingMessage.headers.getOverallRetryAttemptOrDefault()
        val nextOverall = currentOverall + 1

        // exceed per-retry limit?
        if (nextAttempt > policy.maxAttempts) {
            return RetryOutcome.Error
        }

        val calculatedDelay = policy.calculateDelay(
            message = incomingMessage,
            consumerContext = consumerContext,
            exceptionDetails = exceptionDetails,
            attempt = nextOverall
        )

        return when (val strategy = config.retry) {
            is TopicConfiguration.TopicRetryStrategy.SingleTopicRetry -> {
                if (nextOverall > strategy.maxTotalRetryAttempts) {
                    return RetryOutcome.Error
                }
                RetryOutcome.Retry(
                    retryTopic = strategy.retryTopic,
                    delay = calculatedDelay,
                    nextAttempt = nextAttempt,
                    overallAttempt = nextOverall,
                    retryIdentifier = policyIdentifier
                )
            }

            is TopicConfiguration.TopicRetryStrategy.ExponentialBackoffMultiTopicRetry -> {
                if (nextOverall > strategy.maxTotalRetryAttempts) {
                    return RetryOutcome.Error
                }
                if (logger.isDebugEnabled) {
                    logger
                        .atDebug()
                        .enrichWithConsumerContext(consumerContext)
                        .log(
                            "Backoff delay | $nextOverall = $calculatedDelay | message = {} ",
                            incomingMessage.toString(Level.DEBUG, addValue = false, addHeaders = true)
                        )
                }

                val bucket = strategy.findBucket(calculatedDelay)
                if (bucket != null) {
                    RetryOutcome.Retry(
                        retryTopic = bucket.topic,
                        delay = calculatedDelay,
                        nextAttempt = nextAttempt,
                        overallAttempt = nextOverall,
                        retryIdentifier = policyIdentifier
                    )
                } else {
                    logger
                        .atWarn()
                        .enrichWithConsumerContext(consumerContext)
                        .log(
                            "No retry bucket for delay. | calculated delay = $calculatedDelay | message = {} ",
                            incomingMessage.toString(Level.WARN, addValue = false, addHeaders = true)
                        )
                    RetryOutcome.Error
                }
            }

            is TopicConfiguration.TopicRetryStrategy.ExponentialBackoffToSingleTopicRetry -> {
                if (nextOverall > strategy.maxTotalRetryAttempts) {
                    return RetryOutcome.Error
                }
                if (logger.isDebugEnabled) {
                    logger
                        .atDebug()
                        .enrichWithConsumerContext(consumerContext)
                        .log(
                            "Backoff delay | $nextOverall = $calculatedDelay | message = {} ",
                            incomingMessage.toString(Level.DEBUG, addValue = false, addHeaders = true)
                        )
                }

                val bucket = strategy.findBucket(calculatedDelay)
                if (bucket != null) {
                    RetryOutcome.Forward(
                        delayTopic = bucket.topic,
                        retryTopic = strategy.retryTopic,
                        delay = calculatedDelay,
                        nextAttempt = nextAttempt,
                        overallAttempt = nextOverall,
                        retryIdentifier = policyIdentifier
                    )
                } else {
                    if (logger.isDebugEnabled) {
                        logger
                            .atDebug()
                            .enrichWithConsumerContext(consumerContext)
                            .log(
                                "No retry bucket for delay | calculated delay = $calculatedDelay | message = {} ",
                                incomingMessage.toString(Level.DEBUG, addValue = false, addHeaders = true)
                            )
                    }
                    RetryOutcome.Error
                }
            }

            TopicConfiguration.TopicRetryStrategy.NoneStrategy ->
                RetryOutcome.Error
        }
    }
}
