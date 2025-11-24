package com.trendyol.quafka.examples.console.consumers.single

import com.trendyol.quafka.common.header
import com.trendyol.quafka.consumer.configuration.QuafkaConsumerBuilder
import com.trendyol.quafka.consumer.configuration.withDeserializer
import com.trendyol.quafka.events.EventBus
import com.trendyol.quafka.examples.console.consumers.ConsumerBase
import com.trendyol.quafka.examples.console.exceptions.DatabaseConcurrencyException
import com.trendyol.quafka.examples.console.exceptions.DomainException
import com.trendyol.quafka.extensions.common.pipelines.*
import com.trendyol.quafka.extensions.consumer.single.pipelines.*
import com.trendyol.quafka.extensions.consumer.single.pipelines.PipelineMessageHandler.Companion.usePipelineMessageHandler
import com.trendyol.quafka.extensions.delaying.MessageDelayer
import com.trendyol.quafka.extensions.errorHandling.configuration.*
import com.trendyol.quafka.extensions.errorHandling.recoverer.*
import com.trendyol.quafka.extensions.errorHandling.recoverer.middlewares.RecoverableMessageExecutorMiddleware
import com.trendyol.quafka.producer.QuafkaProducer
import org.apache.kafka.common.serialization.StringDeserializer
import org.slf4j.Logger
import kotlin.system.measureTimeMillis
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.minutes
import kotlin.time.Duration.Companion.seconds

/**
 * Advanced consumer using pipeline/middleware architecture for message processing.
 *
 * **What it shows:**
 * - Middleware-based message processing (pipeline pattern)
 * - Multiple retry strategies per topic:
 *   - Single topic retry
 *   - Multi-topic exponential backoff
 *   - No retry strategy
 *   - Exponential backoff to single topic
 * - Custom middlewares (measure, logging)
 * - Error handling with recoverable message executor
 * - Dead letter queue (DLQ) handling
 * - Delayed message processing
 *
 * **Retry strategies:**
 * - **topic.v1:** Single retry topic with 3 attempts
 * - **topic.v2:** Exponential backoff across 4 delay topics (10s, 30s, 1m, 5m)
 * - **topic.v3:** No retry, straight to DLQ
 * - **topic.v4:** Exponential backoff delays, then single retry topic
 *
 * **Use when:** Need flexible error handling, retries, and middleware composition.
 */
class PipelinedConsumer(private val quafkaProducer: QuafkaProducer<String, String>, properties: MutableMap<String, Any>) :
    ConsumerBase(properties) {
    val danglingTopic = "dangling.topic"
    val messageDelayer = MessageDelayer()
    val eventBus = EventBus()

    val topic1 = TopicConfiguration(
        topic = "topic.v1",
        retry = TopicRetryStrategy.SingleTopicRetry(
            retryTopic = "topic.v1.retry",
            maxOverallAttempts = 3
        ),
        deadLetterTopic = "topic.v1.error"
    )
    val topic2 = TopicConfiguration(
        topic = "topic.v2",
        retry = TopicRetryStrategy.ExponentialBackoffMultiTopicRetry(
            maxOverallAttempts = 3,
            retryTopics = listOf(
                TopicRetryStrategy.DelayTopicConfiguration("topic.v2.retry.10sec", 10.seconds),
                TopicRetryStrategy.DelayTopicConfiguration("topic.v2.retry.30sec", 30.seconds),
                TopicRetryStrategy.DelayTopicConfiguration("topic.v2.retry.1min", 1.minutes),
                TopicRetryStrategy.DelayTopicConfiguration("topic.v2.retry.5min", 5.minutes)
            )
        ),
        deadLetterTopic = "topic.v2.error"
    )
    val topic3 = TopicConfiguration(
        topic = "topic.v3",
        retry = TopicRetryStrategy.NoneStrategy,
        deadLetterTopic = "topic.v3.error"
    )
    val topic4 = TopicConfiguration(
        topic = "topic.v4",
        retry = TopicRetryStrategy.ExponentialBackoffToSingleTopicRetry(
            maxOverallAttempts = 3,
            retryTopic = "topic.v4.retry",
            delayTopics = listOf(
                TopicRetryStrategy.DelayTopicConfiguration("topic.v4.delay.10sec", 10.seconds),
                TopicRetryStrategy.DelayTopicConfiguration("topic.v4.delay.30sec", 30.seconds),
                TopicRetryStrategy.DelayTopicConfiguration("topic.v4.delay.1min", 1.minutes),
                TopicRetryStrategy.DelayTopicConfiguration("topic.v4.delay.5min", 5.minutes)
            )
        ),
        deadLetterTopic = "topic.v4.error"
    )
    val topics = listOf(
        topic1,
        topic2,
        topic3,
        topic4
    )

    override suspend fun run() {
        val recoverableMessageExecutor = buildMessageExecutor()
        val consumer = QuafkaConsumerBuilder<String, String>(properties)
            .withDeserializer(StringDeserializer())
            .subscribeToDelayedTopics(topics, quafkaProducer, messageDelayer, danglingTopic)
            .subscribe(*topics.getAllSubscriptionTopics().toTypedArray()) {
                this
                    .usePipelineMessageHandler {
                        this
                            .useMiddleware(RecoverableMessageExecutorMiddleware(recoverableMessageExecutor))
                            .useMiddleware(MeasureMiddleware(logger))
                            .use { envelope: SingleMessageEnvelope<String, String>, next ->
                                val incomingMessage = envelope.message
                                when {
                                    topic1.isSuitableTopic(incomingMessage.topic) -> {
                                        // process topic.v1 and topic.v1.retry
                                    }

                                    topic2.isSuitableTopic(incomingMessage.topic) -> {
                                        // process topic.v2 and topic.v2.retry.10sec, topic.v2.retry.30sec, topic.v2.retry.1min, topic.v2.retry.5min
                                    }

                                    topic3.isSuitableTopic(incomingMessage.topic) -> {
                                        // process topic.v3
                                    }

                                    topic4.isSuitableTopic(incomingMessage.topic) -> {
                                        // process topic.v4 and topic.v4.retry
                                    }
                                }
                            }
                    }
                    .withBackpressure(20)
                    .autoAckAfterProcess(true)
            }.build()

        consumer.start()
    }

    private fun buildMessageExecutor(): RecoverableMessageExecutor<String, String> = RecoverableMessageExecutorBuilder(
        TopicResolver(topics),
        danglingTopic,
        quafkaProducer,
        messageDelayer
    ).withExceptionDetailsProvider { throwable: Throwable -> ExceptionDetails(throwable, throwable.message!!) }
        .withMessageDelayer(messageDelayer)
        .withPolicyProvider { message, consumerContext, exceptionDetails, _ ->
            when (exceptionDetails.exception) {
                is DatabaseConcurrencyException -> RetryPolicy.FullRetry(
                    identifier = PolicyIdentifier("DatabaseConcurrencyException"),
                    inMemoryConfig = InMemoryRetryConfig.basic(maxAttempts = 3, initialDelay = 100.milliseconds),
                    nonBlockingConfig = NonBlockingRetryConfig.exponentialRandomBackoff(
                        maxAttempts = 10,
                        initialDelay = 1.seconds,
                        maxDelay = 50.seconds
                    )
                )

                is DomainException -> RetryPolicy.NoRetry

                else -> RetryPolicy.InMemory(
                    identifier = PolicyIdentifiers.forException(exceptionDetails.exception),
                    config = InMemoryRetryConfig.basic(maxAttempts = 3, initialDelay = 100.milliseconds)
                )
            }
        }.withOutgoingMessageModifier { message, consumerContext, exceptionDetails ->
            val newHeaders = this.headers.toMutableList()
            newHeaders.add(header("X-Handled", true))
            this.copy(headers = newHeaders)
        }.build()
}

class MeasureMiddleware<TEnvelope, TKey, TValue>(private val logger: Logger) :
    SingleMessageBaseMiddleware<TEnvelope, TKey, TValue>() where TEnvelope : SingleMessageEnvelope<TKey, TValue> {
    override suspend fun execute(
        envelope: TEnvelope,
        next: MiddlewareFn<TEnvelope>
    ) {
        val ms = measureTimeMillis {
            next(envelope)
        }
        logger.info("Message took $ms ms")
    }
}
