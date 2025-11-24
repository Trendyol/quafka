package com.trendyol.quafka.examples.console.consumers.single

import com.trendyol.quafka.common.header
import com.trendyol.quafka.consumer.configuration.*
import com.trendyol.quafka.examples.console.consumers.ConsumerBase
import com.trendyol.quafka.examples.console.exceptions.*
import com.trendyol.quafka.extensions.delaying.MessageDelayer
import com.trendyol.quafka.extensions.errorHandling.configuration.subscribeWithErrorHandling
import com.trendyol.quafka.extensions.errorHandling.recoverer.*
import com.trendyol.quafka.producer.QuafkaProducer
import org.apache.kafka.common.serialization.StringDeserializer
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.minutes
import kotlin.time.Duration.Companion.seconds

/**
 * Consumer with advanced retry and error handling using `subscribeWithErrorHandling`.
 *
 * **What it shows:**
 * - Simplified retry configuration with `subscribeWithErrorHandling()`
 * - Multiple retry strategies (same as PipelinedConsumer)
 * - Exception-based retry policies:
 *   - Full retry (in-memory + non-blocking)
 *   - In-memory only retry
 *   - No retry
 * - Custom exception handling per exception type
 * - Message modification on retry (adding headers)
 * - Type-safe PolicyIdentifier usage
 * - DelayCalculator for simplified delay configuration
 *
 * **Retry policies:**
 * - **DatabaseConcurrencyException:** Full retry (3 in-memory + 10 non-blocking attempts)
 * - **DomainException:** No retry, straight to DLQ
 * - **Others:** In-memory only (3 attempts)
 *
 * **Use when:** Need simplified retry setup without manual middleware configuration.
 */
class RetryableConsumer(private val quafkaProducer: QuafkaProducer<String, String>, properties: MutableMap<String, Any>) :
    ConsumerBase(properties) {
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
        deadLetterTopic = "topic.v3.error",
        policyBuilder = {
            this.onException<Throwable> {
                this.config = InMemoryRetryConfig.basic(3, 10.milliseconds)
            }
        }
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
        val danglingTopic = "dangling.topic"

        val consumer = QuafkaConsumerBuilder<String, String>(properties)
            .withDeserializer(StringDeserializer())
            .subscribeWithErrorHandling(topics, danglingTopic, quafkaProducer) {
                this
                    .withRecoverable {
                        withExceptionDetailsProvider { throwable: Throwable -> ExceptionDetails(throwable, throwable.message!!) }
                            .withMessageDelayer(MessageDelayer())
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
                            }
                    }.withSingleMessageHandler { incomingMessage, consumerContext ->
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
                    }.withBackpressure(20)
                    .autoAckAfterProcess(true)
            }.build()

        consumer.start()
    }
}
