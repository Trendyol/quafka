package com.trendyol.quafka.examples.console.consumers.single

import com.trendyol.quafka.common.header
import com.trendyol.quafka.consumer.configuration.*
import com.trendyol.quafka.examples.console.consumers.ConsumerBase
import com.trendyol.quafka.examples.console.exceptions.*
import com.trendyol.quafka.extensions.errorHandling.configuration.subscribeWithErrorHandling
import com.trendyol.quafka.extensions.errorHandling.recoverer.*
import com.trendyol.quafka.producer.QuafkaProducer
import org.apache.kafka.common.serialization.StringDeserializer
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.minutes
import kotlin.time.Duration.Companion.seconds

/**
 * Demonstrates topic-level policy-based retry configuration for Kafka consumers.
 *
 * This example showcases how to configure exception-specific retry policies at the topic level,
 * allowing different topics to have different retry strategies while sharing common exception
 * handling logic. The policy-based approach provides fine-grained control over retry behavior
 * based on exception types.
 *
 * ## Key Features Demonstrated:
 * - **Topic-Level Policy Configuration**: Define retry policies per topic using `policyBuilder`
 * - **Reusable Exception Policies**: Share common exception handling across multiple topics
 * - **Dynamic Identifier Derivation**: Automatically derive policy identifiers from exception types at runtime
 * - **Multiple Retry Strategies**: Showcase all four retry strategies in one consumer
 * - **Exception-Specific Behavior**: Different retry logic for different exception types
 *
 * ## Retry Strategies Showcased:
 * 1. **Single Topic Retry** (topic1): Simple retry to a single topic
 * 2. **Exponential Backoff Multi-Topic** (topic2): Multiple retry topics with increasing delays
 * 3. **None Strategy** (topic3): No non-blocking retry, only in-memory
 * 4. **Exponential Backoff to Single Topic** (topic4): Delay topics forwarding to a single retry topic
 *
 * ## Exception Handling Strategy:
 * - **DatabaseConcurrencyException**: Full retry with both in-memory (3 attempts) and non-blocking (10 attempts)
 * - **DomainException**: No retry - immediately fail to dead letter topic
 * - **All Other Exceptions**: In-memory retry only (3 attempts) with dynamic identifier
 *
 * ## Usage:
 * ```kotlin
 * val producer = QuafkaProducerBuilder<String, String>(properties)
 *     .withSerializer(StringSerializer(), StringSerializer())
 *     .build()
 *
 * val consumer = TopicPolicyBasedRetryableConsumer(producer, consumerProperties)
 * consumer.run()
 * ```
 *
 * @property quafkaProducer The producer used for sending retry and error messages
 * @property properties Kafka consumer configuration properties
 *
 * @see TopicConfiguration
 * @see TopicRetryStrategy
 * @see RetryPolicy
 * @see PolicyIdentifiers
 */
class TopicPolicyBasedRetryableConsumer(private val quafkaProducer: QuafkaProducer<String, String>, properties: MutableMap<String, Any>) :
    ConsumerBase(properties) {

    fun TopicRetryStrategy<RetryPolicyBuilder.Full>.retryOnCommonErrors() {
        onException<DatabaseConcurrencyException>(PolicyIdentifiers.forException<DatabaseConcurrencyException>()) {
            this.inMemoryConfig = InMemoryRetryConfig.basic(maxAttempts = 3, initialDelay = 100.milliseconds)
            this.nonBlockingConfig = NonBlockingRetryConfig.exponentialRandomBackoff(
                maxAttempts = 10,
                initialDelay = 1.seconds,
                maxDelay = 50.seconds
            )
        }.onException<DomainException> {
            // don't retry
            dontRetry()
        }.onException<Throwable>(identifierFactory = { ex -> PolicyIdentifiers.forException(ex) }) {
            this.inMemoryConfig = InMemoryRetryConfig.basic(maxAttempts = 3, initialDelay = 100.milliseconds)
        }
    }

    val topic1 = TopicConfiguration(
        topic = "topic.v1",
        retry = TopicRetryStrategy.SingleTopicRetry(
            retryTopic = "topic.v1.retry",
            maxOverallAttempts = 3
        ),
        deadLetterTopic = "topic.v1.error",
        policyBuilder = {
            this.retryOnCommonErrors()
        }
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
        deadLetterTopic = "topic.v2.error",
        policyBuilder = {
            this.retryOnCommonErrors()
        }
    )
    val topic3 = TopicConfiguration(
        topic = "topic.v3",
        retry = TopicRetryStrategy.NoneStrategy,
        deadLetterTopic = "topic.v3.error",
        policyBuilder = {
            this.onException<Throwable> {
                this.config = InMemoryRetryConfig.basic(2, 10.milliseconds)
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
        deadLetterTopic = "topic.v4.error",
        policyBuilder = {
            this.retryOnCommonErrors()
        }
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
                        withExceptionDetailsProvider { throwable: Throwable -> ExceptionDetails(throwable, throwable.stackTraceToString()) }
                            .withOutgoingMessageModifier { message, consumerContext, exceptionDetails ->
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
