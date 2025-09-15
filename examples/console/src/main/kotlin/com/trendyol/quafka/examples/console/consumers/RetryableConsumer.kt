package com.trendyol.quafka.examples.console.consumers

import com.trendyol.quafka.common.header
import com.trendyol.quafka.consumer.configuration.QuafkaConsumerBuilder
import com.trendyol.quafka.examples.console.DomainException
import com.trendyol.quafka.examples.console.exceptions.DatabaseConcurrencyException
import com.trendyol.quafka.extensions.delaying.MessageDelayer
import com.trendyol.quafka.extensions.errorHandling.configuration.subscribeWithErrorHandling
import com.trendyol.quafka.extensions.errorHandling.recoverer.*
import com.trendyol.quafka.producer.QuafkaProducer
import org.apache.kafka.common.serialization.StringDeserializer
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.minutes
import kotlin.time.Duration.Companion.seconds

class RetryableConsumer(
    private val quafkaProducer: QuafkaProducer<String, String>,
    servers: String
) : ConsumerBase(servers) {
    val topic1 = TopicConfiguration(
        topic = "topic.v1",
        retry = TopicConfiguration.TopicRetryStrategy.SingleTopicRetry(
            retryTopic = "topic.v1.retry",
            maxTotalRetryAttempts = 3
        ),
        deadLetterTopic = "topic.v1.error"
    )
    val topic2 = TopicConfiguration(
        topic = "topic.v2",
        retry = TopicConfiguration.TopicRetryStrategy.ExponentialBackoffMultiTopicRetry(
            maxTotalRetryAttempts = 3,
            delayTopics = listOf(
                TopicConfiguration.TopicRetryStrategy.DelayTopicConfiguration("topic.v2.retry.10sec", 10.seconds),
                TopicConfiguration.TopicRetryStrategy.DelayTopicConfiguration("topic.v2.retry.30sec", 30.seconds),
                TopicConfiguration.TopicRetryStrategy.DelayTopicConfiguration("topic.v2.retry.1min", 1.minutes),
                TopicConfiguration.TopicRetryStrategy.DelayTopicConfiguration("topic.v2.retry.5min", 5.minutes)
            )
        ),
        deadLetterTopic = "topic.v2.error"
    )
    val topic3 = TopicConfiguration(
        topic = "topic.v3",
        retry = TopicConfiguration.TopicRetryStrategy.NoneStrategy,
        deadLetterTopic = "topic.v3.error"
    )
    val topic4 = TopicConfiguration(
        topic = "topic.v4",
        retry = TopicConfiguration.TopicRetryStrategy.ExponentialBackoffToSingleTopicRetry(
            maxTotalRetryAttempts = 3,
            retryTopic = "topic.v4.retry",
            delayTopics = listOf(
                TopicConfiguration.TopicRetryStrategy.DelayTopicConfiguration("topic.v4.delay.10sec", 10.seconds),
                TopicConfiguration.TopicRetryStrategy.DelayTopicConfiguration("topic.v4.delay.30sec", 30.seconds),
                TopicConfiguration.TopicRetryStrategy.DelayTopicConfiguration("topic.v4.delay.1min", 1.minutes),
                TopicConfiguration.TopicRetryStrategy.DelayTopicConfiguration("topic.v4.delay.5min", 5.minutes)
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

    override fun run(properties: MutableMap<String, Any>) {
        val deserializer = StringDeserializer()
        val danglingTopic = "dangling.topic"

        val consumer = QuafkaConsumerBuilder<String, String>(properties)
            .withDeserializer(deserializer, deserializer)
            .subscribeWithErrorHandling(topics, danglingTopic, quafkaProducer) {
                this
                    .withRecoverable {
                        withExceptionDetailsProvider { throwable: Throwable -> ExceptionDetails(throwable, throwable.message!!) }
                            .withMessageDelayer(MessageDelayer())
                            .withPolicyProvider { message, consumerContext, exceptionReport ->
                                when (exceptionReport.exception) {
                                    is DatabaseConcurrencyException -> RetryPolicy.FullRetry(
                                        identifier = "DatabaseConcurrencyException",
                                        inMemoryConfig = InMemoryRetryConfig.basic(maxAttempts = 3, initialDelay = 100.milliseconds),
                                        nonBlockingConfig = NonBlockingRetryConfig.exponentialRandomBackoff(
                                            maxAttempts = 10,
                                            initialDelay = 1.seconds,
                                            maxDelay = 50.seconds
                                        )
                                    )

                                    is DomainException -> RetryPolicy.NoRetry

                                    else -> RetryPolicy.InMemoryOnly(
                                        identifier = "DatabaseConcurrencyException",
                                        config = InMemoryRetryConfig.basic(maxAttempts = 3, initialDelay = 100.milliseconds)
                                    )
                                }
                            }.withOutgoingMessageModifier { message, consumerContext, exceptionReport ->
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
