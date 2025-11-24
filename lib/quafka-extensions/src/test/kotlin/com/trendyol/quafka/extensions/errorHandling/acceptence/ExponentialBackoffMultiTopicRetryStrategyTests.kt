package com.trendyol.quafka.extensions.errorHandling.acceptence

import com.trendyol.quafka.*
import com.trendyol.quafka.common.HeaderParsers.asInstant
import com.trendyol.quafka.common.HeaderParsers.asInt
import com.trendyol.quafka.common.HeaderParsers.asString
import com.trendyol.quafka.common.HeaderParsers.key
import com.trendyol.quafka.common.get
import com.trendyol.quafka.extensions.delaying.DelayHeaders
import com.trendyol.quafka.extensions.errorHandling.configuration.subscribeWithErrorHandling
import com.trendyol.quafka.extensions.errorHandling.recoverer.*
import com.trendyol.quafka.producer.OutgoingMessage
import io.kotest.assertions.nondeterministic.eventually
import io.kotest.core.spec.style.FeatureSpec
import io.kotest.matchers.*
import io.kotest.matchers.collections.shouldNotContainAll
import org.apache.kafka.clients.consumer.KafkaConsumer
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds
import kotlin.time.toJavaDuration

class ExponentialBackoffMultiTopicRetryStrategyTests :
    FeatureSpec({
        val kafka = EmbeddedKafka.instance

        fun assert(
            consumer: KafkaConsumer<String, String>,
            originalTopicName: String,
            exceptionDetail: String,
            attempt: Int,
            overallAttempts: Int
        ): String? {
            val delayMessages = consumer.poll(1.seconds.toJavaDuration())
            delayMessages.count() shouldBe 1
            val message = delayMessages.first()
            message.headers().get(RecoveryHeaders.ORIGINAL_TOPIC)!!.asString() shouldBe originalTopicName
            message.headers().get(RecoveryHeaders.ORIGINAL_PARTITION)!!.asInt() shouldBe 0
            message.headers().get(RecoveryHeaders.ORIGINAL_OFFSET)!!.asInt() shouldBe 0
            message.headers().get(RecoveryHeaders.EXCEPTION_DETAIL)!!.asString() shouldBe exceptionDetail
            message.headers().get(RecoveryHeaders.EXCEPTION_AT)?.asInstant() shouldNotBe null
            message.headers().get(RecoveryHeaders.RETRY_ATTEMPTS)?.asInt() shouldBe attempt
            message.headers().get(RecoveryHeaders.OVERALL_RETRY_ATTEMPTS)?.asInt() shouldBe overallAttempts
            message.headers().get(RecoveryHeaders.PUBLISHED_AT)?.asInstant() shouldNotBe null
            message.headers().get(DelayHeaders.DELAY_PUBLISHED_AT)?.asInstant() shouldNotBe null
            return message.key() shouldBe "key"
        }

        feature("Exponential Backoff Multi Topic Retry Strategy") {
            scenario("retry failed message through different delay topics then forward to error topic if fails 3 times") {
                // arrange

                // create topics
                val topicName = kafka.getRandomTopicName()
                val errorTopicName = "$topicName.error"
                val delay1sTopicName = "$topicName.delay-100ms"
                val delay2sTopicName = "$topicName.delay-200ms"
                val delay3sTopicName = "$topicName.delay-300ms"

                kafka.createTopics(
                    listOf(
                        kafka.newTopic(topicName, 1),
                        kafka.newTopic(errorTopicName, 1),
                        kafka.newTopic(delay1sTopicName, 1),
                        kafka.newTopic(delay2sTopicName, 1),
                        kafka.newTopic(delay3sTopicName, 1)
                    )
                )

                // produce messages to topics
                kafka
                    .createStringStringQuafkaProducer()
                    .use { producer ->
                        producer.send(
                            OutgoingMessage.create(
                                topic = topicName,
                                partition = null,
                                key = "key",
                                value = "value",
                                headers = emptyList()
                            )
                        )

                        val maxAttempts = 3

                        val waitGroup = WaitGroup(4) // main message + retried messages
                        val consumer = kafka.createStringStringQuafkaConsumer { builder ->
                            builder
                                .subscribeWithErrorHandling(
                                    listOf(
                                        TopicConfiguration(
                                            topicName,
                                            TopicRetryStrategy.ExponentialBackoffMultiTopicRetry(
                                                retryTopics = listOf(
                                                    TopicRetryStrategy.DelayTopicConfiguration(delay1sTopicName, 100.milliseconds),
                                                    TopicRetryStrategy.DelayTopicConfiguration(delay2sTopicName, 200.milliseconds),
                                                    TopicRetryStrategy.DelayTopicConfiguration(delay3sTopicName, 300.milliseconds)
                                                ),
                                                maxOverallAttempts = maxAttempts
                                            ),
                                            errorTopicName
                                        )
                                    ),
                                    "dangling-topic",
                                    producer
                                ) {
                                    this
                                        .withRecoverable {
                                            withPolicyProvider { _, message, consumerContext, exceptionDetails ->
                                                RetryPolicy.NonBlocking(
                                                    PolicyIdentifier("test"),
                                                    NonBlockingRetryConfig.custom(3) { _, _, _, attempt ->
                                                        (attempt * 100).milliseconds
                                                    }
                                                )
                                            }.withExceptionDetailsProvider { throwable -> ExceptionDetails(throwable, throwable.message!!) }
                                        }.withSingleMessageHandler { incomingMessage, consumerContext ->
                                            try {
                                                throw Exception("error from ${incomingMessage.topic}")
                                            } finally {
                                                waitGroup.done()
                                            }
                                        }.autoAckAfterProcess(true)
                                }
                        }

                        // act
                        consumer.start()
                        consumer.use {
                            waitGroup.wait()

                            // assert
                            // delay topics should contain messages (each message goes directly to delay topics which act as retry topics)
                            kafka
                                .createStringStringConsumer(delay1sTopicName)
                                .use { consumer ->
                                    eventually(10.seconds) {
                                        assert(consumer, topicName, "error from $topicName", 1, 1)
                                    }
                                }

                            kafka
                                .createStringStringConsumer(delay2sTopicName)
                                .use { consumer ->
                                    eventually(10.seconds) {
                                        assert(consumer, topicName, "error from $delay1sTopicName", 2, 2)
                                    }
                                }

                            kafka
                                .createStringStringConsumer(delay3sTopicName)
                                .use { consumer ->
                                    eventually(10.seconds) {
                                        assert(consumer, topicName, "error from $delay2sTopicName", 3, 3)
                                    }
                                }

                            // error topic should contain final failed message
                            kafka
                                .createStringStringConsumer(errorTopicName)
                                .use { consumer ->
                                    eventually(10.seconds) {
                                        val errorMessages = consumer.poll(1.seconds.toJavaDuration())
                                        errorMessages.count() shouldBe 1
                                        val errorMessage = errorMessages.first()
                                        val headerKeys = errorMessage.headers().toList().map { it.key }
                                        headerKeys shouldNotContainAll listOf(DelayHeaders.DELAY_PUBLISHED_AT, DelayHeaders.DELAY_SECONDS, DelayHeaders.DELAY_STRATEGY)
                                        errorMessage.headers().get(RecoveryHeaders.RETRY_ATTEMPTS)?.asInt() shouldBe 3
                                        errorMessage.headers().get(RecoveryHeaders.OVERALL_RETRY_ATTEMPTS)?.asInt() shouldBe 3
                                        errorMessage.key() shouldBe "key"
                                    }
                                }
                        }
                    }
            }

            scenario("successfully process message after retry delay") {
                // arrange
                val topicName = kafka.getRandomTopicName()
                val errorTopicName = "$topicName.error"
                val delay5sTopicName = "$topicName.delay-5s"

                kafka.createTopics(
                    listOf(
                        kafka.newTopic(topicName, 1),
                        kafka.newTopic(errorTopicName, 1),
                        kafka.newTopic(delay5sTopicName, 1)
                    )
                )

                kafka
                    .createStringStringQuafkaProducer()
                    .use { producer ->
                        producer.send(
                            OutgoingMessage.create(
                                topic = topicName,
                                partition = null,
                                key = "success-key",
                                value = "success-value",
                                headers = emptyList()
                            )
                        )

                        var attemptCount = 0
                        val waitGroup = WaitGroup(2) // Initial + one retry

                        val consumer = kafka.createStringStringQuafkaConsumer { builder ->
                            builder
                                .subscribeWithErrorHandling(
                                    listOf(
                                        TopicConfiguration(
                                            topicName,
                                            TopicRetryStrategy.ExponentialBackoffMultiTopicRetry(
                                                retryTopics = listOf(
                                                    TopicRetryStrategy.DelayTopicConfiguration(delay5sTopicName, 5.seconds)
                                                ),
                                                maxOverallAttempts = 3
                                            ),
                                            errorTopicName
                                        )
                                    ),
                                    "dangling-topic",
                                    producer
                                ) {
                                    this
                                        .withRecoverable {
                                            withPolicyProvider { _, message, consumerContext, exceptionDetails ->
                                                RetryPolicy.NonBlocking(PolicyIdentifier("test"), NonBlockingRetryConfig.exponentialRandomBackoff(3, 10.milliseconds, 10.seconds))
                                            }.withExceptionDetailsProvider { throwable -> ExceptionDetails(throwable, throwable.message!!) }
                                        }.withSingleMessageHandler { incomingMessage, consumerContext ->
                                            try {
                                                attemptCount++
                                                if (attemptCount == 1) {
                                                    throw Exception("temporary error")
                                                }
                                                // Success on second attempt
                                            } finally {
                                                waitGroup.done()
                                            }
                                        }
                                }
                        }

                        // act
                        consumer.start()
                        consumer.use {
                            waitGroup.wait()

                            // assert - message should be processed successfully after retry
                            kafka
                                .createStringStringConsumer(errorTopicName)
                                .use { consumer ->
                                    eventually(5.seconds) {
                                        val errorMessages = consumer.poll(1.seconds.toJavaDuration())
                                        errorMessages.count() shouldBe 0 // No messages should go to error topic
                                    }
                                }

                            attemptCount shouldBe 2 // Should be processed twice (initial + retry)
                        }
                    }
            }
        }
    })
