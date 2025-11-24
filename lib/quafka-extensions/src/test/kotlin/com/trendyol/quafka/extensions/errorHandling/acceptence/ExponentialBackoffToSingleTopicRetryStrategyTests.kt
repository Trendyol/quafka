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
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds
import kotlin.time.toJavaDuration

class ExponentialBackoffToSingleTopicRetryStrategyTests :
    FeatureSpec({
        val kafka = EmbeddedKafka.instance

        feature("Exponential Backoff To Single Topic Retry Strategy") {
            scenario("retry failed message through delay topics then forward to single retry topic then error topic if fails 3 times") {
                // arrange

                // create topics
                val topicName = kafka.getRandomTopicName()
                val retryTopicName = "$topicName.retry"
                val errorTopicName = "$topicName.error"
                val delay5sTopicName = "$topicName.delay-5s"
                val delay10sTopicName = "$topicName.delay-10s"
                val delay30sTopicName = "$topicName.delay-30s"

                kafka.createTopics(
                    listOf(
                        kafka.newTopic(topicName, 1),
                        kafka.newTopic(retryTopicName, 1),
                        kafka.newTopic(errorTopicName, 1),
                        kafka.newTopic(delay5sTopicName, 1),
                        kafka.newTopic(delay10sTopicName, 1),
                        kafka.newTopic(delay30sTopicName, 1)
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

                        val waitGroup = WaitGroup(3)
                        val consumer = kafka.createStringStringQuafkaConsumer { builder ->
                            builder
                                .subscribeWithErrorHandling(
                                    listOf(
                                        TopicConfiguration(
                                            topicName,
                                            TopicRetryStrategy.ExponentialBackoffToSingleTopicRetry(
                                                retryTopic = retryTopicName,
                                                maxOverallAttempts = maxAttempts,
                                                delayTopics = listOf(
                                                    TopicRetryStrategy.DelayTopicConfiguration(delay5sTopicName, 5.seconds),
                                                    TopicRetryStrategy.DelayTopicConfiguration(delay10sTopicName, 10.seconds),
                                                    TopicRetryStrategy.DelayTopicConfiguration(delay30sTopicName, 30.seconds)
                                                )
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
                                                throw Exception("error")
                                            } finally {
                                                waitGroup.done()
                                            }
                                        }
                                }
                        }

                        // act
                        consumer.start()
                        waitGroup.wait()

                        // assert
                        // delay topics should contain messages
                        val delay5sConsumer = kafka.createStringStringConsumer(delay5sTopicName)
                        eventually(10.seconds) {
                            val delayMessages = delay5sConsumer.poll(1.seconds.toJavaDuration())
                            delayMessages.count() shouldBe 3
                            for ((index, message) in delayMessages.sortedBy { it.offset() }.toList().withIndex()) {
                                message.headers().get(RecoveryHeaders.ORIGINAL_TOPIC)!!.asString() shouldBe topicName
                                message.headers().get(RecoveryHeaders.ORIGINAL_PARTITION)!!.asInt() shouldBe 0
                                message.headers().get(RecoveryHeaders.ORIGINAL_OFFSET)!!.asInt() shouldBe 0
                                message.headers().get(RecoveryHeaders.EXCEPTION_DETAIL)!!.asString() shouldBe "error"
                                message.headers().get(RecoveryHeaders.EXCEPTION_AT)?.asInstant() shouldNotBe null
                                message.headers().get(RecoveryHeaders.RETRY_ATTEMPTS)?.asInt() shouldBe index + 1
                                message.headers().get(RecoveryHeaders.OVERALL_RETRY_ATTEMPTS)?.asInt() shouldBe index + 1
                                message.headers().get(RecoveryHeaders.PUBLISHED_AT)?.asInstant() shouldNotBe null
                                message.headers().get(DelayHeaders.DELAY_PUBLISHED_AT)?.asInstant() shouldNotBe null
                                message.headers().get(RecoveryHeaders.FORWARDING_TOPIC)?.asString() shouldBe retryTopicName

                                message.key() shouldBe "key"
                            }
                        }

                        // error topic should contain final failed message
                        val errorTopicConsumer = kafka.createStringStringConsumer(errorTopicName)
                        eventually(10.seconds) {
                            val errorMessages = errorTopicConsumer.poll(1.seconds.toJavaDuration())
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

            scenario("successfully process message after retry delay") {
                // arrange
                val topicName = kafka.getRandomTopicName()
                val retryTopicName = "$topicName.retry"
                val errorTopicName = "$topicName.error"
                val delay5sTopicName = "$topicName.delay-5s"

                kafka.createTopics(
                    listOf(
                        kafka.newTopic(topicName, 1),
                        kafka.newTopic(retryTopicName, 1),
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
                                            TopicRetryStrategy.ExponentialBackoffToSingleTopicRetry(
                                                retryTopic = retryTopicName,
                                                maxOverallAttempts = 3,
                                                delayTopics = listOf(
                                                    TopicRetryStrategy.DelayTopicConfiguration(delay5sTopicName, 5.seconds)
                                                )
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
                        waitGroup.wait()

                        // assert - message should be processed successfully after retry
                        val errorTopicConsumer = kafka.createStringStringConsumer(errorTopicName)
                        eventually(5.seconds) {
                            val errorMessages = errorTopicConsumer.poll(1.seconds.toJavaDuration())
                            errorMessages.count() shouldBe 0 // No messages should go to error topic
                        }

                        attemptCount shouldBe 2 // Should be processed twice (initial + retry)
                    }
            }
        }
    })
