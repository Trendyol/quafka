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

class SingleTopicRetryStrategyTests :
    FeatureSpec({
        val kafka = EmbeddedKafka.instance

        feature("Single Topic Retry Strategy") {
            scenario("retry failed message in retry topic then forward to error topic if fails 3 times") {
                // arrange

                // create topics
                val topicName = kafka.getRandomTopicName()
                val retryTopicName = "$topicName.retry"
                val errorTopicName = "$topicName.error"
                kafka.createTopics(
                    listOf(
                        kafka.newTopic(topicName, 1),
                        kafka.newTopic(retryTopicName, 1),
                        kafka.newTopic(errorTopicName, 1)
                    )
                )

                // produce messages to topics
                val producer = kafka.createStringStringQuafkaProducer()
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
                                    TopicConfiguration.TopicRetryStrategy.SingleTopicRetry(
                                        retryTopicName,
                                        maxTotalRetryAttempts = maxAttempts
                                    ),
                                    errorTopicName
                                )
                            ),
                            "dangling-topic",
                            producer
                        ) {
                            this
                                .withRecoverable {
                                    withPolicyProvider { message, consumerContext, exceptionReport ->
                                        RetryPolicy.NonBlockingOnly("", NonBlockingRetryConfig.exponentialRandomBackoff(3, 10.milliseconds, 10.seconds))
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
                consumer.use {
                    waitGroup.wait()

                    // assert
                    // retry-topic
                    kafka
                        .createStringStringConsumer(retryTopicName)
                        .use { consumer ->
                            eventually(10.seconds) {
                                val retryMessages = consumer.poll(1.seconds.toJavaDuration())
                                retryMessages.count() shouldBe 3
                                for ((index, message) in retryMessages.sortedBy { it.offset() }.toList().withIndex()) {
                                    message.headers().get(RecoveryHeaders.ORIGINAL_TOPIC)!!.asString() shouldBe topicName
                                    message.headers().get(RecoveryHeaders.ORIGINAL_PARTITION)!!.asInt() shouldBe 0
                                    message.headers().get(RecoveryHeaders.ORIGINAL_OFFSET)!!.asInt() shouldBe 0
                                    message.headers().get(RecoveryHeaders.EXCEPTION_DETAIL)!!.asString() shouldBe "error"
                                    message.headers().get(RecoveryHeaders.EXCEPTION_AT)?.asInstant() shouldNotBe null
                                    message.headers().get(RecoveryHeaders.RETRY_ATTEMPT)?.asInt() shouldBe index + 1
                                    message.headers().get(RecoveryHeaders.OVERALL_RETRY_ATTEMPT)?.asInt() shouldBe index + 1
                                    message.headers().get(RecoveryHeaders.PUBLISHED_AT)?.asInstant() shouldNotBe null
                                    message.headers().get(DelayHeaders.DELAY_PUBLISHED_AT)?.asInstant() shouldNotBe null

                                    message.key() shouldBe "key"
                                }
                            }
                        }

                    kafka
                        .createStringStringConsumer(errorTopicName)
                        .use { consumer ->
                            eventually(10.seconds) {
                                val errorMessages = consumer.poll(1.seconds.toJavaDuration())
                                errorMessages.count() shouldBe 1
                                val errorMessage = errorMessages.first()
                                val headerKeys = errorMessage.headers().toList().map { it.key }
                                headerKeys shouldNotContainAll listOf(DelayHeaders.DELAY_PUBLISHED_AT, DelayHeaders.DELAY_SECONDS, DelayHeaders.DELAY_STRATEGY)
                                errorMessage.headers().get(RecoveryHeaders.RETRY_ATTEMPT)?.asInt() shouldBe 3
                                errorMessage.headers().get(RecoveryHeaders.OVERALL_RETRY_ATTEMPT)?.asInt() shouldBe 3
                                errorMessage.key() shouldBe "key"
                            }
                        }
                }
            }
        }
    })
