package com.trendyol.quafka.extensions.errorHandling.acceptence

import com.trendyol.quafka.*
import com.trendyol.quafka.common.HeaderParsers.key
import com.trendyol.quafka.extensions.errorHandling.configuration.subscribeWithErrorHandling
import com.trendyol.quafka.extensions.errorHandling.recoverer.*
import com.trendyol.quafka.producer.OutgoingMessage
import io.kotest.assertions.nondeterministic.eventually
import io.kotest.core.spec.style.FeatureSpec
import io.kotest.matchers.collections.shouldContainAll
import io.kotest.matchers.shouldBe
import kotlin.time.Duration.Companion.seconds
import kotlin.time.toJavaDuration

class NoneStrategyTests :
    FeatureSpec({
        val kafka = EmbeddedKafka.instance

        feature("None Strategy") {
            scenario("forward failed message to error topic") {
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

                val waitGroup = WaitGroup(1)
                val consumer = kafka.createStringStringQuafkaConsumer { builder ->
                    builder
                        .subscribeWithErrorHandling(
                            listOf(
                                TopicConfiguration(
                                    topicName,
                                    TopicConfiguration.TopicRetryStrategy.NoneStrategy,
                                    errorTopicName
                                )
                            ),
                            "dangling-topic",
                            producer
                        ) {
                            this
                                .withRecoverable {
                                    withExceptionDetailsProvider { throwable -> ExceptionDetails(throwable, throwable.message!!) }
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
                    kafka
                        .createStringStringConsumer(errorTopicName)
                        .use { consumer ->
                            eventually(10.seconds) {
                                val producedMessages = consumer.poll(1.seconds.toJavaDuration())
                                producedMessages.count() shouldBe 1
                                val firstMessage = producedMessages.first()
                                firstMessage.key() shouldBe "key"
                                val headerKeys = firstMessage.headers().toList().map { it.key }
                                headerKeys shouldContainAll listOf("X-Exception", "X-PublishedAt")
                            }
                        }
                }
            }
        }
    })
