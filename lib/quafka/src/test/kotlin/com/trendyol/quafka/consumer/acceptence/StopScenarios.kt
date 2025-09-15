package com.trendyol.quafka.consumer.acceptence

import com.trendyol.quafka.EmbeddedKafka
import com.trendyol.quafka.consumer.*
import com.trendyol.quafka.producer.OutgoingMessage
import io.kotest.core.spec.style.FunSpec
import io.kotest.matchers.shouldBe
import java.util.concurrent.CountDownLatch

class StopScenarios :
    FunSpec({
        val kafka = EmbeddedKafka.instance

        test("should stop consumer") {
            // arrange
            val topic = kafka.getRandomTopicName()
            kafka
                .createStringStringQuafkaProducer()
                .use { producer ->
                    producer.send(
                        OutgoingMessage.create<String, String>(
                            topic = topic,
                            value = "value1",
                            key = null
                        )
                    )
                }
            val consumedMessages = mutableListOf<IncomingMessage<String, String>>()
            val waiter = CountDownLatch(1)
            val consumer = kafka.createStringStringQuafkaConsumer { builder ->
                builder.subscribe(topic) {
                    withSingleMessageHandler { incomingMessage, consumerContext ->
                        consumedMessages.add(incomingMessage)
                        waiter.countDown()
                    }
                }
            }

            consumer.start()
            consumer.use {
                waiter.await()
                // act
                consumer.stop()

                // assert
                consumedMessages.size shouldBe 1
                consumedMessages[0].value shouldBe "value1"
                consumer.status shouldBe QuafkaConsumer.ConsumerStatus.Ready
            }
        }
    })
