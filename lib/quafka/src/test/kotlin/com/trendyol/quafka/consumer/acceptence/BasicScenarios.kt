package com.trendyol.quafka.consumer.acceptence

import com.trendyol.quafka.*
import com.trendyol.quafka.consumer.*
import com.trendyol.quafka.producer.OutgoingMessage
import io.kotest.core.spec.style.FunSpec
import io.kotest.matchers.shouldBe
import java.util.concurrent.*

class BasicScenarios :
    FunSpec({
        val kafka = EmbeddedKafka.instance

        test("should subscribe to topic and consume message") {
            // arrange
            val topic = kafka.getRandomTopicName()
            kafka.createTopic(topic, 1)
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
                    producer.send(
                        OutgoingMessage.create<String, String>(
                            topic = topic,
                            value = "value2",
                            key = null
                        )
                    )
                }
            val waitGroup = WaitGroup(2)

            val consumedMessages = ConcurrentLinkedQueue<IncomingMessage<String, String>>()
            val consumer = kafka.createStringStringQuafkaConsumer { builder ->
                builder.subscribe(topic) {
                    withSingleMessageHandler { incomingMessage, consumerContext ->
                        consumedMessages.add(incomingMessage)
                        waitGroup.add(-1)
                    }
                }
            }

            // act
            consumer.use {
                it.start()
                // assert
                consumer.status shouldBe QuafkaConsumer.ConsumerStatus.Running
                waitGroup.wait()
                consumedMessages.size shouldBe 2
                consumedMessages.first().value shouldBe "value1"
                consumedMessages.last().value shouldBe "value2"
            }
        }

        test("should consume from two different topic with same handler") {
            // arrange
            val topic1 = kafka.getRandomTopicName()
            val topic2 = kafka.getRandomTopicName()
            kafka.createTopic(topic1, 1)
            kafka.createTopic(topic2, 1)

            kafka
                .createStringStringQuafkaProducer()
                .use { producer ->
                    producer.send(
                        OutgoingMessage.create<String, String>(
                            topic = topic1,
                            value = "value1",
                            key = null
                        )
                    )
                    producer.send(
                        OutgoingMessage.create<String, String>(
                            topic = topic2,
                            value = "value2",
                            key = null
                        )
                    )
                }
            val waitGroup = WaitGroup(2)
            val consumedMessages = ConcurrentHashMap<String, ConcurrentLinkedQueue<IncomingMessage<String, String>>>()
            val consumer = kafka.createStringStringQuafkaConsumer { builder ->
                builder.subscribe(topic1, topic2) {
                    withSingleMessageHandler { incomingMessage, consumerContext ->
                        consumedMessages.computeIfAbsent(incomingMessage.topic) { ConcurrentLinkedQueue() }.add(incomingMessage)
                        waitGroup.add(-1)
                    }
                }
            }

            // act
            consumer
                .use {
                    it.start()

                    // assert
                    waitGroup.wait()
                    consumedMessages.size shouldBe 2
                    consumedMessages[topic1]!!.first().value shouldBe "value1"
                    consumedMessages[topic1]!!.first().topic shouldBe topic1
                    consumedMessages[topic2]!!.first().value shouldBe "value2"
                    consumedMessages[topic2]!!.first().topic shouldBe topic2
                }
        }

        test("should handle messages from multiple topics and partitions") {
            // arrange
            val totalMessagePerPartition = 100

            // create topics
            val topics = kafka.createTopics(
                listOf(
                    kafka.newTopic(kafka.getRandomTopicName(), 5),
                    kafka.newTopic(kafka.getRandomTopicName(), 5)
                )
            )

            // produce messages to topics
            val messages = kafka.buildMessages(topics, totalMessagePerPartition)
            kafka
                .createStringStringQuafkaProducer()
                .use { producer ->
                    producer.sendAll(messages)
                }

            // build consumer
            val waitGroup = WaitGroup(messages.size)
            val consumedMessages = ConcurrentHashMap<String, ConcurrentLinkedQueue<IncomingMessage<String, String>>>()
            val consumer = kafka.createStringStringQuafkaConsumer { builder ->
                builder.subscribe(*topics.map { it.name() }.toTypedArray()) {
                    withSingleMessageHandler { incomingMessage, consumerContext ->
                        consumedMessages.computeIfAbsent(incomingMessage.topic) { ConcurrentLinkedQueue() }.add(incomingMessage)

                        incomingMessage.ack()
                        waitGroup.add(-1)
                    }.withBackpressure(100)
                }
            }

            // act
            consumer.use {
                it.start()

                // assert
                waitGroup.wait()
                topics.forEach { topic ->
                    val topicMessages = consumedMessages[topic.name()]!!
                    val partitions = topicMessages.groupBy { it.partition }
                    topicMessages.size shouldBe totalMessagePerPartition * partitions.size
                    partitions.forEach { partition ->
                        partition.value.size shouldBe totalMessagePerPartition
                    }
                }
            }
        }
    })
