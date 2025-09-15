package com.trendyol.quafka.consumer.acceptence

import com.trendyol.quafka.*
import com.trendyol.quafka.consumer.IncomingMessage
import io.kotest.core.spec.style.FunSpec
import io.kotest.matchers.shouldBe
import kotlinx.coroutines.*
import java.util.concurrent.*

class PartitionScenarios :
    FunSpec({
        val kafka = EmbeddedKafka.instance

        test("should handle partition changes and handle new messages from new partitions") {
            // arrange
            val totalMessagePerPartition = 10

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

                    val alteredTopics = topics.map { kafka.newTopic(it.name(), partition = it.numPartitions() + 1) }
                    val alteredTopicsMessages = kafka.buildMessages(alteredTopics, totalMessagePerPartition).filter { it.partition == 5 }

                    // build consumer
                    val waitGroup = WaitGroup(messages.size + alteredTopicsMessages.size)
                    val consumedMessages = ConcurrentHashMap<String, ConcurrentLinkedQueue<IncomingMessage<String, String>>>()
                    val consumer = kafka.createStringStringQuafkaConsumer { builder ->
                        builder.subscribe(*topics.map { it.name() }.toTypedArray()) {
                            withSingleMessageHandler { incomingMessage, _ ->
                                consumedMessages.computeIfAbsent(incomingMessage.topic) { ConcurrentLinkedQueue() }.add(incomingMessage)
                                delay(10)
                                incomingMessage.commit()
                                waitGroup.add(-1)
                            }
                        }
                    }
                    consumer.start()
                    consumer.use {
                        // increase partitions
                        withContext(Dispatchers.IO) {
                            delay(5000)
                        }
                        kafka.increasePartitions(alteredTopics)
                        producer.sendAll(alteredTopicsMessages)

                        // assert
                        waitGroup.wait()
                        alteredTopics.forEach { topic ->
                            val topicMessages = consumedMessages[topic.name()]!!
                            val partitions = topicMessages.groupBy { it.partition }
                            topicMessages.size shouldBe totalMessagePerPartition * partitions.size
                            partitions.forEach { partition ->
                                partition.value.size shouldBe totalMessagePerPartition
                            }
                        }
                    }
                }
        }
    })
