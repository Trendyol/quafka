package com.trendyol.quafka.consumer.acceptence

import com.trendyol.quafka.*
import com.trendyol.quafka.consumer.IncomingMessage
import com.trendyol.quafka.consumer.QuafkaConsumer
import io.kotest.core.spec.style.FunSpec
import io.kotest.matchers.shouldBe
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.delay
import kotlinx.coroutines.withContext
import org.apache.kafka.common.TopicPartition
import java.util.concurrent.*
import kotlin.random.Random

class RebalanceScenarios :
    FunSpec({
        val kafka = EmbeddedKafka.instance

        test("should continue to process messages successfuly when rebalance occurred frequently ") {
            // arrange
            val totalMessagePerPartition = 10

            // create topics
            val topics = kafka.createTopics(
                listOf(
                    kafka.newTopic(kafka.getRandomTopicName(), 3),
                    kafka.newTopic(kafka.getRandomTopicName(), 8)
                )
            )

            // produce messages to topics
            val messages = kafka.buildMessages(topics, totalMessagePerPartition)
            kafka
                .createStringStringQuafkaProducer()
                .use { producer ->
                    producer.sendAll(messages)
                }

            val waitGroup = WaitGroup(messages.size)
            val consumedMessages = ConcurrentHashMap<TopicPartition, ConcurrentLinkedQueue<Pair<String, IncomingMessage<String, String>>>>()

            val consumers = CopyOnWriteArrayList<QuafkaConsumer<String, String>>()
            val consumerCount = 5
            // build consumer
            val scope = Dispatchers.IO
            withContext(scope) {
                (0 until consumerCount)
                    .map {
                        async {
                            val consumerName = "consumer-$it"
                            val consumer = kafka.createStringStringQuafkaConsumer { builder ->
                                builder
                                    .withClientId(consumerName)
                                    .subscribe(*topics.map { it.name() }.toTypedArray()) {
                                        withSingleMessageHandler { incomingMessage, consumerContext ->
                                            delay(Random.nextLong(500, 1000))
                                            incomingMessage.ack()
                                            consumedMessages.computeIfAbsent(incomingMessage.topicPartition) { ConcurrentLinkedQueue() }.add(Pair(consumerName, incomingMessage))
                                            waitGroup.add(-1)
                                        }.withBackpressure(100)
                                    }
                            }
                            consumer.start()
                            consumers.add(consumer)
                        }
                    }
            }

            try {
                waitGroup.wait()
                consumedMessages.forEach { (_, topicPartitionMessages) ->
                    val topicMessages = topicPartitionMessages.map { it.second }
                    try {

                        topicMessages.size shouldBe totalMessagePerPartition
                        for ((index, message) in topicMessages.withIndex()) {
                            message.offset shouldBe index
                        }
                    } catch (e: Throwable) {
                        println(topicMessages)
                        throw e
                    }
                }

                val createdTopicPartitions = topics
                    .flatMap { createdTopic ->
                        (0 until createdTopic.numPartitions())
                            .map { TopicPartition(createdTopic.name(), it) }
                    }.sortedBy { it.toString() }
                consumedMessages.map { it.key }.sortedBy { it.toString() } shouldBe createdTopicPartitions
            } finally {
                consumers.forEach { it.close() }
            }
        }

        test("should continue to process messages after rebalance") {
            // arrange
            val totalMessagePerPartition = 10

            // create topics
            val topics = kafka.createTopics(
                listOf(
                    kafka.newTopic(kafka.getRandomTopicName(), 3),
                    kafka.newTopic(kafka.getRandomTopicName(), 8)
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
            val consumedMessages = ConcurrentHashMap<TopicPartition, ConcurrentLinkedQueue<Pair<String, IncomingMessage<String, String>>>>()
            val consumer1 = kafka.createStringStringQuafkaConsumer { builder ->
                builder.subscribe(*topics.map { it.name() }.toTypedArray()) {
                    withSingleMessageHandler { incomingMessage, consumerContext ->
                        delay(Random.nextLong(500, 1000))
                        incomingMessage.ack()
                        consumedMessages.computeIfAbsent(incomingMessage.topicPartition) { ConcurrentLinkedQueue() }.add(Pair("consumer-1", incomingMessage))
                        waitGroup.add(-1)
                    }.withBackpressure(100)
                }
            }

            // act
            consumer1.use {
                it.start()
                delay(5000)
                val consumer2 = kafka.createStringStringQuafkaConsumer { builder ->
                    builder.subscribe(*topics.map { it.name() }.toTypedArray()) {
                        withSingleMessageHandler { incomingMessage, consumerContext ->
                            delay(Random.nextLong(500, 1000))
                            incomingMessage.ack()
                            consumedMessages.computeIfAbsent(incomingMessage.topicPartition) { ConcurrentLinkedQueue() }.add(Pair("consumer-2", incomingMessage))
                            waitGroup.add(-1)
                        }.withBackpressure(100)
                    }
                }
                consumer2.start()
                // assert

                waitGroup.wait()
                consumedMessages.forEach { (_, topicPartitionMessages) ->
                    val topicMessages = topicPartitionMessages.map { it.second }
                    try {

                        topicMessages.size shouldBe totalMessagePerPartition
                        for ((index, message) in topicMessages.withIndex()) {
                            message.offset shouldBe index
                        }
                    } catch (e: Throwable) {
                        println(topicMessages)
                        throw e
                    }
                }

                val createdTopicPartitions = topics
                    .flatMap { createdTopic ->
                        (0 until createdTopic.numPartitions())
                            .map { TopicPartition(createdTopic.name(), it) }
                    }.sortedBy { it.toString() }
                consumedMessages.map { it.key }.sortedBy { it.toString() } shouldBe createdTopicPartitions
            }
        }
    })
