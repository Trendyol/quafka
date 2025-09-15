package com.trendyol.quafka.consumer.acceptence

import com.trendyol.quafka.*
import com.trendyol.quafka.consumer.*
import com.trendyol.quafka.consumer.configuration.CommitOptions
import com.trendyol.quafka.events.subscribe
import io.kotest.core.spec.style.FunSpec
import io.kotest.matchers.shouldBe
import kotlinx.coroutines.*
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.TopicPartition
import java.util.concurrent.*

class CommitScenarios :
    FunSpec({
        val kafka = EmbeddedKafka.instance

        test("should stuck because of commit waiting to other commits in chunk") {
            // arrange
            val totalMessagePerPartition = 10

            // create topics
            val topics = kafka.createTopics(
                listOf(
                    kafka.newTopic(kafka.getRandomTopicName(), 1)
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
                    withBatchMessageHandler(batchSize = 5, handler = { incomingMessages, _ ->
                        incomingMessages.sortedByDescending { it.offset }.map { incomingMessage ->
                            consumedMessages.computeIfAbsent(incomingMessage.topic) { ConcurrentLinkedQueue() }.add(incomingMessage)
                            incomingMessage.commit().await() // will be stucked here
                            waitGroup.add(-1)
                        }
                    })
                }
            }

            consumer.start()
            consumer.use {
                // assert
                val result = withTimeoutOrNull(5000) {
                    waitGroup.wait()
                    true
                }
                result shouldBe null
            }
        }

        test("should commit all messages when async all") {
            // arrange
            val totalMessagePerPartition = 10

            // create topics
            val topics = kafka.createTopics(
                listOf(
                    kafka.newTopic(kafka.getRandomTopicName(), 1)
                )
            )

            // produce messages to topics
            val messages = kafka.buildMessages(topics, totalMessagePerPartition)
            kafka.createStringStringQuafkaProducer().use { producer ->
                producer.sendAll(messages)
            }

            // build consumer
            val waitGroup = WaitGroup(messages.size)
            val consumedMessages = ConcurrentHashMap<String, ConcurrentLinkedQueue<IncomingMessage<String, String>>>()
            val consumer = kafka.createStringStringQuafkaConsumer { builder ->
                builder.subscribe(*topics.map { it.name() }.toTypedArray()) {
                    withBatchMessageHandler(batchSize = 5, handler = { incomingMessages, _ ->
                        val tasks = incomingMessages.map { incomingMessage ->
                            async {
                                consumedMessages.computeIfAbsent(incomingMessage.topic) { ConcurrentLinkedQueue() }.add(incomingMessage)
                                delay(10)
                                incomingMessage.commit().await()
                                waitGroup.add(-1)
                            }
                        }
                        tasks.awaitAll()
                    })
                }
            }

            consumer.start()
            consumer.use {
                // assert
                val result = withTimeoutOrNull(5000) {
                    waitGroup.wait()
                    true
                }
                result shouldBe true
            }
        }

        test("should commit largest offset when acked in reverse order") {
            // arrange
            val totalMessagePerPartition = 10

            // create topics
            val topics = kafka.createTopics(
                listOf(
                    kafka.newTopic(kafka.getRandomTopicName(), 1)
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
            val waitGroup = WaitGroup(1)
            val consumer = kafka.createStringStringQuafkaConsumer { builder ->
                builder.subscribe(*topics.map { it.name() }.toTypedArray()) {
                    withBatchMessageHandler(batchSize = 10, handler = { incomingMessages, _ ->
                        // ack offset largest to small, 10,9,8...0
                        incomingMessages.sortedByDescending { it.offset }.map { incomingMessage ->
                            incomingMessage.ack()
                        }
                    })
                }
            }
            consumer.start()
            consumer.use {
                // wait
                val commitedOffsets = mutableMapOf<TopicPartition, OffsetAndMetadata>()
                val eventPublisherTask = async {
                    consumer.eventPublisher.subscribe<Events.OffsetsCommitted> {
                        commitedOffsets.putAll(it.offsets)
                        waitGroup.add(-1)
                    }
                }
                waitGroup.wait()

                // assert
                commitedOffsets[TopicPartition(topics.first().name(), 0)]!!.offset() shouldBe 10

                // clean
                eventPublisherTask.cancel()
            }
        }

        test("should commit largest offset when committed in reverse order") {
            // arrange
            val totalMessagePerPartition = 10

            // create topics
            val topics = kafka.createTopics(
                listOf(
                    kafka.newTopic(kafka.getRandomTopicName(), 1)
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
            val waitGroup = WaitGroup(1)
            val consumer = kafka.createStringStringQuafkaConsumer { builder ->
                builder
                    .withCommitOptions(CommitOptions(deferCommitsUntilNoGaps = false))
                    .subscribe(*topics.map { it.name() }.toTypedArray()) {
                        withBatchMessageHandler(batchSize = 10, handler = { incomingMessages, _ ->
                            // ack offset largest to small, 10,9,8...0
                            incomingMessages.sortedByDescending { it.offset }.map { incomingMessage ->
                                incomingMessage.commit()
                            }
                        })
                    }
            }
            consumer.start()
            consumer.use {
                // wait
                val commitedOffsets = mutableMapOf<TopicPartition, OffsetAndMetadata>()
                val eventPublisherTask = async {
                    consumer.eventPublisher.subscribe<Events.OffsetsCommitted> {
                        commitedOffsets.putAll(it.offsets)
                        waitGroup.add(-1)
                    }
                }
                waitGroup.wait()

                // assert
                commitedOffsets[TopicPartition(topics.first().name(), 0)]!!.offset() shouldBe 10

                // clean
                eventPublisherTask.cancel()
            }
        }
    })
