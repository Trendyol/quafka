package com.trendyol.quafka.consumer.internals

import com.trendyol.quafka.IncomingMessageBuilder
import com.trendyol.quafka.consumer.TopicPartitionOffset
import io.kotest.core.spec.style.FunSpec
import io.kotest.matchers.*
import io.mockk.*
import kotlinx.coroutines.*
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.TopicPartition
import org.slf4j.Logger

class PartitionAssignmentManagerTests :
    FunSpec({

        lateinit var kafkaConsumer: KafkaConsumer<String, String>
        lateinit var sut: PartitionAssignmentManager<String, String>
        lateinit var testScope: CoroutineScope
        val logger: Logger = mockk(relaxed = true)

        beforeTest {
            kafkaConsumer = mockk(relaxed = true)
            testScope = CoroutineScope(Dispatchers.IO)

            sut = PartitionAssignmentManager(
                kafkaConsumer = kafkaConsumer,
                logger = logger,
                parentScope = testScope,
                quafkaConsumerOptions = mockk(relaxed = true),
                offsetManager = mockk(relaxed = true)
            )
        }

        test("should pause partitions") {
            // arrange
            val topicPartition1 = TopicPartition("topic1", 1)
            val topic1Message1 = IncomingMessageBuilder(
                topic = topicPartition1.topic(),
                partition = topicPartition1.partition(),
                offset = 5,
                key = "key",
                value = "value"
            ).build()
            val topicPartition2 = TopicPartition("topic2", 2)
            val topic1Message2 = IncomingMessageBuilder(
                topic = topicPartition1.topic(),
                partition = topicPartition1.partition(),
                offset = 99,
                key = "key",
                value = "value"
            ).build()

            sut.assignPartitions(
                listOf(
                    TopicPartitionOffset(topicPartition1, 1),
                    TopicPartitionOffset(topicPartition2, 1)
                )
            )
            sut.get(topicPartition1)?.pause(topic1Message1)
            sut.get(topicPartition2)?.pause(topic1Message2)

            val pausedTopicPartitions = slot<List<TopicPartition>>()
            every { kafkaConsumer.pause(capture(pausedTopicPartitions)) } returns Unit

            // act
            sut.pauseResumePartitions()

            // assert
            pausedTopicPartitions.captured shouldBe listOf(
                topicPartition1,
                topicPartition2
            )
        }

        test("should resume partitions") {
            // arrange
            val topicPartition1 = TopicPartition("topic1", 1)
            val topic1Message1 = IncomingMessageBuilder(
                topic = topicPartition1.topic(),
                partition = topicPartition1.partition(),
                offset = 5,
                key = "key",
                value = "value"
            ).build()
            val topicPartition2 = TopicPartition("topic2", 2)
            val topic1Message2 = IncomingMessageBuilder(
                topic = topicPartition1.topic(),
                partition = topicPartition1.partition(),
                offset = 99,
                key = "key",
                value = "value"
            ).build()

            sut.assignPartitions(
                listOf(
                    TopicPartitionOffset(topicPartition1, 1),
                    TopicPartitionOffset(topicPartition2, 1)
                )
            )
            sut.get(topicPartition1)?.pause(topic1Message1)
            sut.get(topicPartition2)?.pause(topic1Message2)
            sut.get(topicPartition1)?.resume()
            sut.get(topicPartition2)?.resume()

            every { kafkaConsumer.paused() } returns setOf(topicPartition1, topicPartition2)
            val resumedTopicPartitions = slot<List<TopicPartition>>()
            every { kafkaConsumer.resume(capture(resumedTopicPartitions)) } returns Unit
            val seekedTopicPartitions = mutableListOf<TopicPartition>()
            val seekedOffsets = mutableListOf<Long>()
            every { kafkaConsumer.seek(capture(seekedTopicPartitions), capture(seekedOffsets)) } returns Unit

            // act
            sut.pauseResumePartitions()

            // assert
            resumedTopicPartitions.captured shouldBe listOf(
                topicPartition1,
                topicPartition2
            )
            val topicPartitionOffsets = seekedTopicPartitions.zip(seekedOffsets)
            topicPartitionOffsets.first { it.first == topicPartition1 }.second shouldBe 5
            topicPartitionOffsets.first { it.first == topicPartition2 }.second shouldBe 99
        }

        test("should revoke assigned topic partition") {
            // arrange
            val topicPartition1 = TopicPartition("topic1", 1)
            sut.assignPartitions(
                listOf(
                    TopicPartitionOffset(topicPartition1, 1)
                )
            )

            // act
            sut.revokePartitions(
                listOf(
                    topicPartition1
                )
            )

            // assert
            sut.get(topicPartition1) shouldBe null
        }

        test("should assign and revoke topic partitions") {
            // arrange
            sut.assignPartitions(
                listOf(
                    TopicPartitionOffset(TopicPartition("topic1", 1), 1),
                    TopicPartitionOffset(TopicPartition("topic2", 1), 1)
                )
            )

            // act
            sut.assignPartitions(
                listOf(
                    TopicPartitionOffset(TopicPartition("topic2", 1), 10),
                    TopicPartitionOffset(TopicPartition("topic3", 1), 1),
                    TopicPartitionOffset(TopicPartition("topic4", 1), 1)
                )
            )

            // assert
            sut.get(TopicPartition("topic1", 1)) shouldBe null
            sut.get(TopicPartition("topic2", 1))?.assignedOffset shouldBe 10
            sut.get(TopicPartition("topic3", 1))?.assignedOffset shouldBe 1
            sut.get(TopicPartition("topic4", 1))?.assignedOffset shouldBe 1
        }
    })
