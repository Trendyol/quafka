package com.trendyol.quafka.consumer.internals

import com.trendyol.quafka.WaitGroup
import com.trendyol.quafka.common.Waiter
import com.trendyol.quafka.consumer.TopicPartition
import com.trendyol.quafka.consumer.configuration.QuafkaConsumerOptions
import io.kotest.core.spec.style.FunSpec
import io.mockk.*
import kotlinx.coroutines.*
import org.apache.kafka.clients.consumer.*
import org.slf4j.Logger
import java.time.Duration
import java.util.Optional
import kotlin.time.Duration.Companion.seconds

class PollingConsumerTests :
    FunSpec({

        lateinit var quafkaConsumerOptions: QuafkaConsumerOptions<String, String>
        lateinit var consumer: KafkaConsumer<String, String>
        lateinit var partitionAssignmentManager: PartitionAssignmentManager<String, String>
        lateinit var offsetManager: OffsetManager<String, String>
        var job = Job()
        var scope = CoroutineScope(Dispatchers.IO + job)
        val logger: Logger = mockk(relaxed = true)
        var waitGroup: Waiter? = null
        lateinit var pollingConsumer: PollingConsumer<String, String>

        beforeEach {
            quafkaConsumerOptions = mockk(relaxed = true) {
                every { pollDuration } returns 1.seconds
            }
            consumer = mockk(relaxed = true)
            partitionAssignmentManager = mockk(relaxed = true)
            offsetManager = mockk(relaxed = true)
            waitGroup = mockk(relaxed = true)
            job = Job()
            scope = CoroutineScope(Dispatchers.IO + job)
            pollingConsumer = PollingConsumer(
                quafkaConsumerOptions = quafkaConsumerOptions,
                consumer = consumer,
                partitionAssignmentManager = partitionAssignmentManager,
                offsetManager = offsetManager,
                scope = scope,
                logger = logger,
                waiter = waitGroup
            )

            every { quafkaConsumerOptions.subscribedTopics() } returns listOf("topicA", "topicB")
        }

        test("should poll in a loop until scope is not active (empty records)") {
            // arrange
            every { consumer.poll(any<Duration>()) } returns ConsumerRecords.empty()

            // act
            pollingConsumer.start()

            delay(500)
            scope.cancel()
            job.join()

            // assert
            verify(atLeast = 2) { consumer.poll(any<Duration>()) }
            verify { offsetManager.tryToFlushOffsets() } // processWaitingTasks called
            verify { partitionAssignmentManager.pauseResumePartitions() }
        }

        test("should throw CancellationException and should not log errors or rethrow") {
            // arrange
            pollingConsumer.start()
            delay(100)

            // act
            scope.cancel()

            // assert
            verify(exactly = 0) { logger.warn("an error occurred when closing consumer", any<Throwable>()) }
        }

        test("should dispatch messages when poll returns non-empty records") {
            // arrange
            val waiter = WaitGroup(1)
            val tp = TopicPartition("testTopic", 0)
            val records = buildConsumerRecords(tp, listOf(10L, 11L))
            every { consumer.poll(any<Duration>()) } returns records coAndThen {
                waiter.done()
                ConsumerRecords.empty()
            }

            val assignedPartition = mockk<AssignedTopicPartition<String, String>>(relaxed = true) {
                every { isPaused } returns false
                coEvery { offerMessage(any()) } returns EnqueueResult.Ok
            }
            every { partitionAssignmentManager.get(tp) } returns assignedPartition

            // act
            pollingConsumer.start()
            waiter.wait()

            // assert
            coVerify { assignedPartition.offerMessage(any()) }
        }

        test("should skip messages dispatching if assignedPartition is paused") {
            // arrange
            val waiter = WaitGroup(1)
            val tp = TopicPartition("testTopic", 0)
            val records = buildConsumerRecords(tp, listOf(10L, 11L))
            every { consumer.poll(any<Duration>()) } returns records coAndThen {
                waiter.done()
                ConsumerRecords.empty()
            }
            val assignedPartition = mockk<AssignedTopicPartition<String, String>>(relaxed = true) {
                every { isPaused } returns true
            }
            every { partitionAssignmentManager.get(tp) } returns assignedPartition

            // act
            pollingConsumer.start()
            waiter.wait()

            // assert
            coVerify(atLeast = 0) { assignedPartition.offerMessage(any()) }
        }

        test("should skip messages dispatching if assignedPartition is null") {
            // arrange
            val waiter = WaitGroup(1)
            val tp = TopicPartition("testTopic", 0)
            val records = buildConsumerRecords(tp, listOf(10L, 11L))
            every { consumer.poll(any<Duration>()) } returns records coAndThen {
                waiter.done()
                ConsumerRecords.empty()
            }
            every { partitionAssignmentManager.get(tp) } returns null

            // act
            pollingConsumer.start()
            waiter.wait()

            // assert
            verify(exactly = 1) { partitionAssignmentManager.get(tp) }
        }

        test("should subscribe and start consumer and launch poll loop") {
            // arrange
            val mockedResult = mockk<ConsumerRecords<String, String>>()
            every { consumer.poll(any<Duration>()) } answers {
                scope.cancel()
                mockedResult
            }
            every { consumer.subscribe(listOf("topicA", "topicB"), pollingConsumer) } just runs

            // act
            pollingConsumer.start()
            delay(100)

            // assert
            verify(exactly = 1) { consumer.subscribe(listOf("topicA", "topicB"), pollingConsumer) }
            verify(exactly = 1) { consumer.poll(any<Duration>()) }
            verify(exactly = 1) { waitGroup?.done() }
        }

        test("should stops workers, flush offsets, unsubscribe, close consumer") {
            // arrange
            every { offsetManager.flushOffsetsSync(any()) } just runs
            pollingConsumer.start()
            delay(100)

            // act
            job.cancelAndJoin()

            // Then
            verify { offsetManager.flushOffsetsSync(any()) }
            verify { consumer.wakeup() }
            verify { consumer.unsubscribe() }
            verify { consumer.close(any<Duration>()) }
        }

        test("should flush offsets and revoke partitions") {
            // Given
            every { offsetManager.flushOffsetsSync(any()) } just runs
            every { partitionAssignmentManager.revokePartitions(any()) } just runs
            val revokedPartitions = mutableListOf(
                TopicPartition("topicA", 0),
                TopicPartition("topicB", 1)
            )

            // When
            pollingConsumer.onPartitionsRevoked(revokedPartitions)

            // Then
            verify(exactly = 1) { partitionAssignmentManager.revokePartitions(any()) }
        }
    })

private fun buildConsumerRecords(
    topicPartition: TopicPartition,
    offsets: List<Long>
): ConsumerRecords<String, String> {
    val records = offsets.map { offset ->
        mockk<ConsumerRecord<String, String>>(relaxed = true).also {
            every { it.topic() } returns topicPartition.topic()
            every { it.partition() } returns topicPartition.partition()
            every { it.offset() } returns offset
            every { it.leaderEpoch() } returns Optional.empty()
        }
    }
    return ConsumerRecords(
        mapOf(topicPartition to records)
    )
}
