package com.trendyol.quafka.consumer.internals

import com.trendyol.quafka.*
import com.trendyol.quafka.common.InvalidConfigurationException
import com.trendyol.quafka.consumer.TopicPartition
import com.trendyol.quafka.consumer.configuration.*
import io.kotest.assertions.throwables.shouldThrow
import io.kotest.core.spec.style.FunSpec
import io.kotest.matchers.*
import io.mockk.*
import kotlinx.coroutines.*
import kotlin.time.Duration.Companion.seconds

class AssignedTopicPartitionTests :
    FunSpec({

        val topicPartition = TopicPartition("test-topic", 0)
        val topicPartitionMessageBuilder = TopicPartitionBasedMessageBuilder<String, String>(topicPartition)

        fun createSut(
            commitOptions: CommitOptions = mockk {
                every { duration } returns 5.seconds
                every { deferCommitsUntilNoGaps } returns true
                every { allowOutOfOrderCommit } returns false
                every { maxRetryAttemptOnFail } returns 0
            },
            subscriptionOptions: TopicSubscriptionOptions<String, String> = mockk(relaxed = true) {
                every { backpressureBufferSize } returns 10
            },
            offsetManager: OffsetManager<String, String> = mockk(relaxed = true) {
            },
            topicPartitionOffsets: TopicPartitionOffsets = mockk(relaxed = true) {
            }
        ): AssignedTopicPartition<String, String> {
            val scope = CoroutineScope(Dispatchers.IO)
            val options: QuafkaConsumerOptions<String, String> = mockk(relaxed = true) {
                val t = this
                every { t.commitOptions } returns commitOptions
                every { t.getSubscriptionOptionsByTopicName(any()) } returns subscriptionOptions
            }
            every { offsetManager.register(any(), any()) } returns topicPartitionOffsets
            return AssignedTopicPartition.create(
                topicPartition = topicPartition,
                parentScope = scope,
                quafkaConsumerOptions = options,
                initialOffset = 10,
                offsetManager = offsetManager
            )
        }

        test("should ack message") {
            // arrange
            val message1 = topicPartitionMessageBuilder.new("key", "value", 7).build()
            val topicPartitionOffsets = mockk<TopicPartitionOffsets>(relaxed = true)
            every { topicPartitionOffsets.acknowledge(message1) } returns true
            val sut = createSut(
                topicPartitionOffsets = topicPartitionOffsets
            )

            sut.offerMessage(message1)

            // act
            message1.ack()

            // asset
            verify { topicPartitionOffsets.acknowledge(message1) }
        }

        test("incoming message can commit itself") {
            // arrange
            val message1 = topicPartitionMessageBuilder.new("key", "value", 7).build()
            val topicPartitionOffsets = mockk<TopicPartitionOffsets>(relaxed = true)
            every { topicPartitionOffsets.commit(message1) } returns CompletableDeferred(Unit)
            val sut = createSut(
                topicPartitionOffsets = topicPartitionOffsets
            )
            sut.offerMessage(message1)

            // act
            message1.commit()

            // asset
            verify { topicPartitionOffsets.commit(message1) }
        }

        test("should update assigned offset") {
            // arrange
            val sut = createSut()
            // act
            sut.updateAssignedOffset(20)
            // asset
            sut.assignedOffset shouldBe 20
        }

        test("should not send message and returns WorkerClosed when not send to worker") {
            // arrange
            val sut = createSut()

            val message1 = topicPartitionMessageBuilder.new("key", "value", 7).build()

            // act
            sut.offerMessage(message1)
            sut.stopWorker()
            val result = sut.offerMessage(message1)

            // asset
            result shouldBe EnqueueResult.WorkerClosed
        }

        test("should send message and returns Ok") {
            // arrange
            val message1 = topicPartitionMessageBuilder.new("key", "value", 7).build()
            val topicPartitionOffsets = mockk<TopicPartitionOffsets>(relaxed = true)
            every { topicPartitionOffsets.track(message1) } just Runs
            val sut = createSut(
                topicPartitionOffsets = topicPartitionOffsets
            )

            // act
            val result = sut.offerMessage(message1)

            // asset
            result shouldBe EnqueueResult.Ok
            verify { topicPartitionOffsets.track(message1) }
        }

        test("should not send message and returns Backpressure when backpressure enabled") {
            // arrange
            val message1 = topicPartitionMessageBuilder.new("key", "value", 7).build()
            val message2 = topicPartitionMessageBuilder.new("key", "value", 8).build()
            val message3 = topicPartitionMessageBuilder.new("key", "value", 9).build()

            val topicPartitionOffsets = mockk<TopicPartitionOffsets>(relaxed = true)
            every { topicPartitionOffsets.totalOffset() } returnsMany listOf(0, 1, 2)
            every { topicPartitionOffsets.track(any()) } just Runs

            val sut = createSut(
                subscriptionOptions = mockk {
                    every { backpressureBufferSize } returns 2
                    every { backpressureReleaseTimeout } returns 10.seconds
                },
                topicPartitionOffsets = topicPartitionOffsets
            )
            sut.offerMessage(message1)
            sut.offerMessage(message2)

            // act
            val result = sut.offerMessage(message3)

            // asset
            result shouldBe EnqueueResult.Backpressure
            sut.isBackpressureActive shouldBe true
        }

        test("should release backpressure") {
            // arrange

            val topicPartitionOffsets = mockk<TopicPartitionOffsets>(relaxed = true)
            every { topicPartitionOffsets.totalOffset() } returnsMany listOf(0, 1, 2, 0, 0)
            every { topicPartitionOffsets.track(any()) } just Runs

            val sut = createSut(
                subscriptionOptions = mockk(relaxed = true) {
                    every { backpressureBufferSize } returns 2
                    every { backpressureReleaseTimeout } returns 10.seconds
                },
                topicPartitionOffsets = topicPartitionOffsets
            )
            val message1 = topicPartitionMessageBuilder.new("key", "value", 1).build()
            val message2 = topicPartitionMessageBuilder.new("key", "value", 2).build()
            val message3 = topicPartitionMessageBuilder.new("key", "value", 3).build()
            sut.offerMessage(message1)
            sut.offerMessage(message2)
            val firstAttempt = sut.offerMessage(message3)

            // act
            sut.releaseBackpressureIfPossible()

            val secondAttempt = sut.offerMessage(message3)

            // asset
            sut.isBackpressureActive shouldBe false
            firstAttempt shouldBe EnqueueResult.Backpressure
            secondAttempt shouldBe EnqueueResult.Ok
        }

        test("should pause if not paused yet") {
            // arrange
            val sut = createSut()
            // act
            val message1 = topicPartitionMessageBuilder.new("key", "value", 7).build()
            sut.pause(message1, autoResumeTimeout = 5.seconds)

            // asset
            sut.isPaused shouldBe true
            sut.latestPausedOffset shouldBe 7
            sut.hasPendingResumeJob shouldBe true
        }

        test("should change paused offset with greater offset when already paused with lower offset") {
            // arrange
            val sut = createSut()
            val message1 = topicPartitionMessageBuilder.new("key", "value", 7).build()
            sut.pause(message1)

            // act
            val message2 = topicPartitionMessageBuilder.new("key", "value", 9).build()
            sut.pause(message2, autoResumeTimeout = 5.seconds)

            // asset
            sut.isPaused shouldBe true
            sut.latestPausedOffset shouldBe 9
            sut.hasPendingResumeJob shouldBe true
        }

        test("should skip pausing with lower offset when already paused with greater offset") {
            // arrange
            val sut = createSut()
            val message1 = topicPartitionMessageBuilder.new("key", "value", 7).build()
            sut.pause(message1)

            // act
            val topic1Message2 = IncomingMessageBuilder(
                topic = "topic1",
                partition = 1,
                offset = 3,
                key = "key",
                value = "value"
            ).build()
            sut.pause(topic1Message2)

            // asset
            sut.isPaused shouldBe true
            sut.latestPausedOffset shouldBe 7
        }

        test("should resume from last paused offset") {
            // arrange
            val sut = createSut()
            val message1 = topicPartitionMessageBuilder.new("key", "value", 7).build()
            sut.pause(message1)

            // act
            sut.resume()

            // asset
            sut.isPaused shouldBe false
            sut.latestPausedOffset shouldBe 7
            sut.hasPendingResumeJob shouldBe false
        }

        test("should resume from specific offset") {
            // arrange
            val sut = createSut()
            val message1 = topicPartitionMessageBuilder.new("key", "value", 7).build()
            sut.pause(message1)

            // act
            sut.resume(15)

            // asset
            sut.isPaused shouldBe false
            sut.latestPausedOffset shouldBe 7
            sut.hasPendingResumeJob shouldBe false
        }

        test("should force resume even when not paused") {
            // arrange
            val sut = createSut()

            // act
            sut.resume(15, force = true)

            // asset
            sut.isPaused shouldBe false
        }

        test("should not resume when not forced and not paused") {
            // arrange
            val sut = createSut()

            // act
            sut.resume(15, force = false)

            // asset
            sut.isPaused shouldBe false
        }

        test("should schedule auto-resume job with timeout") {
            // arrange
            val sut = createSut()
            val message1 = topicPartitionMessageBuilder.new("key", "value", 7).build()

            // act
            sut.pause(message1, autoResumeTimeout = 1.seconds)

            // asset
            sut.isPaused shouldBe true
            sut.hasPendingResumeJob shouldBe true
        }

        test("should not schedule auto-resume when timeout is zero") {
            // arrange
            val sut = createSut()
            val message1 = topicPartitionMessageBuilder.new("key", "value", 7).build()

            // act
            sut.pause(message1, autoResumeTimeout = 0.seconds)

            // asset
            sut.isPaused shouldBe true
            sut.hasPendingResumeJob shouldBe false
        }

        test("should cancel previous auto-resume job when scheduling new one") {
            // arrange
            val sut = createSut()
            val message1 = topicPartitionMessageBuilder.new("key", "value", 7).build()
            val message2 = topicPartitionMessageBuilder.new("key", "value", 8).build()

            // act
            sut.pause(message1, autoResumeTimeout = 1.seconds)
            val firstJobExists = sut.hasPendingResumeJob
            sut.pause(message2, autoResumeTimeout = 1.seconds)

            // asset
            firstJobExists shouldBe true
            sut.hasPendingResumeJob shouldBe true
        }

        test("should acknowledge message only once") {
            // arrange
            val message1 = topicPartitionMessageBuilder.new("key", "value", 7).build()
            val topicPartitionOffsets = mockk<TopicPartitionOffsets>(relaxed = true)
            every { topicPartitionOffsets.acknowledge(message1) } returns true
            val sut = createSut(topicPartitionOffsets = topicPartitionOffsets)
            sut.offerMessage(message1)

            // act
            message1.ack()
            message1.ack() // second call should be ignored

            // asset
            verify(exactly = 1) { topicPartitionOffsets.acknowledge(message1) }
        }

        test("should commit message only once") {
            // arrange
            val message1 = topicPartitionMessageBuilder.new("key", "value", 7).build()
            val topicPartitionOffsets = mockk<TopicPartitionOffsets>(relaxed = true)
            every { topicPartitionOffsets.commit(message1) } returns CompletableDeferred(Unit)
            val sut = createSut(topicPartitionOffsets = topicPartitionOffsets)
            sut.offerMessage(message1)

            // act
            message1.commit()
            val secondCommit = message1.commit() // second call should return completed deferred

            // asset
            verify(exactly = 1) { topicPartitionOffsets.commit(message1) }
            secondCommit.isCompleted shouldBe true
        }

        test("should not release backpressure when total offset is not zero") {
            // arrange
            val topicPartitionOffsets = mockk<TopicPartitionOffsets>(relaxed = true)
            every { topicPartitionOffsets.totalOffset() } returnsMany listOf(0, 1, 2, 1) // not zero
            every { topicPartitionOffsets.track(any()) } just Runs

            val sut = createSut(
                subscriptionOptions = mockk {
                    every { backpressureBufferSize } returns 2
                    every { backpressureReleaseTimeout } returns 10.seconds
                },
                topicPartitionOffsets = topicPartitionOffsets
            )
            val message1 = topicPartitionMessageBuilder.new("key", "value", 1).build()
            val message2 = topicPartitionMessageBuilder.new("key", "value", 2).build()
            val message3 = topicPartitionMessageBuilder.new("key", "value", 3).build()
            sut.offerMessage(message1)
            sut.offerMessage(message2)
            sut.offerMessage(message3) // triggers backpressure

            // act
            sut.releaseBackpressureIfPossible()

            // asset
            sut.isBackpressureActive shouldBe true
        }

        test("should close and cleanup resources") {
            // arrange
            val offsetManager = mockk<OffsetManager<String, String>>(relaxed = true)
            val sut = createSut(offsetManager = offsetManager)
            val message1 = topicPartitionMessageBuilder.new("key", "value", 7).build()
            sut.pause(message1, autoResumeTimeout = 1.seconds) // create auto-resume job

            // act
            sut.close()

            // asset
            verify { offsetManager.unregister(topicPartition) }
        }

        test("should stop worker when closing") {
            // arrange
            val sut = createSut()
            val message1 = topicPartitionMessageBuilder.new("key", "value", 7).build()
            sut.offerMessage(message1) // initialize worker

            // act
            sut.close()

            // verify worker is stopped by trying to offer another message
            val message2 = topicPartitionMessageBuilder.new("key", "value", 8).build()
            val result = sut.offerMessage(message2)

            // asset
            result shouldBe EnqueueResult.WorkerClosed
        }

        test("should handle stopWorker when worker is not initialized") {
            // arrange
            val sut = createSut()

            // act & asset - should not throw
            sut.stopWorker()
        }

        test("should throw InvalidConfigurationException when subscription options not found") {
            // arrange
            val scope = CoroutineScope(Dispatchers.IO)
            val options: QuafkaConsumerOptions<String, String> = mockk(relaxed = true) {
                every { getSubscriptionOptionsByTopicName("test-topic") } returns null
            }
            val offsetManager: OffsetManager<String, String> = mockk(relaxed = true)

            // act & asset
            shouldThrow<InvalidConfigurationException> {
                AssignedTopicPartition.create(
                    topicPartition = topicPartition,
                    parentScope = scope,
                    quafkaConsumerOptions = options,
                    initialOffset = 10,
                    offsetManager = offsetManager
                )
            }.message shouldBe "Subscription options not found for topic: test-topic"
        }

        test("should handle message untracking when worker is closed") {
            // arrange
            val message1 = topicPartitionMessageBuilder.new("key", "value", 7).build()
            val topicPartitionOffsets = mockk<TopicPartitionOffsets>(relaxed = true)
            every { topicPartitionOffsets.track(message1) } just Runs
            every { topicPartitionOffsets.untrack(message1) } just Runs
            val sut = createSut(topicPartitionOffsets = topicPartitionOffsets)

            sut.stopWorker()

            // act
            val result = sut.offerMessage(message1)

            // asset
            result shouldBe EnqueueResult.WorkerClosed
            verify(exactly = 0) { topicPartitionOffsets.track(message1) }
            verify(exactly = 0) { topicPartitionOffsets.untrack(message1) }
        }
    })
