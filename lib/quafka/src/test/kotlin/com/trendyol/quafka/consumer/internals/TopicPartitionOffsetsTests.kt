package com.trendyol.quafka.consumer.internals

import com.trendyol.quafka.TopicPartitionBasedMessageBuilder
import com.trendyol.quafka.consumer.TopicPartition
import com.trendyol.quafka.consumer.configuration.CommitOptions
import io.kotest.assertions.throwables.shouldThrow
import io.kotest.core.spec.style.FunSpec
import io.kotest.matchers.*
import io.mockk.*
import kotlinx.coroutines.*

class TopicPartitionOffsetsTests :
    FunSpec({
        val topicPartition = TopicPartition("test-topic", 0)
        val defaultCommitOptions = CommitOptions()
        val topicPartitionMessageBuilder = TopicPartitionBasedMessageBuilder<String, String>(topicPartition)

        fun createSut(
            commitOptions: CommitOptions = defaultCommitOptions,
            job: Job = Job()
        ): TopicPartitionOffsets = TopicPartitionOffsets(commitOptions, mockk(relaxed = true), topicPartition, CoroutineScope(Dispatchers.IO + job))

        test("should track offset") {
            // arrange
            val sut = createSut()
            val message1 = topicPartitionMessageBuilder.new("key", "value", 7).build()

            // act
            sut.track(message1)

            // asset
            val offsetState = sut.getOffset(7)
            offsetState shouldNotBe null
            offsetState!!.commitMode shouldBe null
            offsetState.continuation shouldBe null
            offsetState.retryAttempt shouldBe 0
            offsetState.maxRetryAttempts shouldBe defaultCommitOptions.maxRetryAttemptOnFail
            offsetState.isCompleted shouldBe false
            offsetState.exception shouldBe null
            offsetState.isCommitMode shouldBe false
            offsetState.isAckMode shouldBe false
            offsetState.isSuccessfullyCompleted() shouldBe false
            offsetState.isReady shouldBe false
        }

        test("should untrack offset") {
            // arrange
            val sut = createSut()
            val message1 = topicPartitionMessageBuilder.new("key", "value", 7).build()
            sut.track(message1)

            // act
            sut.untrack(message1)

            // asset
            sut.getOffset(7) shouldBe null
            sut.totalOffset() shouldBe 0
        }

        test("should return total tracked offset size") {
            // arrange
            val sut = createSut()
            val message1 = topicPartitionMessageBuilder.new("key", "value", 7).build()
            sut.track(message1)

            // act
            val size = sut.totalOffset()

            // asset
            size shouldBe 1
        }

        test("should return pending offsets if there is a waiting commit") {
            // arrange
            val sut = createSut()
            val message1 = topicPartitionMessageBuilder.new("key", "value", 7).build()
            sut.track(message1)

            sut.commit(message1)

            // act
            val hasPendingCommit = sut.hasPendingCommit()

            // asset
            hasPendingCommit shouldBe true
        }

        test("should not return ready offsets when there is a gap") {
            // arrange
            val sut = createSut()
            val message1 = topicPartitionMessageBuilder.new("key", "value", 7).build()
            sut.track(message1)
            val message2 = topicPartitionMessageBuilder.new("key", "value", 4).build()
            sut.track(message2)
            val message3 = topicPartitionMessageBuilder.new("key", "value", 1).build()
            sut.track(message3)

            sut.acknowledge(message1)
            sut.acknowledge(message2)
            // act
            val readyOffsets = sut.getPendingOffsets()

            // asset
            readyOffsets.size shouldBe 0
        }

        test("should return only smallest part of offset when there is a gap between offsets") {
            // arrange
            val sut = createSut()

            val message2 = topicPartitionMessageBuilder.new("key", "value", 2).build()
            val message1 = topicPartitionMessageBuilder.new("key", "value", 1).build()
            val message3 = topicPartitionMessageBuilder.new("key", "value", 7).build()
            sut.track(message1)
            sut.track(message2)
            sut.track(message3)

            sut.acknowledge(message2)
            sut.acknowledge(message3)
            // assert
            sut.getPendingOffsets().size shouldBe 0
            // act
            sut.acknowledge(message1)
            val readyOffsets = sut.getPendingOffsets()
            readyOffsets.size shouldBe 3
            readyOffsets[0].offset shouldBe 1
            readyOffsets[1].offset shouldBe 2
            readyOffsets[2].offset shouldBe 7
            // assert
        }

        test("should acknowledge") {
            // arrange
            val sut = createSut()
            val message1 = topicPartitionMessageBuilder.new("key", "value", 7).build()
            sut.track(message1)

            // act
            val acked = sut.acknowledge(message1)

            // asset
            acked shouldBe true
            val readyOffsets = sut.getPendingOffsets()
            readyOffsets.size shouldBe 1
            readyOffsets[0].offset shouldBe 7
            readyOffsets[0].isCommitMode shouldBe false
            readyOffsets[0].isAckMode shouldBe true
            readyOffsets[0].isCompleted shouldBe false
        }

        test("should acknowledge with lower offset when allowOutOfOrderCommit is true and deferCommitsUntilNoGaps is false") {
            // arrange
            val sut = createSut(
                commitOptions = mockk {
                    every { allowOutOfOrderCommit } returns true
                    every { deferCommitsUntilNoGaps } returns false
                    every { maxRetryAttemptOnFail } returns 0
                }
            )

            val message1 = topicPartitionMessageBuilder.new("key", "value", 7).build()
            sut.track(message1)
            sut.acknowledge(message1)

            // act
            val message2 = topicPartitionMessageBuilder.new("key", "value", 3).build()
            sut.track(message2)
            val acked = sut.acknowledge(message2)

            // asset
            acked shouldBe true
            val readyOffsets = sut.getPendingOffsets().sortedBy { it.offset }
            readyOffsets.size shouldBe 2
            readyOffsets[0].offset shouldBe 3
            readyOffsets[0].isCommitMode shouldBe false
            readyOffsets[0].isAckMode shouldBe true
            readyOffsets[0].isCompleted shouldBe false
            readyOffsets[1].offset shouldBe 7
            readyOffsets[1].isCommitMode shouldBe false
            readyOffsets[1].isAckMode shouldBe true
            readyOffsets[1].isCompleted shouldBe false
        }

        test("should not acknowledge with lower offset when allowOutOfOrderCommit is false") {
            // arrange
            val sut = createSut()
            val message1 = topicPartitionMessageBuilder.new("key", "value", 7).build()
            sut.track(message1)
            sut.acknowledge(message1)
            sut.completeOffsets(
                listOf(
                    sut.getPendingOffsets().first()
                ),
                null
            )

            // act
            val message2 = topicPartitionMessageBuilder.new("key", "value", 3).build()
            sut.track(message2)
            val acked = sut.acknowledge(message2)

            // asset
            acked shouldBe false
            val readyOffsets = sut.getPendingOffsets().sortedBy { it.offset }
            readyOffsets.size shouldBe 0
        }

        test("should not commit with lower offset when allowOutOfOrderCommit is false") {
            // arrange
            val sut = createSut()
            val message1 = topicPartitionMessageBuilder.new("key", "value", 7).build()
            sut.track(message1)
            sut.acknowledge(message1)
            sut.completeOffsets(
                listOf(
                    sut.getPendingOffsets().first()
                ),
                null
            )

            // act
            val message2 = topicPartitionMessageBuilder.new("key", "value", 3).build()
            sut.track(message2)
            val commitJob = sut.commit(message2)

            // asset
            commitJob.isCompleted shouldBe true
            val readyOffsets = sut.getPendingOffsets().sortedBy { it.offset }
            readyOffsets.size shouldBe 0
        }

        test("should commit") {
            // arrange
            val sut = createSut()
            val message1 = topicPartitionMessageBuilder.new("key", "value", 7).build()
            sut.track(message1)

            // act
            val commitJob = sut.commit(message1)

            // assert
            val readyOffsets = sut.getPendingOffsets()
            readyOffsets.size shouldBe 1
            readyOffsets[0].offset shouldBe 7
            readyOffsets[0].isCommitMode shouldBe true
            readyOffsets[0].isCompleted shouldBe false

            val waitJob = async {
                delay(500)
                sut.completeOffsets(
                    listOf(
                        sut.getPendingOffsets().first()
                    ),
                    null
                )
            }
            commitJob.await()

            // asset
            sut.getPendingOffsets().size shouldBe 0
            sut.getLatestCommitedOffset() shouldBe 7

            // clear
            waitJob.cancelAndJoin()
        }

        test("should throw exception when commit failed") {
            // arrange
            val sut = createSut(
                commitOptions = mockk {
                    every { allowOutOfOrderCommit } returns false
                    every { deferCommitsUntilNoGaps } returns false
                    every { maxRetryAttemptOnFail } returns 0
                }
            )
            val message1 = topicPartitionMessageBuilder.new("key", "value", 7).build()
            sut.track(message1)

            // act
            val commitJob = sut.commit(message1)

            // assert
            val readyOffsets = sut.getPendingOffsets()
            val waitJob = async {
                delay(500)
                sut.completeOffsets(
                    listOf(
                        readyOffsets.first()
                    ),
                    Exception("failed")
                )
            }

            val exception = shouldThrow<Exception> {
                commitJob.await()
            }

            // asset
            exception.message shouldBe "failed"
            sut.getPendingOffsets().size shouldBe 0
            sut.getLatestCommitedOffset() shouldNotBe 7

            // clear
            waitJob.cancelAndJoin()
        }

        test("should throw exception when commit failed two times") {
            // arrange
            val sut = createSut(
                commitOptions = mockk {
                    every { allowOutOfOrderCommit } returns false
                    every { deferCommitsUntilNoGaps } returns false
                    every { maxRetryAttemptOnFail } returns 1
                }
            )
            val message1 = topicPartitionMessageBuilder.new("key", "value", 7).build()
            sut.track(message1)

            // act
            val commitJob = sut.commit(message1)

            // assert

            val waitJob = async {
                delay(500)
                val readyOffsets = sut.getPendingOffsets()
                sut.completeOffsets(
                    listOf(
                        readyOffsets.first()
                    ),
                    Exception("failed")
                )
                val readyOffsets2 = sut.getPendingOffsets()
                sut.completeOffsets(
                    listOf(
                        readyOffsets2.first()
                    ),
                    Exception("failed")
                )
            }

            val exception = shouldThrow<Exception> {
                commitJob.await()
            }

            // asset
            exception.message shouldBe "failed"
            sut.getPendingOffsets().size shouldBe 0
            sut.getLatestCommitedOffset() shouldNotBe 7

            // clear
            waitJob.cancelAndJoin()
        }

        test("should return empty list when no offsets tracked") {
            // arrange
            val sut = createSut()

            // act
            val pendingOffsets = sut.getPendingOffsets()

            // asset
            pendingOffsets shouldBe emptyList()
        }

        test("should return all ready offsets when deferCommitsUntilNoGaps is false") {
            // arrange
            val sut = createSut(
                commitOptions = mockk {
                    every { deferCommitsUntilNoGaps } returns false
                    every { allowOutOfOrderCommit } returns true
                    every { maxRetryAttemptOnFail } returns 0
                }
            )

            val message1 = topicPartitionMessageBuilder.new("key", "value", 1).build()
            val message2 = topicPartitionMessageBuilder.new("key", "value", 5).build()
            val message3 = topicPartitionMessageBuilder.new("key", "value", 3).build()

            sut.track(message1)
            sut.track(message2)
            sut.track(message3)

            sut.acknowledge(message1)
            sut.commit(message2)
            // message3 not acknowledged, so not ready

            // act
            val pendingOffsets = sut.getPendingOffsets()

            // asset
            pendingOffsets.size shouldBe 2
            pendingOffsets[0].offset shouldBe 1 // sorted by offset
            pendingOffsets[1].offset shouldBe 5
        }

        test("should return empty list when first offset is not ready") {
            // arrange
            val sut = createSut() // deferCommitsUntilNoGaps is true by default

            val message1 = topicPartitionMessageBuilder.new("key", "value", 1).build()
            val message2 = topicPartitionMessageBuilder.new("key", "value", 2).build()

            sut.track(message1)
            sut.track(message2)

            // Only acknowledge message2, not message1
            sut.acknowledge(message2)

            // act
            val pendingOffsets = sut.getPendingOffsets()

            // asset
            pendingOffsets shouldBe emptyList()
        }

        test("should test canBeCommitted when offset exists and is ready") {
            // arrange
            val sut = createSut()
            val message1 = topicPartitionMessageBuilder.new("key", "value", 7).build()
            sut.track(message1)
            sut.acknowledge(message1)

            // act
            val canCommit = sut.canBeCommitted(7)

            // asset
            canCommit shouldBe true
        }

        test("should test canBeCommitted when offset does not exist") {
            // arrange
            val sut = createSut()

            // act
            val canCommit = sut.canBeCommitted(7)

            // asset
            canCommit shouldBe false
        }

        test("should test canBeCommitted when offset is not ready") {
            // arrange
            val sut = createSut()
            val message1 = topicPartitionMessageBuilder.new("key", "value", 7).build()
            sut.track(message1)
            // Don't acknowledge or commit

            // act
            val canCommit = sut.canBeCommitted(7)

            // asset
            canCommit shouldBe false
        }

        test("should test canBeCommitted when offset is less than latest committed") {
            // arrange
            val sut = createSut()
            val message1 = topicPartitionMessageBuilder.new("key", "value", 10).build()
            val message2 = topicPartitionMessageBuilder.new("key", "value", 5).build()

            sut.track(message1)
            sut.acknowledge(message1)
            sut.completeOffsets(listOf(sut.getOffset(10)!!)) // commit offset 10

            sut.track(message2)
            sut.acknowledge(message2)

            // act
            val canCommit = sut.canBeCommitted(5)

            // asset
            canCommit shouldBe false
        }

        test("should return latest committed offset") {
            // arrange
            val sut = createSut()
            val message1 = topicPartitionMessageBuilder.new("key", "value", 15).build()
            sut.track(message1)
            sut.acknowledge(message1)
            sut.completeOffsets(listOf(sut.getOffset(15)!!))

            // act
            val latestOffset = sut.getLatestCommitedOffset()

            // asset
            latestOffset shouldBe 15
        }

        test("should return unassigned offset when no commits made") {
            // arrange
            val sut = createSut()

            // act
            val latestOffset = sut.getLatestCommitedOffset()

            // asset
            latestOffset shouldBe AssignedTopicPartition.UNASSIGNED_OFFSET
        }

        test("should not acknowledge non-tracked message") {
            // arrange
            val sut = createSut()
            val message1 = topicPartitionMessageBuilder.new("key", "value", 7).build()
            // Don't track the message

            // act
            val acked = sut.acknowledge(message1)

            // asset
            acked shouldBe false
        }

        test("should not commit non-tracked message") {
            // arrange
            val sut = createSut()
            val message1 = topicPartitionMessageBuilder.new("key", "value", 7).build()
            // Don't track the message

            // act
            val commitJob = sut.commit(message1)

            // asset
            commitJob.isCompleted shouldBe true
        }

        test("should handle commit after acknowledge") {
            // arrange
            val sut = createSut()
            val message1 = topicPartitionMessageBuilder.new("key", "value", 7).build()
            sut.track(message1)
            sut.acknowledge(message1)

            // act
            val commitJob = sut.commit(message1)

            // asset
            commitJob.isCompleted shouldBe false
            sut.getOffset(7)!!.isAckMode shouldBe false
            sut.getOffset(7)!!.isCommitMode shouldBe true
        }

        test("should handle acknowledge after commit") {
            // arrange
            val sut = createSut()
            val message1 = topicPartitionMessageBuilder.new("key", "value", 7).build()
            sut.track(message1)
            sut.commit(message1)

            // act
            val acked = sut.acknowledge(message1)

            // asset
            // Should not change the state since it's already in commit mode
            acked shouldBe true
            sut.getOffset(7)!!.isAckMode shouldBe false
            sut.getOffset(7)!!.isCommitMode shouldBe true
        }

        test("should handle multiple complete operations") {
            // arrange
            val sut = createSut()
            val message1 = topicPartitionMessageBuilder.new("key", "value", 7).build()
            sut.track(message1)
            sut.acknowledge(message1)

            val offsetState = sut.getOffset(7)!!

            // act - complete twice
            sut.completeOffsets(listOf(offsetState))
            sut.completeOffsets(listOf(offsetState)) // Should not cause issues

            // asset
            sut.getOffset(7) shouldBe null // Should be removed after completion
            sut.getLatestCommitedOffset() shouldBe 7
        }

        test("should not update latest committed offset when completing with exception") {
            // arrange
            val sut = createSut()
            val message1 = topicPartitionMessageBuilder.new("key", "value", 7).build()
            sut.track(message1)
            sut.acknowledge(message1)

            val offsetState = sut.getOffset(7)!!

            // act
            sut.completeOffsets(listOf(offsetState), RuntimeException("test error"))

            // asset
            sut.getOffset(7)!!.retryAttempt shouldBe 1
            sut.getLatestCommitedOffset() shouldNotBe 7 // Should not be updated
        }

        test("should keep latest committed offset to highest value") {
            // arrange
            val sut = createSut()
            val message1 = topicPartitionMessageBuilder.new("key", "value", 5).build()
            val message2 = topicPartitionMessageBuilder.new("key", "value", 10).build()
            val message3 = topicPartitionMessageBuilder.new("key", "value", 3).build()

            sut.track(message1)
            sut.track(message2)
            sut.track(message3)

            sut.acknowledge(message1)
            sut.acknowledge(message2)
            sut.acknowledge(message3)

            // act - complete in non-sequential order
            sut.completeOffsets(listOf(sut.getOffset(10)!!)) // Complete highest first
            val latestAfterFirst = sut.getLatestCommitedOffset()

            sut.completeOffsets(listOf(sut.getOffset(3)!!)) // Complete lowest
            val latestAfterSecond = sut.getLatestCommitedOffset()

            sut.completeOffsets(listOf(sut.getOffset(5)!!)) // Complete middle
            val latestAfterThird = sut.getLatestCommitedOffset()

            // asset
            latestAfterFirst shouldBe 10
            latestAfterSecond shouldBe 10 // Should not decrease
            latestAfterThird shouldBe 10 // Should not change
        }

        test("should track message with custom retry attempts") {
            // arrange
            val customCommitOptions = mockk<CommitOptions> {
                every { maxRetryAttemptOnFail } returns 5
                every { deferCommitsUntilNoGaps } returns true
                every { allowOutOfOrderCommit } returns false
            }
            val sut = createSut(customCommitOptions)
            val message1 = topicPartitionMessageBuilder.new("key", "value", 7).build()

            // act
            sut.track(message1)

            // asset
            val offsetState = sut.getOffset(7)
            offsetState shouldNotBe null
            offsetState!!.maxRetryAttempts shouldBe 5
        }

        test("should handle retry logic correctly") {
            // arrange
            val sut = createSut(
                commitOptions = mockk {
                    every { allowOutOfOrderCommit } returns false
                    every { deferCommitsUntilNoGaps } returns false
                    every { maxRetryAttemptOnFail } returns 2
                }
            )
            val message1 = topicPartitionMessageBuilder.new("key", "value", 7).build()
            sut.track(message1)

            val commitJob = sut.commit(message1)
            val offsetState = sut.getOffset(7)!!

            // act - fail first time
            sut.completeOffsets(listOf(offsetState), RuntimeException("first failure"))

            // asset - should still be in pending list for retry
            val pendingAfterFirstFailure = sut.getPendingOffsets()
            pendingAfterFirstFailure.size shouldBe 1
            pendingAfterFirstFailure[0].retryAttempt shouldBe 1
            pendingAfterFirstFailure[0].isCompleted shouldBe false

            // act - fail second time
            sut.completeOffsets(listOf(pendingAfterFirstFailure[0]), RuntimeException("second failure"))

            // asset - should still be pending for one more retry
            val pendingAfterSecondFailure = sut.getPendingOffsets()
            pendingAfterSecondFailure.size shouldBe 1
            pendingAfterSecondFailure[0].retryAttempt shouldBe 2
            pendingAfterSecondFailure[0].isCompleted shouldBe false

            // act - fail third time (exceeds max retries)
            sut.completeOffsets(listOf(pendingAfterSecondFailure[0]), RuntimeException("final failure"))

            // asset - should be completed and removed
            sut.getPendingOffsets().size shouldBe 0
            sut.getOffset(7) shouldBe null
            commitJob.isCompleted shouldBe true
        }

        test("should handle isValidForCommit correctly with allowOutOfOrderCommit") {
            // arrange
            val sut = createSut(
                commitOptions = mockk {
                    every { allowOutOfOrderCommit } returns true
                    every { deferCommitsUntilNoGaps } returns false
                    every { maxRetryAttemptOnFail } returns 0
                }
            )

            val message1 = topicPartitionMessageBuilder.new("key", "value", 10).build()
            val message2 = topicPartitionMessageBuilder.new("key", "value", 5).build()

            sut.track(message1)
            sut.acknowledge(message1)
            sut.completeOffsets(listOf(sut.getOffset(10)!!)) // Latest committed = 10

            sut.track(message2)

            // act - should allow acknowledging lower offset when allowOutOfOrderCommit is true
            val acked = sut.acknowledge(message2)

            // asset
            acked shouldBe true
        }

        test("should not have pending commits when no commits made") {
            // arrange
            val sut = createSut()
            val message1 = topicPartitionMessageBuilder.new("key", "value", 7).build()
            sut.track(message1)
            sut.acknowledge(message1) // Only acknowledge, don't commit

            // act
            val hasPending = sut.hasPendingCommit()

            // asset
            hasPending shouldBe false
        }
    })
