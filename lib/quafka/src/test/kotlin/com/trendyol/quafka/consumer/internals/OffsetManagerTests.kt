package com.trendyol.quafka.consumer.internals

import com.trendyol.quafka.*
import com.trendyol.quafka.consumer.TopicPartition
import com.trendyol.quafka.consumer.configuration.CommitOptions
import io.kotest.assertions.throwables.shouldThrow
import io.kotest.core.spec.style.*
import io.kotest.matchers.*
import io.mockk.*
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Job
import org.apache.kafka.clients.consumer.*
import org.slf4j.*

@Suppress("UNCHECKED_CAST")
class OffsetManagerTests :
    FunSpec({
        lateinit var consumer: KafkaConsumer<String, String>
        lateinit var sut: OffsetManager<String, String>
        val logger: Logger = mockk(relaxed = true)

        val topicPartition0 = TopicPartition("topic1", 0)
        val topicPartition0MessageBuilder = TopicPartitionBasedMessageBuilder<String, String>(topicPartition0)

        beforeEach {
            consumer = mockk(relaxed = true)
            sut = OffsetManager(
                kafkaConsumer = consumer,
                quafkaConsumerOptions = mockk(relaxed = true) {
                    every { commitOptions } returns CommitOptions(maxRetryAttemptOnFail = 3)
                },
                logger = logger
            )
        }

        test("should flush offset async with fire and forget") {
            // arrange
            val tracker = sut.register(topicPartition0, CoroutineScope(Job()))
            val message1 = topicPartition0MessageBuilder.new("key", "value", 5).build()
            tracker.track(message1)
            tracker.acknowledge(message1)

            val committingOffset = slot<Map<TopicPartition, OffsetAndMetadata>>()
            every { consumer.commitAsync(capture(committingOffset), any()) } returns Unit

            // act
            sut.flushOffsetsAsync(fireAndForget = true)

            // assert
            committingOffset.captured shouldBe mapOf(
                topicPartition0 to OffsetAndMetadata(6)
            )
        }

        test("should complete offsets with exception when error occurred flushing offset async") {
            // arrange
            val tracker = sut.register(topicPartition0, CoroutineScope(Job()))
            val message1 = topicPartition0MessageBuilder.new("key", "value", 5).build()
            tracker.track(message1)
            tracker.acknowledge(message1)

            val exception = Exception("an error occurred")
            every { consumer.commitAsync(any(), any()) } throws exception

            // act
            val expectedEx = shouldThrow<Exception> { sut.flushOffsetsAsync() }

            // assert
            sut.getOffset(topicPartition0, 5)!!.exception shouldBe exception
            expectedEx shouldBe exception
        }

        test("should try commit async again when an error happens when committing") {
            // arrange
            val tracker = sut.register(topicPartition0, CoroutineScope(Job()))
            val message1 = topicPartition0MessageBuilder.new("key", "value", 5).build()
            tracker.track(message1)
            tracker.acknowledge(message1)

            val committingOffset = slot<Map<TopicPartition, OffsetAndMetadata>>()
            val exception = Exception("an error occurred")
            every { consumer.commitAsync(capture(committingOffset), any()) } answers {
                val callback: OffsetCommitCallback = it.invocation.args[1] as OffsetCommitCallback
                callback.onComplete(it.invocation.args[0] as MutableMap<TopicPartition, OffsetAndMetadata>, exception)
            } andThenAnswer {
                val callback: OffsetCommitCallback = it.invocation.args[1] as OffsetCommitCallback
                callback.onComplete(it.invocation.args[0] as MutableMap<TopicPartition, OffsetAndMetadata>, null)
            }

            // act
            sut.flushOffsetsAsync()

            // assert
            committingOffset.captured shouldBe mapOf(
                topicPartition0 to OffsetAndMetadata(6)
            )
        }

        test("should flush offset async and wait for completion") {
            // arrange
            val tracker = sut.register(topicPartition0, CoroutineScope(Job()))
            val message1 = topicPartition0MessageBuilder.new("key", "value", 5).build()
            tracker.track(message1)
            tracker.acknowledge(message1)

            val committingOffset = slot<Map<TopicPartition, OffsetAndMetadata>>()
            every { consumer.commitAsync(capture(committingOffset), any()) } answers {
                val callback: OffsetCommitCallback = it.invocation.args[1] as OffsetCommitCallback
                callback.onComplete(it.invocation.args[0] as MutableMap<TopicPartition, OffsetAndMetadata>, null)
            }

            // act
            sut.flushOffsetsAsync()

            // assert
            committingOffset.captured shouldBe mapOf(
                topicPartition0 to OffsetAndMetadata(6)
            )
        }

        test("should flush offsets sync and complete offsets") {
            // arrange
            val tracker = sut.register(topicPartition0, CoroutineScope(Job()))
            val message1 = topicPartition0MessageBuilder.new("key", "value", 5).build()
            tracker.track(message1)
            tracker.acknowledge(message1)

            val committingOffset = slot<Map<TopicPartition, OffsetAndMetadata>>()
            every { consumer.commitSync(capture(committingOffset)) } returns Unit

            // act
            sut.flushOffsetsSync()

            // assert
            committingOffset.captured shouldBe mapOf(
                topicPartition0 to OffsetAndMetadata(6)
            )
        }

        test("should complete offsets with exception when error occurred flushing offset sync") {
            val tracker = sut.register(topicPartition0, CoroutineScope(Job()))
            val message1 = topicPartition0MessageBuilder.new("key", "value", 5).build()
            tracker.track(message1)
            tracker.acknowledge(message1)

            // arrange
            val exception = Exception("an error occurred")
            every { consumer.commitSync(any<Map<TopicPartition, OffsetAndMetadata>>()) } throws exception

            // act
            val expectedEx = shouldThrow<Exception> { sut.flushOffsetsSync() }

            // assert
            sut.getOffset(topicPartition0, 5)!!.exception shouldBe exception
            expectedEx shouldBe exception
        }
    })
