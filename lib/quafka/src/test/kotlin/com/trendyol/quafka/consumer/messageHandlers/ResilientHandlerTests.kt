package com.trendyol.quafka.consumer.messageHandlers

import com.trendyol.quafka.consumer.*
import com.trendyol.quafka.consumer.Events.toDetail
import com.trendyol.quafka.consumer.configuration.QuafkaConsumerOptions
import com.trendyol.quafka.events.*
import io.kotest.assertions.nondeterministic.eventually
import io.kotest.assertions.throwables.shouldThrow
import io.kotest.core.spec.style.FunSpec
import io.kotest.matchers.collections.shouldHaveSize
import io.kotest.matchers.longs.shouldBeGreaterThan
import io.kotest.matchers.shouldBe
import io.mockk.*
import kotlinx.coroutines.*
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.header.internals.RecordHeaders
import org.apache.kafka.common.record.TimestampType
import java.time.Instant
import java.util.Optional
import java.util.concurrent.atomic.AtomicInteger
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds

class ResilientHandlerTests :
    FunSpec({

        fun createTestMessage(key: String = "test-key", value: String = "test-value"): IncomingMessage<String, String> {
            val consumerRecord = ConsumerRecord(
                "test-topic",
                0,
                0L,
                Instant.now().toEpochMilli(),
                TimestampType.NO_TIMESTAMP_TYPE,
                0,
                0,
                key,
                value,
                RecordHeaders(),
                Optional.empty()
            )

            return IncomingMessage.create(
                consumerRecord,
                IncomingMessageStringFormatter(),
                "test-group",
                "test-client",
                object : Acknowledgment {
                    override fun acknowledge(incomingMessage: IncomingMessage<*, *>) {
                        // no-op for tests
                    }

                    override fun commit(incomingMessage: IncomingMessage<*, *>) =
                        kotlinx.coroutines.CompletableDeferred(Unit)
                }
            )
        }

        fun createTestContext(eventBus: EventBus = EventBus()): ConsumerContext {
            val consumerOptions = mockk<QuafkaConsumerOptions<String, String>>(relaxed = true) {
                every { this@mockk.eventBus } returns eventBus
                every { getGroupId() } returns "test-group"
                every { getClientId() } returns "test-client"
            }
            return ConsumerContext(
                topicPartition = TopicPartition("test-topic", 0),
                coroutineContext = Dispatchers.Default,
                consumerOptions = consumerOptions
            )
        }

        context("Retrying Handler") {
            test("should retry on failure until success") {
                // arrange
                val handler = ResilientHandler.Retrying.basic<String, String>(initialInterval = 50.milliseconds)
                val message = createTestMessage()
                val context = createTestContext()
                val attemptCount = AtomicInteger(0)

                // act
                handler.handle(context, listOf(message)) { _, _ ->
                    val attempt = attemptCount.incrementAndGet()
                    if (attempt < 3) {
                        throw RuntimeException("Simulated error on attempt $attempt")
                    }
                    // Success on 3rd attempt
                }

                // assert
                attemptCount.get() shouldBe 3
            }

            test("should use exponential backoff") {
                // arrange
                val handler = ResilientHandler.Retrying.basic<String, String>(initialInterval = 100.milliseconds)
                val message = createTestMessage()
                val context = createTestContext()
                val timestamps = mutableListOf<Long>()

                // act
                handler.handle(context, listOf(message)) { _, _ ->
                    timestamps.add(System.currentTimeMillis())
                    if (timestamps.size < 3) {
                        throw RuntimeException("Simulated error")
                    }
                }

                // assert - delays should increase
                timestamps.size shouldBe 3
                if (timestamps.size >= 3) {
                    val firstDelay = timestamps[1] - timestamps[0]
                    val secondDelay = timestamps[2] - timestamps[1]
                    // Second delay should be longer than first (exponential backoff)
                    secondDelay shouldBeGreaterThan firstDelay
                }
            }

            test("basic factory method should work with custom config") {
                // arrange
                val handler = ResilientHandler.Retrying.basic<String, String>(
                    initialInterval = 50.milliseconds,
                    maxAttempts = 2,
                    multiplier = 2.0
                )
                val message = createTestMessage()
                val context = createTestContext()
                val attemptCount = AtomicInteger(0)

                // act & assert
                shouldThrow<RuntimeException> {
                    handler.handle(context, listOf(message)) { _, _ ->
                        attemptCount.incrementAndGet()
                        throw RuntimeException("Always fails")
                    }
                }

                // Should have tried maxAttempts times
                attemptCount.get() shouldBe 2
            }
        }

        context("Skipping Handler") {
            test("should skip message on failure") {
                // arrange
                val handler = ResilientHandler.Skipping<String, String>()
                val message = createTestMessage()
                val context = createTestContext()
                var handlerCalled = false

                // act
                handler.handle(context, listOf(message)) { _, _ ->
                    handlerCalled = true
                    throw RuntimeException("Simulated error")
                }

                // assert
                handlerCalled shouldBe true
                // Should not throw - message is skipped
            }

            test("should succeed without error when handler succeeds") {
                // arrange
                val handler = ResilientHandler.Skipping<String, String>()
                val message = createTestMessage()
                val context = createTestContext()
                var processed = false

                // act
                handler.handle(context, listOf(message)) { _, _ ->
                    processed = true
                }

                // assert
                processed shouldBe true
            }
        }

        context("Stopping Handler") {
            test("should publish WorkerFailed event and throw exception") {
                // arrange
                val eventBus = EventBus(Dispatchers.Default)
                val events = mutableListOf<QuafkaEvent>()
                val subJob = launch(Dispatchers.Default) {
                    eventBus.subscribe {
                        events.add(it)
                    }
                }
                val handler = ResilientHandler.Stopping<String, String>()
                val message = createTestMessage()
                val context = createTestContext(eventBus)
                val testException = RuntimeException("Test error")

                try {
                    // act & assert
                    val thrownException = shouldThrow<RuntimeException> {
                        handler.handle(context, listOf(message)) { _, _ ->
                            throw testException
                        }
                    }

                    // assert
                    thrownException shouldBe testException
                    eventually(5.seconds) {
                        events[0] shouldBe Events.WorkerFailed(context.topicPartition, testException, context.consumerOptions.toDetail())
                    }
                } finally {
                    subJob.cancel()
                }
            }

            test("should not throw when handler succeeds") {
                // arrange
                val eventBus = EventBus(Dispatchers.Default)
                val events = mutableListOf<QuafkaEvent>()
                val subJob = launch(Dispatchers.Default) {
                    eventBus.subscribe {
                        events.add(it)
                    }
                }
                val handler = ResilientHandler.Stopping<String, String>()
                val message = createTestMessage()
                val context = createTestContext(eventBus)
                var processed = false
                try {
                    // act
                    handler.handle(context, listOf(message)) { _, _ ->
                        processed = true
                    }

                    // assert
                    processed shouldBe true
                    events.count() shouldBe 0
                } finally {
                    subJob.cancel()
                }
            }
        }

        context("WithSafetyNet Wrapper") {
            test("should use safety net when user handler fails") {
                // arrange
                val userHandler = ResilientHandler.Stopping<String, String>()
                val safetyNetHandler = ResilientHandler.Retrying.basic<String, String>(initialInterval = 50.milliseconds)
                val wrapper = ResilientHandler.WithSafetyNet(userHandler, safetyNetHandler)

                val message = createTestMessage()
                val context = createTestContext()
                val attemptCount = AtomicInteger(0)

                // act
                wrapper.handle(context, listOf(message)) { _, _ ->
                    val attempt = attemptCount.incrementAndGet()
                    if (attempt < 3) {
                        throw RuntimeException("Fail first 2 attempts")
                    }
                }

                // assert - should have retried via safety net
                attemptCount.get() shouldBe 3
            }

            test("should not invoke safety net when user handler succeeds") {
                // arrange
                val userHandler = ResilientHandler.Retrying.basic<String, String>(initialInterval = 50.milliseconds)
                val safetyNetHandler = ResilientHandler.Skipping<String, String>()
                val wrapper = ResilientHandler.WithSafetyNet(userHandler, safetyNetHandler)

                val message = createTestMessage()
                val context = createTestContext()
                var processed = false

                // act
                wrapper.handle(context, listOf(message)) { _, _ ->
                    processed = true
                }

                // assert
                processed shouldBe true
            }
        }

        context("Multiple Messages") {
            test("should handle batch of messages with retry") {
                // arrange
                val handler = ResilientHandler.Retrying.basic<String, String>(initialInterval = 50.milliseconds)
                val messages = listOf(
                    createTestMessage("key1", "value1"),
                    createTestMessage("key2", "value2"),
                    createTestMessage("key3", "value3")
                )
                val context = createTestContext()
                val attemptCount = AtomicInteger(0)
                val processedMessages = mutableListOf<String>()

                // act
                handler.handle(context, messages) { _, msgs ->
                    val attempt = attemptCount.incrementAndGet()
                    if (attempt < 2) {
                        throw RuntimeException("Fail on first attempt")
                    }
                    msgs.forEach { processedMessages.add(it.key) }
                }

                // assert
                attemptCount.get() shouldBe 2
                processedMessages shouldHaveSize 3
                processedMessages shouldBe listOf("key1", "key2", "key3")
            }

            test("should skip batch of messages on failure") {
                // arrange
                val handler = ResilientHandler.Skipping<String, String>()
                val messages = listOf(
                    createTestMessage("key1", "value1"),
                    createTestMessage("key2", "value2")
                )
                val context = createTestContext()

                // act
                handler.handle(context, messages) { _, _ ->
                    throw RuntimeException("Always fail")
                }

                // assert - should not throw
            }
        }

        context("Fatal Errors") {
            test("should not retry on OutOfMemoryError") {
                // arrange
                val handler = ResilientHandler.Retrying.basic<String, String>(initialInterval = 50.milliseconds)
                val message = createTestMessage()
                val context = createTestContext()
                val attemptCount = AtomicInteger(0)

                // act & assert
                shouldThrow<OutOfMemoryError> {
                    handler.handle(context, listOf(message)) { _, _ ->
                        attemptCount.incrementAndGet()
                        throw OutOfMemoryError("Simulated OOM")
                    }
                }

                // Should only try once (fatal error, no retry)
                attemptCount.get() shouldBe 1
            }

            test("should not retry on cancellation") {
                // arrange
                val handler = ResilientHandler.Retrying.basic<String, String>(initialInterval = 50.milliseconds)
                val message = createTestMessage()
                val context = createTestContext()
                val attemptCount = AtomicInteger(0)

                // act & assert
                shouldThrow<kotlinx.coroutines.CancellationException> {
                    handler.handle(context, listOf(message)) { _, _ ->
                        attemptCount.incrementAndGet()
                        throw kotlinx.coroutines.CancellationException("Cancelled")
                    }
                }

                // Should only try once (cancellation, no retry)
                attemptCount.get() shouldBe 1
            }
        }

        context("Performance and Timing") {
            test("should not block unnecessarily on success") {
                // arrange
                val handler = ResilientHandler.Retrying.basic<String, String>(initialInterval = 1.seconds)
                val message = createTestMessage()
                val context = createTestContext()
                val startTime = System.currentTimeMillis()

                // act
                handler.handle(context, listOf(message)) { _, _ ->
                    // Immediate success
                }

                // assert - should complete quickly (no retry delay)
                val duration = System.currentTimeMillis() - startTime
                (duration < 500) shouldBe true // Should be much faster than 1 second
            }
        }
    })
