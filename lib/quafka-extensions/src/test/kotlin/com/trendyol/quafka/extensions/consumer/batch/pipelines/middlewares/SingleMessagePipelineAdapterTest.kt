package com.trendyol.quafka.extensions.consumer.batch.pipelines.middlewares

import com.trendyol.quafka.consumer.*
import com.trendyol.quafka.extensions.common.pipelines.*
import com.trendyol.quafka.extensions.consumer.batch.pipelines.*
import com.trendyol.quafka.extensions.consumer.single.pipelines.*
import io.kotest.core.spec.style.FunSpec
import io.kotest.matchers.collections.shouldContainAll
import io.kotest.matchers.ints.shouldBeGreaterThan
import io.kotest.matchers.nulls.shouldNotBeNull
import io.kotest.matchers.shouldBe
import io.mockk.*
import kotlinx.coroutines.delay
import org.apache.kafka.clients.consumer.ConsumerRecord
import java.util.concurrent.ConcurrentHashMap
import kotlin.time.measureTime

class SingleMessagePipelineAdapterTest :
    FunSpec({

        test("should process each message through single message pipeline sequentially when concurrencyLevel is 1") {
            // Arrange
            val messages = listOf(
                createIncomingMessage("key1", "value1", 0),
                createIncomingMessage("key2", "value2", 1),
                createIncomingMessage("key3", "value3", 2)
            )
            val consumerContext = mockk<ConsumerContext>(relaxed = true)
            val processedValues = mutableListOf<String>()

            // Create single message pipeline
            val singlePipeline = PipelineBuilder<SingleMessageEnvelope<String, String>>()
                .use { envelope, next ->
                    processedValues.add(envelope.message.value)
                    next()
                }
                .build()

            // Create batch pipeline with sequential adapter (concurrencyLevel = 1)
            val adapter = SingleMessagePipelineAdapter<String, String>(
                singlePipeline,
                concurrencyLevel = 1
            )
            val batchEnvelope = BatchMessageEnvelope(messages, consumerContext)

            // Act
            adapter.execute(batchEnvelope) { }

            // Assert
            processedValues.size shouldBe 3
            processedValues shouldBe listOf("value1", "value2", "value3")
        }

        test("should process each message through single message pipeline concurrently when concurrencyLevel is greater than 1") {
            // Arrange
            val messages = listOf(
                createIncomingMessage("key1", "value1", 0),
                createIncomingMessage("key2", "value2", 1),
                createIncomingMessage("key3", "value3", 2)
            )
            val consumerContext = mockk<ConsumerContext>(relaxed = true)
            val processedValues = ConcurrentHashMap.newKeySet<String>()

            // Create single message pipeline
            val singlePipeline = PipelineBuilder<SingleMessageEnvelope<String, String>>()
                .use { envelope, next ->
                    processedValues.add(envelope.message.value)
                    next()
                }
                .build()

            // Create batch pipeline with concurrent adapter (concurrencyLevel > 1)
            val adapter = SingleMessagePipelineAdapter<String, String>(
                singlePipeline,
                concurrencyLevel = 3
            )
            val batchEnvelope = BatchMessageEnvelope(messages, consumerContext)

            // Act
            adapter.execute(batchEnvelope) { }

            // Assert
            processedValues.size shouldBe 3
            processedValues shouldContainAll listOf("value1", "value2", "value3")
        }

        test("concurrent processing should be faster than sequential for slow operations") {
            // Arrange
            val messages = (1..5).map { createIncomingMessage("key$it", "value$it", it.toLong()) }
            val consumerContext = mockk<ConsumerContext>(relaxed = true)
            val delayMs = 100L

            // Create single message pipeline with delay
            val singlePipeline = PipelineBuilder<SingleMessageEnvelope<String, String>>()
                .use { envelope, next ->
                    delay(delayMs)
                    next()
                }
                .build()

            val sequentialAdapter = SingleMessagePipelineAdapter<String, String>(
                singlePipeline,
                concurrencyLevel = 1 // Sequential
            )

            val concurrentAdapter = SingleMessagePipelineAdapter<String, String>(
                singlePipeline,
                concurrencyLevel = 5 // Concurrent
            )

            // Act
            val sequentialTime = measureTime {
                sequentialAdapter.execute(BatchMessageEnvelope(messages, consumerContext)) { }
            }

            val concurrentTime = measureTime {
                concurrentAdapter.execute(BatchMessageEnvelope(messages, consumerContext)) { }
            }

            // Assert
            println("Sequential time: $sequentialTime, Concurrent time: $concurrentTime")
            (concurrentTime < sequentialTime) shouldBe true
        }

        test("should share attributes from batch to single messages when enabled") {
            // Arrange
            val messages = listOf(createIncomingMessage("key1", "value1", 0))
            val consumerContext = mockk<ConsumerContext>(relaxed = true)
            val batchKey = AttributeKey<String>("batchId")
            val batchEnvelope = BatchMessageEnvelope(messages, consumerContext)
            batchEnvelope.attributes.put(batchKey, "batch-123")

            var retrievedValue: String? = null

            // Create single message pipeline that reads batch attribute
            val singlePipeline = PipelineBuilder<SingleMessageEnvelope<String, String>>()
                .use { envelope, next ->
                    retrievedValue = envelope.attributes.getOrNull(batchKey)
                    next()
                }
                .build()

            // Create adapter with shareAttributes enabled
            val adapter = SingleMessagePipelineAdapter<String, String>(
                singlePipeline,
                shareAttributes = true
            )

            // Act
            adapter.execute(batchEnvelope) { }

            // Assert
            retrievedValue shouldBe "batch-123"
        }

        test("should not share attributes when disabled") {
            // Arrange
            val messages = listOf(createIncomingMessage("key1", "value1", 0))
            val consumerContext = mockk<ConsumerContext>(relaxed = true)
            val batchKey = AttributeKey<String>("batchId")
            val batchEnvelope = BatchMessageEnvelope(messages, consumerContext)
            batchEnvelope.attributes.put(batchKey, "batch-123")

            var retrievedValue: String? = "not-null"

            // Create single message pipeline
            val singlePipeline = PipelineBuilder<SingleMessageEnvelope<String, String>>()
                .use { envelope, next ->
                    retrievedValue = envelope.attributes.getOrNull(batchKey)
                    next()
                }
                .build()

            // Create adapter with shareAttributes disabled (default)
            val adapter = SingleMessagePipelineAdapter<String, String>(singlePipeline)

            // Act
            adapter.execute(batchEnvelope) { }

            // Assert
            retrievedValue shouldBe null
        }

        test("should collect attributes from single messages when enabled in sequential mode") {
            // Arrange
            val messages = listOf(
                createIncomingMessage("key1", "value1", 0),
                createIncomingMessage("key2", "value2", 1)
            )
            val consumerContext = mockk<ConsumerContext>(relaxed = true)
            val counterKey = AttributeKey<Int>("counter")
            val batchEnvelope = BatchMessageEnvelope(messages, consumerContext)

            // Create single message pipeline that sets an attribute
            val singlePipeline = PipelineBuilder<SingleMessageEnvelope<String, String>>()
                .use { envelope, next ->
                    val current = envelope.attributes.getOrNull(counterKey) ?: 0
                    envelope.attributes.put(counterKey, current + 1)
                    next()
                }
                .build()

            // Create adapter with collectAttributes enabled
            val adapter = SingleMessagePipelineAdapter<String, String>(
                singlePipeline,
                concurrencyLevel = 1, // Sequential
                collectAttributes = true
            )

            // Act
            adapter.execute(batchEnvelope) { }

            // Assert - last value should be collected
            val finalCount = batchEnvelope.attributes.getOrNull(counterKey)
            finalCount.shouldNotBeNull()
            finalCount shouldBeGreaterThan 0
        }

        test("should collect attributes from single messages when enabled in concurrent mode") {
            // Arrange
            val messages = (1..10).map { createIncomingMessage("key$it", "value$it", it.toLong()) }
            val consumerContext = mockk<ConsumerContext>(relaxed = true)
            val counterKey = AttributeKey<Int>("counter")
            val batchEnvelope = BatchMessageEnvelope(messages, consumerContext)

            // Create single message pipeline that increments a counter
            val singlePipeline = PipelineBuilder<SingleMessageEnvelope<String, String>>()
                .use { envelope, next ->
                    envelope.attributes.put(counterKey, 1)
                    next()
                }
                .build()

            // Create adapter with collectAttributes enabled
            val adapter = SingleMessagePipelineAdapter<String, String>(
                singlePipeline,
                concurrencyLevel = 5, // Concurrent
                collectAttributes = true
            )

            // Act
            adapter.execute(batchEnvelope) { }

            // Assert - attribute should be collected (synchronized)
            val finalCount = batchEnvelope.attributes.getOrNull(counterKey)
            finalCount.shouldNotBeNull()
        }

        test("should work with extension function in sequential mode") {
            // Arrange
            val messages = listOf(
                createIncomingMessage("key1", "value1", 0),
                createIncomingMessage("key2", "value2", 1)
            )
            val consumerContext = mockk<ConsumerContext>(relaxed = true)
            val processedValues = mutableListOf<String>()

            // Create batch pipeline using extension function
            val batchPipeline = PipelineBuilder<BatchMessageEnvelope<String, String>>()
                .useSingleMessagePipeline(concurrencyLevel = 1) {
                    // Explicit sequential
                    use { envelope, next ->
                        processedValues.add(envelope.message.value)
                        next()
                    }
                }
                .build()

            val batchEnvelope = BatchMessageEnvelope(messages, consumerContext)

            // Act
            batchPipeline.execute(batchEnvelope)

            // Assert
            processedValues.size shouldBe 2
            processedValues shouldBe listOf("value1", "value2")
        }

        test("should work with extension function in concurrent mode") {
            // Arrange
            val messages = listOf(
                createIncomingMessage("key1", "value1", 0),
                createIncomingMessage("key2", "value2", 1)
            )
            val consumerContext = mockk<ConsumerContext>(relaxed = true)
            val processedValues = ConcurrentHashMap.newKeySet<String>()

            // Create batch pipeline using extension function
            val batchPipeline = PipelineBuilder<BatchMessageEnvelope<String, String>>()
                .useSingleMessagePipeline(concurrencyLevel = 2) {
                    // Concurrent
                    use { envelope, next ->
                        processedValues.add(envelope.message.value)
                        next()
                    }
                }
                .build()

            val batchEnvelope = BatchMessageEnvelope(messages, consumerContext)

            // Act
            batchPipeline.execute(batchEnvelope)

            // Assert
            processedValues.size shouldBe 2
            processedValues shouldContainAll listOf("value1", "value2")
        }

        test("should use sequential mode when concurrencyLevel is 0 or negative") {
            // Arrange
            val messages = listOf(
                createIncomingMessage("key1", "value1", 0),
                createIncomingMessage("key2", "value2", 1)
            )
            val consumerContext = mockk<ConsumerContext>(relaxed = true)
            val processedValues = mutableListOf<String>()

            val singlePipeline = PipelineBuilder<SingleMessageEnvelope<String, String>>()
                .use { envelope, next ->
                    processedValues.add(envelope.message.value)
                    next()
                }
                .build()

            // Test with concurrencyLevel = 0
            val adapter0 = SingleMessagePipelineAdapter<String, String>(
                singlePipeline,
                concurrencyLevel = 0
            )

            // Test with concurrencyLevel = -1
            val adapterNegative = SingleMessagePipelineAdapter<String, String>(
                singlePipeline,
                concurrencyLevel = -1
            )

            val batchEnvelope = BatchMessageEnvelope(messages, consumerContext)

            // Act & Assert - Should work without errors (sequential mode)
            processedValues.clear()
            adapter0.execute(batchEnvelope) { }
            processedValues.size shouldBe 2
            processedValues shouldBe listOf("value1", "value2")

            processedValues.clear()
            adapterNegative.execute(batchEnvelope) { }
            processedValues.size shouldBe 2
            processedValues shouldBe listOf("value1", "value2")
        }
    }) {

    companion object {
        private fun createIncomingMessage(key: String, value: String, offset: Long): IncomingMessage<String, String> {
            val consumerRecord = ConsumerRecord(
                "test-topic",
                0,
                offset,
                key,
                value
            )

            return IncomingMessage.create(
                consumerRecord,
                IncomingMessageStringFormatter(),
                "groupId",
                "clientId"
            )
        }
    }
}
