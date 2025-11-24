package com.trendyol.quafka.examples.console.consumers.batch

import com.trendyol.quafka.consumer.configuration.*
import com.trendyol.quafka.examples.console.consumers.ConsumerBase
import com.trendyol.quafka.extensions.common.pipelines.*
import com.trendyol.quafka.extensions.consumer.batch.pipelines.PipelineBatchMessageHandler.Companion.usePipelineBatchMessageHandler
import com.trendyol.quafka.extensions.consumer.batch.pipelines.middlewares.*
import kotlinx.coroutines.delay
import org.apache.kafka.common.serialization.StringDeserializer

/**
 * Batch consumer with parallel message processing for high throughput.
 *
 * **What it shows:**
 * - Concurrent processing with `concurrencyLevel`
 * - Thread-safe parallel execution
 * - Performance gain for I/O-bound operations
 * - Per-message acknowledgment in parallel
 * - Batch-level coordination
 *
 * **Configuration:**
 * - Concurrency level: 10 (process 10 messages simultaneously)
 * - Simulates heavy processing with delay
 * - Logs thread names for visibility
 *
 * **Use when:** Processing is I/O-bound (API calls, DB operations, etc.).
 */
class ParallelBatchProcessingConsumer(properties: MutableMap<String, Any>) : ConsumerBase(properties) {
    override suspend fun run() {
        val consumer =
            QuafkaConsumerBuilder<String, String>(properties)
                .withDeserializer(StringDeserializer())
                .subscribe("topic-example") {
                    usePipelineBatchMessageHandler {
                        // Middleware 1: Batch-level logging
                        use { envelope, next ->
                            logger.info("=== Starting batch of ${envelope.messages.size} messages ===")
                            next()
                            logger.info("=== Completed batch of ${envelope.messages.size} messages ===")
                        }

                        // Middleware 2: Process messages concurrently
                        useSingleMessagePipeline(
                            concurrencyLevel = 10 // Process up to 10 messages in parallel
                        ) {
                            // Single message middleware 1: Validation
                            use { envelope, next ->
                                val message = envelope.message
                                if (message.value.isNotBlank()) {
                                    logger.info("[Thread: ${Thread.currentThread().name}] Processing: ${message.value}")
                                    next()
                                } else {
                                    logger.warn("Invalid message: empty value")
                                }
                            }

                            // Single message middleware 2: Simulate heavy processing
                            use { envelope, next ->
                                // Simulate API call or database operation
                                delay(100) // Would be an actual async operation in real code
                                logger.info("Processed: ${envelope.message.value}")
                                next()
                            }

                            // Single message middleware 3: Per-message acknowledgment
                            use { envelope, next ->
                                envelope.message.ack()
                                next()
                            }
                        }

                        // Middleware 3: Batch-level finalization
                        use { envelope, next ->
                            logger.info("All ${envelope.messages.size} messages processed in parallel")
                            next()
                        }
                    }
                }.build()

        consumer.start()
    }
}
