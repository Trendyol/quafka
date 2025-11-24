package com.trendyol.quafka.examples.console.consumers.batch

import com.trendyol.quafka.consumer.configuration.*
import com.trendyol.quafka.examples.console.consumers.ConsumerBase
import com.trendyol.quafka.extensions.common.pipelines.*
import com.trendyol.quafka.extensions.consumer.batch.pipelines.PipelineBatchMessageHandler.Companion.usePipelineBatchMessageHandler
import com.trendyol.quafka.extensions.consumer.batch.pipelines.middlewares.*
import org.apache.kafka.common.serialization.StringDeserializer

/**
 * Hybrid batch consumer processing each message individually using single message pipeline.
 *
 * **What it shows:**
 * - Batch-level and message-level pipelines combined
 * - `useSingleMessagePipeline()` adapter
 * - Individual message validation and processing
 * - Per-message acknowledgment within batch
 * - Attribute sharing between batch and messages
 *
 * **Processing flow:**
 * - Batch received → Batch-level logging
 * - Each message → Individual validation and processing
 * - Each message → Per-message ack
 * - Batch done → Batch-level finalization
 *
 * **Use when:** Need both batch and individual message operations.
 */
class BatchWithSingleMessagePipelineConsumer(properties: MutableMap<String, Any>) : ConsumerBase(properties) {
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

                        // Middleware 2: Process each message individually using single message pipeline
                        useSingleMessagePipeline(
                            shareAttributes = true, // Share batch attributes with single messages
                            collectAttributes = false // Don't collect single message attributes back
                        ) {
                            // This runs for EACH message individually

                            // Single message middleware 1: Validation
                            use { envelope, next ->
                                val message = envelope.message
                                if (message.value.isNotBlank()) {
                                    logger.info("Valid message: ${message.value}")
                                    next()
                                } else {
                                    logger.warn("Invalid message: empty value")
                                    // Skip next() to stop processing this message
                                }
                            }

                            // Single message middleware 2: Business logic
                            use { envelope, next ->
                                val message = envelope.message
                                // Your business logic per message
                                logger.info("Processing individual message: offset=${message.offset}, value=${message.value}")

                                // Simulate some processing
                                // processOrder(message.value)

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
                            logger.info("All ${envelope.messages.size} messages processed successfully")
                            next()
                        }
                    }
                }.build()

        consumer.start()
    }
}
