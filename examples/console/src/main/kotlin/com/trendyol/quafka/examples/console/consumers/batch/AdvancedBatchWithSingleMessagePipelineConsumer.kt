package com.trendyol.quafka.examples.console.consumers.batch

import com.trendyol.quafka.consumer.ackAll
import com.trendyol.quafka.consumer.configuration.QuafkaConsumerBuilder
import com.trendyol.quafka.consumer.configuration.withDeserializer
import com.trendyol.quafka.examples.console.consumers.ConsumerBase
import com.trendyol.quafka.extensions.common.pipelines.AttributeKey
import com.trendyol.quafka.extensions.common.pipelines.use
import com.trendyol.quafka.extensions.consumer.batch.pipelines.PipelineBatchMessageHandler.Companion.usePipelineBatchMessageHandler
import com.trendyol.quafka.extensions.consumer.batch.pipelines.middlewares.useSingleMessagePipeline
import org.apache.kafka.common.serialization.StringDeserializer

/**
 * Advanced hybrid consumer sharing context between batch and message pipelines.
 *
 * **What it shows:**
 * - Attribute sharing from batch to messages
 * - Batch metadata (ID, timestamp) accessible in message handlers
 * - Context propagation across pipeline boundaries
 * - Batch-level acknowledgment after individual processing
 *
 * **Pattern:**
 * 1. Set batch-level metadata (batch ID)
 * 2. Share attributes with single message pipeline
 * 3. Each message accesses batch context
 * 4. Final bulk acknowledgment
 *
 * **Use when:** Messages need batch context for processing or correlation.
 */
class AdvancedBatchWithSingleMessagePipelineConsumer(properties: MutableMap<String, Any>) : ConsumerBase(properties) {
    override suspend fun run() {
        val consumer =
            QuafkaConsumerBuilder<String, String>(properties)
                .withDeserializer(StringDeserializer())
                .subscribe("topic-example") {
                    usePipelineBatchMessageHandler {
                        // Set batch-level metadata
                        use { envelope, next ->
                            val batchIdKey = AttributeKey<String>("batchId")
                            val batchId = "batch-${System.currentTimeMillis()}"
                            envelope.attributes.put(batchIdKey, batchId)
                            logger.info("Processing batch: $batchId")
                            next()
                        }

                        // Process each message with access to batch metadata
                        useSingleMessagePipeline(shareAttributes = true) {
                            use { envelope, next ->
                                val batchIdKey =
                                    com.trendyol.quafka.extensions.common.pipelines.AttributeKey<String>("batchId")
                                val batchId = envelope.attributes.getOrNull(batchIdKey)
                                logger.info("Message offset=${envelope.message.offset} belongs to batch: $batchId")

                                // Process message with batch context
                                // saveToDatabase(message, batchId)

                                next()
                            }
                        }

                        // Final acknowledgment at batch level
                        use { envelope, next ->
                            envelope.messages.ackAll()
                            next()
                        }
                    }
                }.build()

        consumer.start()
    }
}
