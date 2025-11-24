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
 * Parallel batch consumer with shared state across concurrent workers.
 *
 * **What it shows:**
 * - Concurrent processing (10 workers) with shared attributes
 * - Batch-level context (batch ID, timing) accessible by all workers
 * - Thread-safe attribute sharing
 * - Batch-level metrics (duration measurement)
 *
 * **Pattern:**
 * 1. Set batch metadata (ID, start time)
 * 2. Process messages concurrently with shared batch context
 * 3. Calculate batch-level statistics
 * 4. Bulk acknowledgment
 *
 * **Use when:** Parallel processing needs shared batch context or correlation.
 */
class ConcurrentWithAttributesConsumer(properties: MutableMap<String, Any>) : ConsumerBase(properties) {
    override suspend fun run() {
        val consumer =
            QuafkaConsumerBuilder<String, String>(properties)
                .withDeserializer(StringDeserializer())
                .subscribe("topic-example") {
                    usePipelineBatchMessageHandler {
                        // Set batch-level context
                        use { envelope, next ->
                            val batchIdKey = AttributeKey<String>("batchId")
                            val startTimeKey =
                                com.trendyol.quafka.extensions.common.pipelines.AttributeKey<Long>("startTime")

                            envelope.attributes.put(batchIdKey, "batch-${System.currentTimeMillis()}")
                            envelope.attributes.put(startTimeKey, System.currentTimeMillis())

                            logger.info("Starting batch: ${envelope.attributes.get(batchIdKey)}")
                            next()

                            val duration = System.currentTimeMillis() - envelope.attributes.get(startTimeKey)
                            logger.info("Batch completed in ${duration}ms")
                        }

                        // Process concurrently with shared batch context
                        useSingleMessagePipeline(
                            concurrencyLevel = 10,
                            shareAttributes = true // Share batch context with each message
                        ) {
                            use { envelope, next ->
                                val batchId = envelope.attributes.getOrNull(
                                    com.trendyol.quafka.extensions.common.pipelines.AttributeKey<String>(
                                        "batchId"
                                    )
                                )
                                logger.info("[Batch: $batchId] Processing message: ${envelope.message.offset}")

                                // Process with batch context
                                // saveToDatabase(envelope.message, batchId)

                                next()
                            }
                        }

                        // Final acknowledgment
                        use { envelope, next ->
                            envelope.messages.ackAll()
                            next()
                        }
                    }
                }.build()

        consumer.start()
    }
}
