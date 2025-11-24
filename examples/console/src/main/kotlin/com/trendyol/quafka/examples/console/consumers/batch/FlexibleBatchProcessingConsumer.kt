package com.trendyol.quafka.examples.console.consumers.batch

import com.trendyol.quafka.consumer.ackAll
import com.trendyol.quafka.consumer.configuration.QuafkaConsumerBuilder
import com.trendyol.quafka.consumer.configuration.withDeserializer
import com.trendyol.quafka.examples.console.consumers.ConsumerBase
import com.trendyol.quafka.extensions.common.pipelines.use
import com.trendyol.quafka.extensions.consumer.batch.pipelines.PipelineBatchMessageHandler.Companion.usePipelineBatchMessageHandler
import com.trendyol.quafka.extensions.consumer.batch.pipelines.middlewares.useSingleMessagePipeline
import org.apache.kafka.common.serialization.StringDeserializer

/**
 * Configurable batch consumer switching between sequential and parallel modes.
 *
 * **What it shows:**
 * - Toggle between sequential (concurrency=1) and parallel processing
 * - Dynamic concurrency configuration
 * - Same code, different execution modes
 * - Performance comparison
 *
 * **Configuration:**
 * - `useParallelProcessing = true` → 5 concurrent workers
 * - `useParallelProcessing = false` → Sequential processing
 *
 * **Use when:** Need flexibility to switch processing modes based on conditions.
 */
class FlexibleBatchProcessingConsumer(properties: MutableMap<String, Any>) : ConsumerBase(properties) {
    private val useParallelProcessing = true // Toggle this

    override suspend fun run() {
        val consumer =
            QuafkaConsumerBuilder<String, String>(properties)
                .withDeserializer(StringDeserializer())
                .subscribe("topic-example") {
                    usePipelineBatchMessageHandler {
                        val concurrency = if (useParallelProcessing) 5 else 1

                        useSingleMessagePipeline(concurrencyLevel = concurrency) {
                            use { envelope, next ->
                                val mode = if (concurrency > 1) "Concurrent" else "Sequential"
                                logger.info("[$mode] Processing: ${envelope.message.value}")
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
