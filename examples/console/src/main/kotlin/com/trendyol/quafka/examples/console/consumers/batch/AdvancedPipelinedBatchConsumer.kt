package com.trendyol.quafka.examples.console.consumers.batch

import com.trendyol.quafka.common.HeaderParsers.asString
import com.trendyol.quafka.common.get
import com.trendyol.quafka.consumer.ackAll
import com.trendyol.quafka.consumer.configuration.*
import com.trendyol.quafka.examples.console.consumers.ConsumerBase
import com.trendyol.quafka.examples.console.consumers.batch.middlewares.FilterBatchMiddleware
import com.trendyol.quafka.examples.console.consumers.batch.middlewares.LoggingBatchMiddleware
import com.trendyol.quafka.examples.console.consumers.batch.middlewares.MeasureBatchMiddleware
import com.trendyol.quafka.extensions.common.pipelines.*
import com.trendyol.quafka.extensions.consumer.batch.pipelines.*
import com.trendyol.quafka.extensions.consumer.batch.pipelines.PipelineBatchMessageHandler.Companion.usePipelineBatchMessageHandler
import org.apache.kafka.common.serialization.StringDeserializer

/**
 * Advanced batch consumer with custom middlewares and attribute sharing.
 *
 * **What it shows:**
 * - Reusable custom middlewares (measure, logging, filter)
 * - Message filtering based on headers
 * - Attribute storage and retrieval across middlewares
 * - Complex middleware composition
 * - Performance measurement
 *
 * **Middleware chain:**
 * 1. Measure - Time batch processing
 * 2. Logging - Log batch info
 * 3. Filter - Keep only messages with 'type' header
 * 4. Metadata - Store batch ID in attributes
 * 5. Business logic - Access shared attributes
 * 6. Acknowledgment - Bulk ack
 *
 * **Use when:** Need complex workflows with filtering, metrics, and shared state.
 */
class AdvancedPipelinedBatchConsumer(properties: MutableMap<String, Any>) : ConsumerBase(properties) {
    override suspend fun run() {
        val consumer =
            QuafkaConsumerBuilder<String, String>(properties)
                .withDeserializer(StringDeserializer())
                .subscribe("topic-example") {
                    usePipelineBatchMessageHandler {
                        // Middleware 1: Measure execution time
                        useMiddleware(MeasureBatchMiddleware())

                        // Middleware 2: Logging
                        useMiddleware(LoggingBatchMiddleware(logger))

                        // Middleware 3: Filter messages (example: only messages with specific header)
                        useMiddleware(
                            FilterBatchMiddleware { message ->
                                message.headers.get("type")?.asString() != null
                            }
                        )

                        // Middleware 4: Custom processing with attributes
                        use { envelope: BatchMessageEnvelope<String, String>, next ->
                            // Store some metadata in attributes
                            envelope.attributes.put(
                                AttributeKey("batchId"),
                                envelope.messages.firstOrNull()?.offset?.toString() ?: "unknown"
                            )
                            next()
                        }

                        // Middleware 5: Business logic
                        use { envelope: BatchMessageEnvelope<String, String>, next ->
                            val batchId = envelope.attributes.getOrNull(
                                AttributeKey("batchId")
                            )
                            logger.info("Processing batch with ID: $batchId")

                            for (incomingMessage in envelope.messages) {
                                // Your business logic here
                                logger.info("Processing message: ${incomingMessage.toString(addValue = true, addHeaders = false)}")
                            }
                            next()
                        }

                        // Middleware 6: Acknowledgment
                        use { envelope: BatchMessageEnvelope<String, String>, next ->
                            envelope.messages.ackAll()
                            logger.info("Acknowledged ${envelope.messages.size} messages")
                            next()
                        }
                    }
                }.build()

        consumer.start()
    }
}
