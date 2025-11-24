package com.trendyol.quafka.examples.console.consumers.batch

import com.trendyol.quafka.consumer.ackAll
import com.trendyol.quafka.consumer.configuration.QuafkaConsumerBuilder
import com.trendyol.quafka.examples.console.consumers.ConsumerBase
import com.trendyol.quafka.extensions.common.pipelines.AttributeKey
import com.trendyol.quafka.extensions.common.pipelines.use
import com.trendyol.quafka.extensions.consumer.batch.pipelines.PipelineBatchMessageHandler.Companion.usePipelineBatchMessageHandler
import com.trendyol.quafka.extensions.consumer.batch.pipelines.middlewares.useSingleMessagePipeline
import kotlinx.coroutines.delay
import org.apache.kafka.common.serialization.StringDeserializer
import java.util.concurrent.ConcurrentLinkedQueue

/**
 * Result object for tracking individual message processing outcomes.
 */
data class ProcessingResult(val offset: Long, val success: Boolean, val message: String, val processingTimeMs: Long)

/**
 * Advanced batch consumer collecting individual processing results for analytics.
 *
 * **What it shows:**
 * - Collecting results from concurrent message processing
 * - Thread-safe result aggregation with ConcurrentLinkedQueue
 * - Batch-level metrics (success/failure counts, avg processing time)
 * - Error handling without stopping batch processing
 * - Detailed logging of individual results
 *
 * **Metrics collected:**
 * - Total processing time
 * - Success vs failure counts
 * - Average processing time per message
 * - Individual message outcomes
 *
 * **Use when:** Need detailed analytics, monitoring, or reporting on batch processing.
 */
class MessageResultCollectorConsumer(properties: MutableMap<String, Any>) : ConsumerBase(properties) {
    override suspend fun run() {
        val resultsAttributeKey = AttributeKey<ConcurrentLinkedQueue<ProcessingResult>>("results")
        val deserializer = StringDeserializer()
        val consumer = QuafkaConsumerBuilder<String, String>(properties)
            .withDeserializer(deserializer, deserializer)
            .subscribe("demo-topic") {
                usePipelineBatchMessageHandler {
                    // Middleware 1: Initialize results collector
                    use { envelope, next ->

                        envelope.attributes.put(resultsAttributeKey, ConcurrentLinkedQueue<ProcessingResult>())

                        logger.info("=== Starting batch processing with result collection ===")
                        logger.info("Batch size: ${envelope.messages.size}")

                        val startTime = System.currentTimeMillis()
                        next()

                        // After processing, access collected results
                        val results = envelope.attributes[resultsAttributeKey]
                        val duration = System.currentTimeMillis() - startTime

                        logger.info("=== Batch completed in ${duration}ms ===")
                        logger.info("Total messages processed: ${results.size}")

                        val successCount = results.count { it.success }
                        val failureCount = results.size - successCount
                        val avgProcessingTime = if (results.isNotEmpty()) {
                            results.sumOf { it.processingTimeMs } / results.size
                        } else {
                            0
                        }

                        logger.info("Success: $successCount, Failures: $failureCount")
                        logger.info("Average processing time: ${avgProcessingTime}ms per message")

                        // Print detailed results
                        results.forEachIndexed { index, result ->
                            val status = if (result.success) "✓" else "✗"
                            logger.info("  [$status] Offset ${result.offset}: ${result.message} (${result.processingTimeMs}ms)")
                        }
                    }

                    // Middleware 2: Process each message and collect results
                    useSingleMessagePipeline(shareAttributes = true, collectAttributes = true, concurrencyLevel = 10) {
                        use { envelope, next ->
                            val startTime = System.currentTimeMillis()

                            try {
                                // Simulate message processing
                                val messageValue = envelope.message.value
                                logger.info("Processing message at offset ${envelope.message.offset}: $messageValue")

                                // Your actual business logic here
                                // Simulate some processing time
                                delay((10..50).random().toLong())

                                val processingTime = System.currentTimeMillis() - startTime

                                // Create result
                                val result = ProcessingResult(
                                    offset = envelope.message.offset,
                                    success = true,
                                    message = "Processed: $messageValue",
                                    processingTimeMs = processingTime
                                )

                                // Collect result
                                val results = envelope.attributes[resultsAttributeKey]
                                results.add(result)

                                next()
                            } catch (e: Exception) {
                                val processingTime = System.currentTimeMillis() - startTime

                                // Even on error, collect the result
                                val result = ProcessingResult(
                                    offset = envelope.message.offset,
                                    success = false,
                                    message = "Error: ${e.message}",
                                    processingTimeMs = processingTime
                                )

                                val results = envelope.attributes[resultsAttributeKey]
                                results.add(result)

                                logger.error("Error processing message at offset ${envelope.message.offset}", e)
                                // Don't throw, continue processing other messages
                            }
                        }
                    }

                    // Middleware 3: Acknowledgment
                    use { envelope, next ->
                        envelope.messages.ackAll()
                        logger.info("Acknowledged ${envelope.messages.size} messages")
                        next()
                    }
                }
            }.build()

        consumer.start()
    }
}
