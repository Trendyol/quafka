package com.trendyol.quafka.examples.console.consumers.batch

import com.trendyol.quafka.consumer.ackAll
import com.trendyol.quafka.consumer.configuration.*
import com.trendyol.quafka.examples.console.consumers.ConsumerBase
import com.trendyol.quafka.extensions.common.pipelines.*
import com.trendyol.quafka.extensions.consumer.batch.pipelines.*
import com.trendyol.quafka.extensions.consumer.batch.pipelines.PipelineBatchMessageHandler.Companion.usePipelineBatchMessageHandler
import org.apache.kafka.common.serialization.StringDeserializer

/**
 * Batch consumer using pipeline/middleware architecture.
 *
 * **What it shows:**
 * - Middleware pattern for batch processing
 * - Composable processing steps (logging, business logic, ack)
 * - Clear separation of concerns
 * - Sequential middleware execution
 *
 * **Middleware order:**
 * 1. Logging - Log batch size
 * 2. Business logic - Process each message
 * 3. Acknowledgment - Bulk ack all messages
 *
 * **Use when:** Need modular, reusable batch processing logic.
 */
class PipelinedBatchConsumer(properties: MutableMap<String, Any>) : ConsumerBase(properties) {
    override suspend fun run() {
        val consumer =
            QuafkaConsumerBuilder<String, String>(properties)
                .withDeserializer(StringDeserializer())
                .subscribe("topic-example") {
                    usePipelineBatchMessageHandler {
                        // First middleware: Logging
                        use { envelope: BatchMessageEnvelope<String, String>, next ->
                            logger.info("Processing batch of ${envelope.messages.size} messages")
                            next()
                        }

                        // Second middleware: Business logic
                        use { envelope: BatchMessageEnvelope<String, String>, next ->
                            for (incomingMessage in envelope.messages) {
                                logger.info("Processing message: ${incomingMessage.toString(addValue = true, addHeaders = true)}")
                                // Your business logic here
                            }
                            next()
                        }

                        // Third middleware: Acknowledgment
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
