package com.trendyol.quafka.examples.console.producers

import com.trendyol.quafka.common.header
import com.trendyol.quafka.producer.OutgoingMessage
import com.trendyol.quafka.producer.configuration.QuafkaProducerBuilder
import org.apache.kafka.common.serialization.StringSerializer
import java.util.UUID

/**
 * Producer demonstrating message headers for metadata and tracing.
 *
 * **What it shows:**
 * - Adding custom headers to messages
 * - Distributed tracing headers (trace-id, span-id)
 * - Versioning and schema headers
 * - Correlation IDs for request tracking
 *
 * **Common header patterns:**
 * - **Tracing:** trace-id, span-id, service-name
 * - **Versioning:** message-version, schema-version
 * - **Content:** content-type, encoding
 * - **Correlation:** correlation-id, request-id
 *
 * **Use when:** Production systems requiring tracing, versioning, or metadata.
 */
class HeaderProducer(properties: MutableMap<String, Any>) : ProducerBase(properties) {
    override suspend fun run() {
        val producer = QuafkaProducerBuilder<String, String>(properties)
            .withSerializer(StringSerializer(), StringSerializer())
            .build()

        try {
            logger.info("=== Header Producer Example ===")

            // Send with distributed tracing headers
            repeat(5) { index ->
                val traceId = UUID.randomUUID().toString()
                val spanId = UUID.randomUUID().toString()

                val headers = listOf(
                    header("trace-id", traceId),
                    header("span-id", spanId),
                    header("service-name", "console-app"),
                    header("message-type", "order-event"),
                    header("timestamp", System.currentTimeMillis())
                )

                val message = OutgoingMessage.create(
                    topic = "demo-topic",
                    key = "traced-$index",
                    value = "Message with tracing headers: $index",
                    headers = headers
                )

                val result = producer.send(message)
                logger.info(
                    "Message with headers sent: key={}, traceId={}, topic={}",
                    "traced-$index",
                    traceId.substring(0, 8),
                    result.recordMetadata.topic()
                )
            }

            // Send with version headers
            repeat(3) { index ->
                val headers = listOf(
                    header("message-version", "2.0"),
                    header("schema-version", "1.5"),
                    header("content-type", "application/json"),
                    header("correlation-id", UUID.randomUUID().toString())
                )

                val message = OutgoingMessage.create(
                    topic = "demo-topic",
                    key = "versioned-$index",
                    value = """{"event": "order.created", "id": $index}""",
                    headers = headers
                )

                val result = producer.send(message)
                logger.info(
                    "Versioned message sent: key={}, version=2.0, topic={}",
                    "versioned-$index",
                    result.recordMetadata.topic()
                )
            }

            logger.info("=== Header Producer completed successfully ===")
        } finally {
            producer.close()
        }
    }
}
