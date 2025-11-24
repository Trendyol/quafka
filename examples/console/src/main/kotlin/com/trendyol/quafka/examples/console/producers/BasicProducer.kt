package com.trendyol.quafka.examples.console.producers

import com.trendyol.quafka.producer.OutgoingMessage
import com.trendyol.quafka.producer.configuration.*
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.StringSerializer

/**
 * Basic producer example demonstrating simple message publishing.
 *
 * **What it shows:**
 * - Creating a producer with String serializer
 * - Sending individual messages one at a time
 * - Basic producer configuration (retries, max in-flight requests)
 * - Getting send results with partition and offset metadata
 *
 * **Use when:** Learning Kafka basics or sending low-volume messages.
 */
class BasicProducer(properties: MutableMap<String, Any>) : ProducerBase(properties) {
    override fun createProperties(baseProperties: MutableMap<String, Any>): MutableMap<String, Any> {
        super.createProperties(baseProperties)
        baseProperties.putIfAbsent(ProducerConfig.RETRIES_CONFIG, 3)
        baseProperties.putIfAbsent(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 5)
        return baseProperties
    }

    override suspend fun run() {
        val producer = QuafkaProducerBuilder<String, String>(properties)
            .withSerializer(StringSerializer())
            .build()

        try {
            logger.info("=== Basic Producer Example ===")

            // Send single messages
            repeat(10) { index ->
                val message = OutgoingMessage.create(
                    topic = "demo-topic",
                    key = "key-$index",
                    value = "Message $index: Hello from Basic Producer!"
                )

                val result = producer.send(message)
                logger.info(
                    "Message sent: key={}, topic={}",
                    "key-$index",
                    result.recordMetadata.topic()
                )
            }

            logger.info("=== Basic Producer completed successfully ===")
        } finally {
            producer.close()
        }
    }
}
