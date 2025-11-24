package com.trendyol.quafka.examples.console.producers

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import com.trendyol.quafka.extensions.producer.OutgoingMessageBuilder
import com.trendyol.quafka.extensions.serialization.json.JsonSerializer
import com.trendyol.quafka.extensions.serialization.json.newMessageWithTypeInfo
import com.trendyol.quafka.producer.configuration.QuafkaProducerBuilder
import org.apache.kafka.common.serialization.ByteArraySerializer

private data class OrderCreated(
    val orderId: String,
    val userId: String,
    val amount: Double,
    val productName: String,
    val timestamp: Long = System.currentTimeMillis()
)

private data class UserRegistered(val userId: String, val email: String, val name: String, val timestamp: Long = System.currentTimeMillis())

/**
 * JSON Producer example using the new JsonSerializer API.
 *
 * This example shows how to:
 * - Create a JSON serializer (no typeResolver needed for serialization!)
 * - Send different event types to different topics
 * - Use header-based type information for consumers
 *
 * **Note:** JsonSerializer doesn't need a typeResolver because we already know
 * the type when serializing. TypeResolver is only needed for deserialization.
 */
class JsonProducer(properties: MutableMap<String, Any>) : ProducerBase(properties) {
    override suspend fun run() {
        val objectMapper = ObjectMapper().apply {
            registerKotlinModule()
        }

        // Create JSON serializer - no typeResolver needed!
        val serializer = JsonSerializer.byteArray(objectMapper)

        val messageBuilder = OutgoingMessageBuilder.create<ByteArray?, ByteArray?>(serializer)

        val producer = QuafkaProducerBuilder<ByteArray?, ByteArray?>(properties)
            .withSerializer(ByteArraySerializer(), ByteArraySerializer())
            .build()

        try {
            logger.info("=== JSON Producer Example ===")

            // Send OrderCreated events
            repeat(5) { index ->
                val order = OrderCreated(
                    orderId = "order-$index",
                    userId = "user-$index",
                    amount = 100.0 * (index + 1),
                    productName = "Product $index"
                )

                val message = messageBuilder
                    .newMessageWithTypeInfo(
                        topic = "demo-orders",
                        key = order.orderId.toByteArray(),
                        value = order
                    )
                    .build()

                val result = producer.send(message)
                logger.info("OrderCreated sent: orderId={}, topic={}", order.orderId, result.recordMetadata.topic())
            }

            // Send UserRegistered events
            repeat(3) { index ->
                val user = UserRegistered(
                    userId = "user-$index",
                    email = "user$index@example.com",
                    name = "User $index"
                )

                val message = messageBuilder
                    .newMessageWithTypeInfo(
                        topic = "demo-users",
                        key = user.userId.toByteArray(),
                        value = user
                    )
                    .build()

                val result = producer.send(message)
                logger.info("UserRegistered sent: userId={}, topic={}", user.userId, result.recordMetadata.topic())
            }

            logger.info("=== JSON Producer completed successfully ===")
        } finally {
            producer.close()
        }
    }
}
