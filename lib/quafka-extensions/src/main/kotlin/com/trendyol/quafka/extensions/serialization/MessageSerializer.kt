package com.trendyol.quafka.extensions.serialization

/**
 * Interface for serializing objects to Kafka message values.
 *
 * This interface provides a contract for custom serialization implementations
 * that convert typed objects into serialized format (e.g., ByteArray, String)
 * for producing to Kafka.
 *
 * @param TValue The serialized type of the message value (e.g., String, ByteArray).
 *
 * @see MessageDeserializer
 * @see JsonSerializer
 * @see OutgoingMessageBuilder
 *
 * @sample
 * ```kotlin
 * class CustomSerializer : MessageSerializer<ByteArray?> {
 *     private val objectMapper = ObjectMapper()
 *
 *     override fun serialize(value: Any?): ByteArray? {
 *         if (value == null) return null
 *
 *         return try {
 *             objectMapper.writeValueAsBytes(value)
 *         } catch (e: Exception) {
 *             throw SerializationException("Failed to serialize: ${e.message}", e)
 *         }
 *     }
 * }
 * ```
 */
interface MessageSerializer<TValue> {

    /**
     * Serializes a value object for producing to Kafka.
     *
     * This method converts a typed object into the serialized format that will
     * be sent to Kafka. The implementation should handle null values appropriately
     * and throw exceptions for serialization failures.
     *
     * @param value The value object to serialize (can be null for tombstone messages).
     * @return The serialized value in the target format, or null if value is null.
     * @throws Exception If serialization fails.
     *
     * @sample
     * ```kotlin
     * // Using the serializer
     * val serializer = JsonSerializer.byteArray(objectMapper)
     * val messageBuilder = OutgoingMessageBuilder.create(serializer)
     *
     * val order = OrderCreated(orderId = "123", amount = 100.0)
     * val message = messageBuilder
     *     .newMessageWithTypeInfo("orders", "order-123", order)
     *     .build()
     *
     * producer.send(message)
     * ```
     */
    fun serialize(value: Any?): TValue
}
