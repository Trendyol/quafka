package com.trendyol.quafka.extensions.serialization

import com.trendyol.quafka.consumer.IncomingMessage

/**
 * Interface for deserializing Kafka message values.
 *
 * This interface provides a contract for custom deserialization implementations
 * that convert serialized message values (e.g., ByteArray, String) into typed objects.
 *
 * **Design Notes:**
 * - Returns [DeserializationResult] for explicit error handling
 * - Supports nullable values for tombstone messages
 * - TKey and TValue represent the serialized types from Kafka (e.g., ByteArray)
 *
 * @param TKey The serialized type of the message key (e.g., String, ByteArray).
 * @param TValue The serialized type of the message value (e.g., String, ByteArray).
 *
 * @see DeserializationResult
 * @see MessageSerializer
 * @see JsonDeserializer
 *
 * @sample
 * ```kotlin
 * class CustomDeserializer : MessageDeserializer<String?, ByteArray?> {
 *     override fun <T> deserialize(
 *         incomingMessage: IncomingMessage<String?, ByteArray?>
 *     ): DeserializationResult<out T> {
 *         val rawValue = incomingMessage.value ?: return DeserializationResult.Null
 *
 *         return try {
 *             val parsed = parseJson(rawValue)
 *             DeserializationResult.Deserialized(parsed as T)
 *         } catch (e: Exception) {
 *             DeserializationResult.Error(
 *                 topicPartitionOffset = incomingMessage.topicPartitionOffset,
 *                 message = "Failed to deserialize: ${e.message}",
 *                 cause = e
 *             )
 *         }
 *     }
 * }
 * ```
 */
interface MessageDeserializer<TKey, TValue> {
    /**
     * Deserializes the value of an incoming message into a typed object.
     *
     * This method converts the serialized message value into the target type,
     * returning a [DeserializationResult] that explicitly represents success,
     * failure, or null value cases.
     *
     * **Implementation Guidelines:**
     * - Return [DeserializationResult.Null] for null/tombstone messages
     * - Return [DeserializationResult.Error] for deserialization failures
     * - Return [DeserializationResult.Deserialized] for successful deserialization
     * - Include detailed error information in Error results
     *
     * @param T The target type to deserialize to.
     * @param incomingMessage The incoming message containing the serialized value.
     * @return A [DeserializationResult] indicating success, failure, or null value.
     *
     * @sample
     * ```kotlin
     * // Using the deserializer
     * when (val result = deserializer.deserialize<OrderCreated>(message)) {
     *     is DeserializationResult.Deserialized -> {
     *         processOrder(result.value)
     *     }
     *     is DeserializationResult.Error -> {
     *         logger.error("Deserialization failed", result.cause)
     *     }
     *     DeserializationResult.Null -> {
     *         logger.info("Tombstone message received")
     *     }
     * }
     * ```
     */
    fun <T> deserialize(incomingMessage: IncomingMessage<TKey, TValue>): DeserializationResult<out T>
}
