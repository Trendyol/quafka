package com.trendyol.quafka.extensions.serialization

import com.trendyol.quafka.consumer.TopicPartitionOffset

/**
 * Represents the result of a message deserialization operation.
 *
 * This sealed class provides type-safe handling of deserialization outcomes,
 * allowing consumers to explicitly handle success, failure, and null value cases
 * without relying on exceptions.
 *
 * **Usage Pattern:**
 * ```kotlin
 * when (val result = deserializer.deserialize<Order>(message)) {
 *     is DeserializationResult.Deserialized -> {
 *         // Success case - process the value
 *         processOrder(result.value)
 *     }
 *     is DeserializationResult.Error -> {
 *         // Error case - log and handle
 *         logger.error("Deserialization failed", result.cause)
 *         handleError(result)
 *     }
 *     DeserializationResult.Null -> {
 *         // Null case - tombstone message
 *         handleTombstone()
 *     }
 * }
 * ```
 *
 * @param T The type of the successfully deserialized value.
 *
 * @see MessageDeserializer
 * @see JsonDeserializer
 */
sealed class DeserializationResult<T> {
    /**
     * Represents a deserialization error.
     *
     * This result indicates that deserialization failed due to malformed data,
     * type mismatches, or other issues. It contains detailed information about
     * the failure including the message location and the underlying cause.
     *
     * **Common Causes:**
     * - JSON parsing errors
     * - Type resolution failures
     * - Schema validation failures
     * - Incompatible data formats
     *
     * @property topicPartitionOffset The location of the message that failed to deserialize.
     * @property message A human-readable error message describing what went wrong.
     * @property cause The underlying exception that caused the failure, if any.
     */
    data class Error(val topicPartitionOffset: TopicPartitionOffset, val message: String, val cause: Throwable? = null) :
        DeserializationResult<Nothing>() {
        override fun toString(): String =
            "DeserializationError ($topicPartitionOffset | message: $message | cause: $cause )"

        /**
         * Converts this error into a [DeserializationException].
         *
         * This is useful when you want to throw an exception based on the deserialization
         * error, for example in error handlers or middleware that expect exceptions.
         *
         * @return A throwable exception representing this deserialization error.
         */
        fun toException(): DeserializationException = DeserializationException(
            topicPartitionOffset = topicPartitionOffset,
            message = this.toString(),
            cause = cause
        )
    }

    /**
     * Represents a null value result.
     *
     * This is distinct from an error - it indicates that the message value was
     * explicitly null. In Kafka, this typically represents a tombstone message,
     * which is used to mark a key for deletion in compacted topics.
     *
     * **Use Cases:**
     * - Tombstone messages in compacted topics
     * - Explicit deletion markers
     * - Null value handling in middleware
     */
    data object Null : DeserializationResult<Nothing>() {
        /**
         * Converts this null result into an error with a custom message.
         *
         * This is useful when null values are not expected in a particular context
         * and should be treated as an error. For example, in business logic where
         * null values should never occur.
         *
         * @param topicPartitionOffset The location of the message.
         * @param message A descriptive error message explaining why null is not allowed.
         * @return A [DeserializationResult.Error] instance representing this null as an error.
         */
        fun toError(
            topicPartitionOffset: TopicPartitionOffset,
            message: String
        ) = Error(topicPartitionOffset, message)
    }

    /**
     * Represents a successfully deserialized value.
     *
     * This result indicates that deserialization was successful and the typed
     * object is available for processing.
     *
     * @property value The deserialized object, strongly typed to [T].
     */
    data class Deserialized<T>(val value: T) : DeserializationResult<T>()
}
