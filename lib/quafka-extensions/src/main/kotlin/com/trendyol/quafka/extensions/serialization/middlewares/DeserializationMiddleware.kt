package com.trendyol.quafka.extensions.serialization.middlewares

import com.trendyol.quafka.extensions.common.pipelines.MiddlewareFn
import com.trendyol.quafka.extensions.consumer.single.pipelines.*
import com.trendyol.quafka.extensions.serialization.*
import com.trendyol.quafka.extensions.serialization.middlewares.DeserializationMiddleware.Companion.DeserializedValueAttributeKey
import com.trendyol.quafka.extensions.serialization.middlewares.DeserializationMiddleware.Companion.getDeserializedValue

/**
 * Pipeline middleware for deserializing message keys and values.
 *
 * This middleware automatically deserializes incoming messages and stores the
 * deserialized objects in the message envelope's attributes. Subsequent middleware
 * in the pipeline can then access the typed objects without needing to handle
 * deserialization themselves.
 *
 * **Attribute Key:**
 * - Deserialized values are stored with [DeserializedValueAttributeKey]
 *
 * **Error Handling:**
 * - Deserialization errors are thrown as exceptions
 * - Null handling is configurable via constructor parameters
 *
 * @param TEnvelope The type of envelope (must extend [SingleMessageEnvelope]).
 * @param TKey The serialized type of the message key.
 * @param TValue The serialized type of the message value.
 * @property messageDeserializer The serialization implementation for deserialization.
 * @property throwExceptionIfValueIsNull If true, throws exception when value deserializes to null. Defaults to true.
 *
 * @see MessageDeserializer
 * @see getDeserializedValue
 *
 * @sample
 * ```kotlin
 * // Basic usage with pipeline
 * usePipelineMessageHandler(messageSerde) {
 *     use(DeserializationMiddleware(messageSerde))
 *
 *     use { envelope, next ->
 *         // Access deserialized value
 *         val order = envelope.getDeserializedValue<OrderCreated>()
 *         logger.info("Processing order: ${order?.orderId}")
 *         next()
 *     }
 * }
 * ```
 *
 * @sample
 * ```kotlin
 * // Allow null values (e.g., for tombstone messages)
 * val deserializer = DeserializationMiddleware(
 *     messageSerde = serde,
 *     throwExceptionIfValueIsNull = false
 * )
 *
 * usePipelineMessageHandler(serde) {
 *     use(deserializer)
 *
 *     use { envelope, next ->
 *         val value = envelope.getDeserializedValue<OrderEvent>()
 *         if (value == null) {
 *             logger.info("Tombstone message detected")
 *         } else {
 *             processEvent(value)
 *         }
 *         next()
 *     }
 * }
 * ```
 */
class DeserializationMiddleware<TEnvelope, TKey, TValue>(
    val messageDeserializer: MessageDeserializer<TKey, TValue>,
    val throwExceptionIfValueIsNull: Boolean = true
) : SingleMessageBaseMiddleware<TEnvelope, TKey, TValue>() where TEnvelope : SingleMessageEnvelope<TKey, TValue> {
    companion object {

        /**
         * Attribute key for storing deserialized message values.
         * Used internally by [DeserializationMiddleware].
         */
        @PublishedApi
        internal val DeserializedValueAttributeKey =
            com.trendyol.quafka.extensions.common.pipelines.AttributeKey<Any>("DeserializedValue")

        /**
         * Extension function to retrieve the deserialized value from the envelope's attributes.
         *
         * This function provides type-safe access to the deserialized message value that was
         * stored by [DeserializationMiddleware].
         *
         * @param T The expected type of the deserialized value.
         * @return The deserialized value cast to type T, or null if not present.
         *
         * @sample
         * ```kotlin
         * use { envelope, next ->
         *     val order = envelope.getDeserializedValue<OrderCreated>()
         *     order?.let {
         *         logger.info("Order ${it.orderId} created")
         *         processOrder(it)
         *     }
         *     next()
         * }
         * ```
         */
        inline fun <reified T> SingleMessageEnvelope<*, *>.getDeserializedValue(): T? = this.attributes.getOrNull(
            DeserializedValueAttributeKey
        ) as T
    }

    /**
     * Executes the deserialization middleware.
     *
     * Deserializes both the key and value, stores them in the envelope's attributes,
     * and passes control to the next middleware.
     *
     * @param envelope The message envelope to process.
     * @param next The next middleware function in the pipeline.
     * @throws DeserializationException if deserialization fails.
     */
    override suspend fun execute(
        envelope: TEnvelope,
        next: MiddlewareFn<TEnvelope>
    ) {
        deserializeValue(envelope)?.let { value -> envelope.attributes.put(DeserializedValueAttributeKey, value) }
        next(envelope)
    }

    /**
     * Deserializes the message value using the configured [MessageSerde].
     *
     * @param envelope The message envelope containing the value to deserialize.
     * @return The deserialized value object, or null if allowed.
     * @throws DeserializationException if deserialization fails or null handling is violated.
     */
    private fun deserializeValue(envelope: TEnvelope): Any? = when (
        val deserializationResult = messageDeserializer
            .deserialize<Any>(
                envelope.message
            )
    ) {
        is DeserializationResult.Deserialized<*> -> deserializationResult.value
        is DeserializationResult.Error -> throw deserializationResult.toException()
        is DeserializationResult.Null -> if (throwExceptionIfValueIsNull) {
            throw deserializationResult
                .toError(envelope.message.topicPartitionOffset, "deserialized value cannot be null")
                .toException()
        } else {
            null
        }
    }
}
