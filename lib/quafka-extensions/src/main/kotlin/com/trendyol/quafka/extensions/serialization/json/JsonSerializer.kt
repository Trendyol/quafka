package com.trendyol.quafka.extensions.serialization.json

import com.fasterxml.jackson.databind.ObjectMapper
import com.trendyol.quafka.common.DefaultCharset
import com.trendyol.quafka.extensions.serialization.MessageSerializer
import java.nio.charset.Charset

/**
 * JSON message serialization for Kafka messages.
 *
 * Supports both String and ByteArray formats through a single implementation.
 *
 * **Usage:**
 * ```kotlin
 * // String-based messages
 * val serializer = JsonSerializer.string(objectMapper)
 *
 * // ByteArray-based messages
 * val serializer = JsonSerializer.byteArray(objectMapper)
 * ```
 */
open class JsonSerializer<TValue>(protected val objectMapper: ObjectMapper, private val targetValueType: Class<TValue>) :
    MessageSerializer<TValue> {

    @Suppress("UNCHECKED_CAST")
    override fun serialize(value: Any?): TValue {
        if (value == null) return null as TValue
        return when {
            // Explicit targets first
            targetValueType === ByteArray::class.java -> when (value) {
                is ByteArray -> value
                else -> objectMapper.writeValueAsBytes(value)
            }

            // Default to String JSON
            else -> objectMapper.writeValueAsString(value)
        } as TValue
    }

    companion object {
        /**
         * Creates a JSON serializer for String-based Kafka messages.
         *
         * @param objectMapper The Jackson ObjectMapper for JSON operations.
         * @param charset The charset for encoding. Defaults to [DefaultCharset].
         */
        @JvmStatic
        @JvmOverloads
        fun string(
            objectMapper: ObjectMapper = ObjectMapper(),
            charset: Charset = DefaultCharset
        ): JsonSerializer<String?> = create(
            objectMapper
        )

        /**
         * Creates a JSON serializer for ByteArray-based Kafka messages.
         *
         * @param objectMapper The Jackson ObjectMapper for JSON operations.
         * @param charset The charset for encoding/decoding. Defaults to [DefaultCharset].
         */
        @JvmStatic
        @JvmOverloads
        fun byteArray(
            objectMapper: ObjectMapper = ObjectMapper()
        ): JsonSerializer<ByteArray?> = create(
            objectMapper
        )

        @JvmStatic
        @JvmOverloads
        inline fun <reified TValue> create(
            objectMapper: ObjectMapper = ObjectMapper()
        ): JsonSerializer<TValue> = JsonSerializer(
            objectMapper,
            TValue::class.java
        )
    }
}
