package com.trendyol.quafka.extensions.serialization.json

import com.fasterxml.jackson.databind.*
import com.trendyol.quafka.common.DefaultCharset
import com.trendyol.quafka.consumer.IncomingMessage
import com.trendyol.quafka.extensions.serialization.*
import java.nio.charset.*

/**
 * JSON message deserialization for Kafka messages.
 *
 * Supports both String and ByteArray formats through a single implementation.
 *
 * **Type Resolution:**
 * The typeResolver lambda receives:
 * - `jsonNode`: The parsed JSON (for content-based resolution)
 * - `message`: The incoming message (for header/topic-based resolution)
 *
 * **Performance:**
 * JSON is parsed once into JsonNode, then reused for both type resolution
 * and deserialization.
 *
 * **Usage:**
 * ```kotlin
 * // String-based messages
 * val deserializer = JsonDeserializer.string(objectMapper) { jsonNode, message ->
 *     message.headers.get("X-MessageType")?.asString()?.let {
 *         Class.forName(it)
 *     }
 * }
 *
 * // ByteArray-based messages
 * val deserializer = JsonDeserializer.byteArray(objectMapper) { jsonNode, message ->
 *     message.headers.get("X-MessageType")?.asString()?.let {
 *         Class.forName(it)
 *     }
 * }
 *
 * // Using helpers
 * val deserializer = JsonDeserializer.string(objectMapper, TypeResolvers.fromHeader("X-MessageType"))
 * ```
 */
open class JsonDeserializer<TKey, TValue> protected constructor(
    protected val objectMapper: ObjectMapper,
    protected val typeResolver: TypeResolver<TKey, TValue>
) : MessageDeserializer<TKey, TValue> {

    override fun <T> deserialize(incomingMessage: IncomingMessage<TKey, TValue>): DeserializationResult<out T> {
        val rawValue = incomingMessage.value ?: return DeserializationResult.Null

        // Step 1: Parse JSON once
        val jsonNode = try {
            convertToJsonNode(rawValue)
        } catch (e: Exception) {
            return DeserializationResult.Error(
                topicPartitionOffset = incomingMessage.topicPartitionOffset,
                message = "error occurred deserializing value as json",
                cause = e
            )
        }

        // Step 2: Resolve type using the parsed JsonNode
        val targetType = try {
            typeResolver(jsonNode, incomingMessage)
        } catch (e: Exception) {
            return DeserializationResult.Error(
                topicPartitionOffset = incomingMessage.topicPartitionOffset,
                message = "An error occurred when resolving type",
                cause = e
            )
        }

        if (targetType == null) {
            return DeserializationResult.Error(
                topicPartitionOffset = incomingMessage.topicPartitionOffset,
                message = "Type not resolved!!"
            )
        }

        // Step 3: Convert JsonNode to target type
        return try {
            val value = objectMapper.convertValue(jsonNode, targetType) as T
            DeserializationResult.Deserialized<T>(value)
        } catch (e: Exception) {
            DeserializationResult.Error(
                topicPartitionOffset = incomingMessage.topicPartitionOffset,
                message = "error occurred binding message to object, body: $jsonNode, type: ${targetType.name}",
                cause = e
            )
        }
    }

    /**
     * Parses raw value to JsonNode.
     * Override this to customize JSON parsing.
     */
    protected open fun convertToJsonNode(rawValue: Any): JsonNode = when (rawValue) {
        is ByteArray -> objectMapper.readTree(rawValue)
        is String -> objectMapper.readTree(rawValue)
        else -> throw IllegalArgumentException("Unsupported value type: ${rawValue::class.java.name}")
    }

    companion object {
        /**
         * Creates a JSON deserializer for String-based Kafka messages.
         *
         * @param objectMapper The Jackson ObjectMapper for JSON operations.
         * @param charset The charset for decoding. Defaults to [DefaultCharset].
         * @param typeResolver Lambda that resolves the target type from the parsed JSON and message.
         */
        @JvmStatic
        @JvmOverloads
        fun string(
            objectMapper: ObjectMapper = ObjectMapper(),
            typeResolver: TypeResolver<String?, String?>
        ): JsonDeserializer<String?, String?> = JsonDeserializer(
            objectMapper,
            typeResolver
        )

        /**
         * Creates a JSON deserializer for ByteArray-based Kafka messages.
         *
         * @param objectMapper The Jackson ObjectMapper for JSON operations.
         * @param charset The charset for encoding/decoding. Defaults to [DefaultCharset].
         * @param typeResolver Lambda that resolves the target type from the parsed JSON and message.
         */
        @JvmStatic
        @JvmOverloads
        fun byteArray(
            objectMapper: ObjectMapper = ObjectMapper(),
            typeResolver: TypeResolver<ByteArray?, ByteArray?>
        ): JsonDeserializer<ByteArray?, ByteArray?> = JsonDeserializer(
            objectMapper,
            typeResolver
        )

        @JvmStatic
        @JvmOverloads
        fun <TKey, TValue> create(
            objectMapper: ObjectMapper = ObjectMapper(),
            typeResolver: TypeResolver<TKey, TValue>
        ): JsonDeserializer<TKey, TValue> = JsonDeserializer(
            objectMapper,
            typeResolver
        )
    }
}
