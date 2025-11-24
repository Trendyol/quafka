package com.trendyol.quafka.extensions.serialization.json

import com.fasterxml.jackson.databind.JsonNode
import com.trendyol.quafka.common.HeaderParsers.asString
import com.trendyol.quafka.common.get
import com.trendyol.quafka.common.header
import com.trendyol.quafka.consumer.IncomingMessage
import com.trendyol.quafka.extensions.producer.OutgoingMessageBuilder
import com.trendyol.quafka.extensions.serialization.json.TypeResolvers.DefaultMessageTypeHeaderKey

/**
 * Type resolver function signature for resolving message types during deserialization.
 *
 * @param TKey The type of the message key.
 * @param TValue The type of the message value.
 */
typealias TypeResolver<TKey, TValue> = (JsonNode, IncomingMessage<TKey, TValue>) -> Class<*>?

/**
 * Common type resolution strategies for JSON deserialization.
 *
 * All resolvers receive both the parsed JsonNode and the IncomingMessage,
 * allowing for efficient type resolution without double-parsing JSON.
 *
 * **Usage:**
 * ```kotlin
 * // From single message header
 * val deserializer = JsonDeserializer.string(objectMapper, TypeResolvers.fromHeader("X-MessageType"))
 *
 * // From multiple headers (checks in order)
 * val deserializer = JsonDeserializer.string(objectMapper,
 *     TypeResolvers.fromHeader(listOf("X-MessageType", "X-EventType"))
 * )
 *
 * // From JSON field
 * val deserializer = JsonDeserializer.string(objectMapper, TypeResolvers.fromJsonField("@type"))
 *
 * // Using a mapping
 * val deserializer = JsonDeserializer.string(objectMapper, TypeResolvers.fromMapping(mapOf(
 *     "OrderCreated" to OrderCreatedEvent::class.java,
 *     "OrderUpdated" to OrderUpdatedEvent::class.java
 * )))
 *
 * // Fixed type
 * val deserializer = JsonDeserializer.string(objectMapper, TypeResolvers.fixed(OrderEvent::class.java))
 *
 * // Migrate from old TypeResolver
 * val oldResolver = AutoPackageNameBasedTypeResolver(HeaderAwareTypeNameExtractor())
 * val deserializer = JsonDeserializer.string(objectMapper, TypeResolvers.fromLegacy(oldResolver))
 * ```
 */
object TypeResolvers {

    @Suppress("ktlint:standard:property-naming")
    var DefaultMessageTypeHeaderKey: String = "X-MessageType"

    /**
     * Resolves type from message headers.
     *
     * Checks multiple headers in order and returns the first match.
     *
     * @param headerNames Collection of header keys to check in order.
     * @param packagePrefix Optional package prefix to prepend to the type name.
     */
    @JvmStatic
    @JvmOverloads
    fun fromHeader(
        headerNames: Collection<String> = listOf(DefaultMessageTypeHeaderKey),
        packagePrefix: String? = null
    ): TypeResolver<*, *> = { _, message ->
        headerNames.firstNotNullOfOrNull { headerName ->
            message.headers.get(headerName)?.asString()?.let { typeName ->
                val fullClassName = if (packagePrefix != null) {
                    "$packagePrefix.$typeName"
                } else {
                    typeName
                }
                try {
                    Class.forName(fullClassName)
                } catch (e: ClassNotFoundException) {
                    null
                }
            }
        }
    }

    /**
     * Resolves type from a single message header.
     *
     * Convenience method for when you have only one header to check.
     *
     * @param headerName The header key to extract the type name from.
     * @param packagePrefix Optional package prefix to prepend to the type name.
     */
    @JvmStatic
    fun fromHeader(
        headerName: String = DefaultMessageTypeHeaderKey,
        packagePrefix: String? = null
    ): TypeResolver<*, *> =
        fromHeader(listOf(headerName), packagePrefix)

    /**
     * Resolves type from a single JSON field in the message body.
     *
     * Convenience method for when you have only one field to check.
     *
     * @param fieldName The JSON field name containing the type.
     * @param packagePrefix Optional package prefix to prepend to the type name.
     */
    @JvmStatic
    fun fromJsonField(
        fieldName: String,
        packagePrefix: String? = null
    ): TypeResolver<*, *> =
        fromJsonField(listOf(fieldName), packagePrefix)

    /**
     * Resolves type from JSON fields in the message body.
     *
     * Checks multiple fields in order and returns the first match.
     *
     * @param fieldNames Collection of JSON field names to check in order.
     * @param packagePrefix Optional package prefix to prepend to the type name.
     */
    @JvmStatic
    @JvmOverloads
    fun fromJsonField(
        fieldNames: Collection<String>,
        packagePrefix: String? = null
    ): TypeResolver<*, *> = { jsonNode, _ ->
        fieldNames.firstNotNullOfOrNull { fieldName ->
            jsonNode.get(fieldName)?.asText()?.let { typeName ->
                val fullClassName = if (packagePrefix != null) {
                    "$packagePrefix.$typeName"
                } else {
                    typeName
                }
                try {
                    Class.forName(fullClassName)
                } catch (e: ClassNotFoundException) {
                    null
                }
            }
        }
    }

    /**
     * Resolves type using an explicit mapping of type names to classes.
     *
     * Convenience method for when you have only one header to check.
     *
     * @param mapping Map of type name to Java class.
     * @param headerName The header key to extract the type name from.
     */
    @JvmStatic
    fun fromMapping(
        mapping: Map<String, Class<*>>,
        headerName: String = DefaultMessageTypeHeaderKey
    ): TypeResolver<*, *> =
        fromMapping(mapping, listOf(headerName))

    /**
     * Resolves type using an explicit mapping of type names to classes.
     *
     * Checks multiple headers in order and returns the first match.
     *
     * @param mapping Map of type name to Java class.
     * @param headerNames Collection of header keys to extract the type name from.
     */
    @JvmStatic
    @JvmOverloads
    fun fromMapping(
        mapping: Map<String, Class<*>>,
        headerNames: Collection<String> = listOf(DefaultMessageTypeHeaderKey)
    ): TypeResolver<*, *> = { _, message ->
        headerNames.firstNotNullOfOrNull { headerName ->
            message.headers.get(headerName)?.asString()?.let { typeName ->
                mapping[typeName]
            }
        }
    }

    /**
     * Resolves type using an explicit mapping, extracting type name from a single JSON field.
     *
     * Convenience method for when you have only one field to check.
     *
     * @param mapping Map of type name to Java class.
     * @param fieldName The JSON field name containing the type.
     */
    @JvmStatic
    fun fromMappingJsonField(
        mapping: Map<String, Class<*>>,
        fieldName: String
    ): TypeResolver<*, *> =
        fromMappingJsonField(mapping, listOf(fieldName))

    /**
     * Resolves type using an explicit mapping, extracting type name from JSON fields.
     *
     * Checks multiple fields in order and returns the first match.
     *
     * @param mapping Map of type name to Java class.
     * @param fieldNames Collection of JSON field names to check in order.
     */
    @JvmStatic
    @JvmOverloads
    fun fromMappingJsonField(
        mapping: Map<String, Class<*>>,
        fieldNames: Collection<String> = listOf("@type")
    ): TypeResolver<*, *> = { jsonNode, _ ->
        fieldNames.firstNotNullOfOrNull { fieldName ->
            jsonNode.get(fieldName)?.asText()?.let { typeName ->
                mapping[typeName]
            }
        }
    }

    /**
     * Always resolves to a fixed type. Useful when a topic contains only one message type.
     *
     * @param type The target class to always deserialize to.
     */
    @JvmStatic
    fun <T> fixed(type: Class<T>): TypeResolver<*, *> = { _, _ -> type }

    /**
     * Chains multiple resolvers, returning the first non-null result.
     *
     * @param resolvers The resolvers to try in order.
     */
    @JvmStatic
    fun <TKey, TValue> chain(
        vararg resolvers: TypeResolver<TKey, TValue>
    ): TypeResolver<TKey, TValue> = { jsonNode, message ->
        resolvers.firstNotNullOfOrNull { resolver ->
            resolver(jsonNode, message)
        }
    }

    /**
     * Resolves type from package and simple class name in headers.
     *
     * Convenience method for when you have single headers to check.
     *
     * @param typeHeaderName Header containing the simple class name.
     * @param packageHeaderName Header containing the package name.
     */
    @JvmStatic
    fun fromPackageAndName(
        typeHeaderName: String,
        packageHeaderName: String
    ): TypeResolver<*, *> =
        fromPackageAndName(listOf(typeHeaderName), listOf(packageHeaderName))

    /**
     * Resolves type from package and simple class name in headers.
     * Useful for auto-generated type names.
     *
     * Checks multiple headers in order for both type and package names.
     *
     * @param typeHeaderNames Collection of headers containing the simple class name.
     * @param packageHeaderNames Collection of headers containing the package name.
     */
    @JvmStatic
    @JvmOverloads
    fun fromPackageAndName(
        typeHeaderNames: Collection<String> = listOf("type"),
        packageHeaderNames: Collection<String> = listOf("package")
    ): TypeResolver<*, *> = { _, message ->
        val typeName = typeHeaderNames.firstNotNullOfOrNull { headerName ->
            message.headers.get(headerName)?.asString()
        }
        val packageName = packageHeaderNames.firstNotNullOfOrNull { headerName ->
            message.headers.get(headerName)?.asString()
        }

        if (typeName != null && packageName != null) {
            try {
                Class.forName("$packageName.$typeName")
            } catch (e: ClassNotFoundException) {
                null
            }
        } else {
            null
        }
    }
}

/**
 * Extension function to create a message with automatic type information header.
 *
 * This convenience function creates a new message and automatically adds the type information
 * to the message headers using the [TypeResolvers.DefaultMessageTypeHeaderKey] (`X-MessageType`).
 *
 *
 * @param TKey The serialized type of the message key.
 * @param TValue The serialized type of the message value.
 * @param topic The Kafka topic to send the message to.
 * @param key The message key (will be serialized).
 * @param value The message value (will be serialized).
 * @param type The type name to include in the header. Defaults to the simple class name of the value.
 * @return A [OutgoingMessageBuilder] with the type header already set.
 *
 * @sample
 * ```kotlin
 * val messageBuilder = OutgoingMessageBuilder(serde)
 *
 * // Using default type (class simple name)
 * val message1 = messageBuilder.newMessageWithTypeInfo(
 *     topic = "orders",
 *     key = "order-123",
 *     value = OrderCreated(...)
 * ).build()
 * // Header: X-MessageType = "OrderCreated"
 *
 * // Using custom type name
 * val message2 = messageBuilder.newMessageWithTypeInfo(
 *     topic = "events",
 *     key = "event-456",
 *     value = CustomEvent(...),
 *     type = "com.example.events.CustomEvent"
 * ).build()
 * // Header: X-MessageType = "com.example.events.CustomEvent"
 * ```
 */
fun <TKey, TValue> OutgoingMessageBuilder<TKey, TValue>.newMessageWithTypeInfo(
    topic: String,
    key: TKey,
    value: Any,
    type: String = value.javaClass.simpleName
) = this.new(topic, key, value).withHeader(header(DefaultMessageTypeHeaderKey, type))
