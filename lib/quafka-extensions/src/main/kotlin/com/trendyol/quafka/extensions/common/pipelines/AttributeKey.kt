package com.trendyol.quafka.extensions.common.pipelines

import com.trendyol.quafka.extensions.common.*

/**
 * A type-safe key for storing and retrieving values in [Attributes].
 *
 * Attribute keys provide a type-safe mechanism for associating metadata with envelopes
 * as they flow through a pipeline. Each key has a name and an associated type, ensuring
 * that values retrieved using the key are of the correct type.
 *
 * @param T The type of value associated with this key.
 * @property name The unique name of the attribute key. Must not be blank.
 * @property type The type information for type-safe operations.
 *
 * @see Attributes
 *
 * @sample
 * ```kotlin
 * // Create a typed key
 * val userIdKey = AttributeKey<String>("userId")
 *
 * // Store and retrieve values
 * attributes.put(userIdKey, "user123")
 * val userId = attributes.getOrNull(userIdKey) // Returns String?
 * ```
 */
data class AttributeKey<T>
@JvmOverloads
constructor(public val name: String, public val type: TypeInfo = typeInfo<Any>()) {
    init {
        require(name.isNotBlank()) { "Name can't be blank" }
    }

    override fun toString(): String = "AttributeKey: $name"

    companion object {
        /**
         * Creates an [AttributeKey] for string values.
         *
         * @param name The name of the attribute key.
         * @return A new [AttributeKey] for [String] values.
         */
        fun string(name: String): AttributeKey<String> = AttributeKey(name)
    }
}

/**
 * Creates a type-safe [AttributeKey] with reified type information.
 *
 * This is the preferred way to create attribute keys as it captures the full type information.
 *
 * @param T The type of value associated with this key.
 * @param name The unique name of the attribute key.
 * @return A new [AttributeKey] with the specified type.
 *
 * @sample
 * ```kotlin
 * val counterKey = AttributeKey<Int>("requestCounter")
 * val userKey = AttributeKey<User>("currentUser")
 * ```
 */
@JvmSynthetic
public inline fun <reified T : Any> AttributeKey(name: String): AttributeKey<T> =
    AttributeKey(name, typeInfo<T>())

/**
 * Copies all attributes from another [Attributes] instance into this one.
 *
 * @param other The source attributes to copy from.
 *
 * @sample
 * ```kotlin
 * val source = Attributes()
 * source.put(AttributeKey<String>("key"), "value")
 *
 * val target = Attributes()
 * target.putAll(source)
 * ```
 */
public fun Attributes.putAll(other: Attributes) {
    other.allKeys.forEach {
        @Suppress("UNCHECKED_CAST")
        put(it as AttributeKey<Any>, other[it])
    }
}

/**
 * Retrieves a value from attributes or returns a default value if the key is not present.
 *
 * @param K The key type.
 * @param V The value type.
 * @param key The attribute key to look up.
 * @param default The default value to return if the key is not found.
 * @return The value associated with the key, or the default value.
 *
 * @sample
 * ```kotlin
 * val timeout = attributes.getOrDefault(AttributeKey<Int>("timeout"), 30)
 * ```
 */
@Suppress("UNCHECKED_CAST")
fun <K : Any, V> Attributes.getOrDefault(
    key: AttributeKey<K>,
    default: V
): V = this.getOrNull(key) as V ?: default

/**
 * Stores a value in attributes with type-safe key handling.
 *
 * This is a convenience extension that handles type casting internally.
 *
 * @param K The key type.
 * @param V The value type.
 * @param key The attribute key.
 * @param value The value to store.
 *
 * @sample
 * ```kotlin
 * attributes.put(AttributeKey<String>("userId"), "user123")
 * ```
 */
@Suppress("UNCHECKED_CAST")
fun <K : Any, V : Any> Attributes.put(
    key: AttributeKey<K>,
    value: V
) = put(key as AttributeKey<V>, value)
