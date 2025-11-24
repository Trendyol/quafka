package com.trendyol.quafka.extensions.common.pipelines

import java.util.concurrent.ConcurrentHashMap

/**
 * A thread-safe, type-safe container for storing arbitrary key-value pairs (attributes).
 *
 * Attributes are used to attach metadata to envelopes as they flow through a pipeline.
 * Middleware can use attributes to share data without modifying the envelope itself.
 *
 * This class is backed by a [ConcurrentHashMap], making it safe for concurrent access.
 *
 * @see AttributeKey
 *
 * @sample
 * ```kotlin
 * val attributes = Attributes()
 *
 * // Store values
 * val userKey = AttributeKey<String>("userId")
 * attributes.put(userKey, "user123")
 *
 * // Retrieve values
 * val userId = attributes.getOrNull(userKey)
 * ```
 */
class Attributes {
    private val map: MutableMap<AttributeKey<*>, Any> = ConcurrentHashMap()

    /**
     * Retrieves the value associated with the given key, or `null` if not present.
     *
     * @param T The type of the value.
     * @param key The attribute key to look up.
     * @return The value associated with the key, or `null` if not found.
     */
    @Suppress("UNCHECKED_CAST")
    fun <T : Any> getOrNull(key: AttributeKey<T>): T? = map[key] as T?

    /**
     * Checks if the attributes contain a value for the given key.
     *
     * @param key The attribute key to check.
     * @return `true` if the key exists, `false` otherwise.
     */
    operator fun contains(key: AttributeKey<*>): Boolean = map.containsKey(key)

    /**
     * Retrieves the value associated with the given key.
     *
     * @param T The type of the value.
     * @param key The attribute key to look up.
     * @return The value associated with the key.
     * @throws IllegalStateException if the key is not present.
     */
    operator fun <T : Any> get(key: AttributeKey<T>): T = getOrNull(key) ?: error("No value for key $key")

    /**
     * Stores a value associated with the given key.
     *
     * If a value already exists for the key, it will be replaced.
     *
     * @param T The type of the value.
     * @param key The attribute key.
     * @param value The value to store.
     */
    fun <T : Any> put(
        key: AttributeKey<T>,
        value: T
    ) {
        map[key] = value
    }

    /**
     * Removes the value associated with the given key.
     *
     * @param T The type of the value.
     * @param key The attribute key to remove.
     */
    fun <T : Any> remove(key: AttributeKey<T>) {
        map.remove(key)
    }

    /**
     * Retrieves and removes the value associated with the given key.
     *
     * @param T The type of the value.
     * @param key The attribute key.
     * @return The value that was associated with the key.
     * @throws IllegalStateException if the key is not present.
     */
    public fun <T : Any> take(key: AttributeKey<T>): T = get(key).also { remove(key) }

    /**
     * Retrieves and removes the value associated with the given key, or `null` if not present.
     *
     * @param T The type of the value.
     * @param key The attribute key.
     * @return The value that was associated with the key, or `null` if not found.
     */
    public fun <T : Any> takeOrNull(key: AttributeKey<T>): T? = getOrNull(key).also { remove(key) }

    /**
     * Returns all attribute keys currently stored.
     *
     * @return A collection of all attribute keys.
     */
    val allKeys: Collection<AttributeKey<*>>
        get() = map.keys

    /**
     * Returns all attribute values currently stored.
     *
     * @return A collection of all attribute values.
     */
    val allValues: Collection<Any>
        get() = map.values

    /**
     * Computes and stores a value if the key is not already present.
     *
     * This operation is atomic. If multiple threads attempt to compute a value
     * for the same key, only one will execute the block.
     *
     * @param T The type of the value.
     * @param key The attribute key.
     * @param block A function to compute the value if the key is absent.
     * @return The existing value or the newly computed value.
     *
     * @sample
     * ```kotlin
     * val counter = attributes.computeIfAbsent(AttributeKey<Int>("counter")) { 0 }
     * ```
     */
    fun <T : Any> computeIfAbsent(
        key: AttributeKey<T>,
        block: () -> T
    ): T {
        @Suppress("UNCHECKED_CAST")
        map[key]?.let { return it as T }
        val result = block()
        @Suppress("UNCHECKED_CAST")
        return (map.putIfAbsent(key, result) ?: result) as T
    }
}
