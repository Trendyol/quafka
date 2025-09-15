package com.trendyol.quafka.extensions.consumer.single.pipelines

import java.util.concurrent.ConcurrentHashMap

class Attributes {
    private val map: MutableMap<AttributeKey<*>, Any> = ConcurrentHashMap()

    @Suppress("UNCHECKED_CAST")
    fun <T : Any> getOrNull(key: AttributeKey<T>): T? = map[key] as T?

    operator fun contains(key: AttributeKey<*>): Boolean = map.containsKey(key)

    operator fun <T : Any> get(key: AttributeKey<T>): T = getOrNull(key) ?: error("No value for key $key")

    fun <T : Any> put(
        key: AttributeKey<T>,
        value: T
    ) {
        map[key] = value
    }

    fun <T : Any> remove(key: AttributeKey<T>) {
        map.remove(key)
    }

    public fun <T : Any> take(key: AttributeKey<T>): T = get(key).also { remove(key) }

    public fun <T : Any> takeOrNull(key: AttributeKey<T>): T? = getOrNull(key).also { remove(key) }

    val allKeys: Collection<AttributeKey<*>>
        get() = map.keys

    val allValues: Collection<Any>
        get() = map.values

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
