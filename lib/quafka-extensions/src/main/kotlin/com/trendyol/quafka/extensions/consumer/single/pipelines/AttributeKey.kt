package com.trendyol.quafka.extensions.consumer.single.pipelines

import com.trendyol.quafka.extensions.common.*

data class AttributeKey<T>
    @JvmOverloads
    constructor(
        public val name: String,
        public val type: TypeInfo = typeInfo<Any>()
    ) {
        init {
            require(name.isNotBlank()) { "Name can't be blank" }
        }

        override fun toString(): String = "AttributeKey: $name"

        companion object {
            fun string(name: String): AttributeKey<String> = AttributeKey(name)
        }
    }

@JvmSynthetic
public inline fun <reified T : Any> AttributeKey(name: String): AttributeKey<T> =
    AttributeKey(name, typeInfo<T>())

public fun Attributes.putAll(other: Attributes) {
    other.allKeys.forEach {
        @Suppress("UNCHECKED_CAST")
        put(it as AttributeKey<Any>, other[it])
    }
}

@Suppress("UNCHECKED_CAST")
fun <K : Any, V> Attributes.getOrDefault(
    key: AttributeKey<K>,
    default: V
): V = this.getOrNull(key) as V ?: default

@Suppress("UNCHECKED_CAST")
fun <K : Any, V : Any> Attributes.put(
    key: AttributeKey<K>,
    value: V
) = put(key as AttributeKey<V>, value)
