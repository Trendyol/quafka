package com.trendyol.quafka.extensions.common

import kotlin.reflect.*

public class TypeInfo(public val type: KClass<*>, public val kotlinType: KType? = null) {
    public constructor(
        type: KClass<*>,
        reifiedType: java.lang.reflect.Type,
        kotlinType: KType? = null
    ) : this(type, kotlinType)

    override fun hashCode(): Int = kotlinType?.hashCode() ?: type.hashCode()

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other !is TypeInfo) return false

        return if (kotlinType != null || other.kotlinType != null) {
            kotlinType == other.kotlinType
        } else {
            type == other.type
        }
    }

    override fun toString(): String = "TypeInfo(${kotlinType ?: type})"
}

public inline fun <reified T> typeInfo(): TypeInfo = TypeInfo(T::class, typeOfOrNull<T>())

@PublishedApi
internal inline fun <reified T> typeOfOrNull(): KType? = try {
    typeOf<T>()
} catch (_: Throwable) {
    null
}
