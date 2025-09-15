package com.trendyol.quafka.extensions.common

inline fun <reified TEnum : Enum<TEnum>> String?.toEnum(
    defaultValue: TEnum,
    ignoreCase: Boolean = true
): TEnum {
    if (this == null) {
        return defaultValue
    }

    return enumValues<TEnum>().find { it.name.equals(this, ignoreCase = ignoreCase) } ?: defaultValue
}
