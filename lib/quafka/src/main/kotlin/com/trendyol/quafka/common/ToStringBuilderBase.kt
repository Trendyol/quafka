package com.trendyol.quafka.common

import com.trendyol.quafka.common.HeaderParsers.key
import com.trendyol.quafka.common.HeaderParsers.value
import org.apache.kafka.common.header.Header

/**
 * Base class for building string representations of objects.
 *
 * Provides utility functions for formatting values, headers, and conditionally appending fields to a string builder.
 */
open class ToStringBuilderBase {
    /**
     * Formats a value for inclusion in a string representation.
     *
     * @param value The value to format. Can be `null`, a `String`, a `ByteArray`, or any other type.
     * @return A formatted string representation of the value.
     */
    open fun formatValue(value: Any?): String = when (value) {
        null -> ""
        is String -> value
        is ByteArray -> String(value, HeaderParsers.defaultCharset)
        else -> value.toString()
    }

    /**
     * Formats a collection of message headers into a string.
     *
     * @param headers An iterable collection of [Header].
     * @return A string representation of the headers.
     */
    open fun formatHeaders(headers: Iterable<Header>): String = headers.joinToString {
        "${it.key} : ${formatValue(it.value)}"
    }

    /**
     * Conditionally appends a field to a string builder.
     *
     * @param stringBuilder The [StringBuilder] to append to.
     * @param condition A condition that determines whether the field should be appended.
     * @param fieldName The name of the field to append.
     * @param valueProvider A lambda that provides the value of the field as a string.
     */
    open fun conditionallyAppendField(
        stringBuilder: StringBuilder,
        condition: Boolean,
        fieldName: String,
        valueProvider: () -> String
    ) {
        if (condition) {
            stringBuilder.append(" | $fieldName: ${valueProvider()}")
        }
    }
}
