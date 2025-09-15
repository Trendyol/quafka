package com.trendyol.quafka.consumer

import com.trendyol.quafka.common.ToStringBuilderBase
import org.slf4j.event.Level

/**
 * Formatter for creating string representations of incoming messages.
 */
open class IncomingMessageStringFormatter : ToStringBuilderBase() {
    /**
     * Formats an [IncomingMessage] into a string.
     *
     * @param incomingMessage The incoming message to format.
     * @param logLevel The logging level (e.g., INFO, DEBUG).
     * @param addValue If `true`, includes the message value in the output.
     * @param addHeaders If `true`, includes the message headers in the output.
     * @return A formatted string representation of the incoming message.
     */
    open fun format(
        incomingMessage: IncomingMessage<*, *>,
        logLevel: Level,
        addValue: Boolean = false,
        addHeaders: Boolean = false
    ): String = buildString {
        appendBaseIncomingDetails(incomingMessage)
        conditionallyAppendField(this, addValue, "value") { formatValue(incomingMessage.value) }
        conditionallyAppendField(this, addHeaders, "headers") { formatHeaders(incomingMessage.headers) }
    }

    /**
     * Appends the base details of an incoming message to a string builder.
     *
     * @param stringBuilder The [StringBuilder] to append to.
     * @param incomingMessage The incoming message whose details are to be appended.
     */
    private fun StringBuilder.appendBaseIncomingDetails(incomingMessage: IncomingMessage<*, *>) {
        append("topic: ${incomingMessage.topic} | partition: ${incomingMessage.partition}")
        append(" | offset: ${incomingMessage.offset} | timestamp: ${incomingMessage.timestamp}")
        append(" | key: ${formatValue(incomingMessage.key)}")
    }

    /**
     * Default instance of [IncomingMessageStringFormatter].
     */
    companion object {
        val Default = IncomingMessageStringFormatter()
    }
}
