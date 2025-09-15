package com.trendyol.quafka.producer

import com.trendyol.quafka.common.ToStringBuilderBase
import org.slf4j.event.Level

/**
 * Formatter for creating string representations of outgoing messages.
 */
class OutgoingMessageStringFormatter : ToStringBuilderBase() {
    /**
     * Formats an [OutgoingMessage] into a string.
     *
     * @param outgoingMessage The outgoing message to format.
     * @param logLevel The logging level (e.g., INFO, DEBUG).
     * @param addValue If `true`, includes the message value in the output.
     * @param addHeaders If `true`, includes the message headers in the output.
     * @return A formatted string representation of the outgoing message.
     */
    fun format(
        outgoingMessage: OutgoingMessage<*, *>,
        logLevel: Level,
        addValue: Boolean = false,
        addHeaders: Boolean = false
    ): String = buildString {
        append("topic: ${outgoingMessage.topic} | partition: ${outgoingMessage.partition}")
        append(" | timestamp: ${outgoingMessage.timestamp} | key: ${formatValue(outgoingMessage.key)}")
        append(" | correlation-metadata: ${outgoingMessage.correlationMetadata}")
        conditionallyAppendField(this, addValue, "value") { formatValue(outgoingMessage.value) }
        conditionallyAppendField(this, addHeaders, "headers") { formatHeaders(outgoingMessage.headers) }
    }

    /**
     * Formats a [DeliveryResult] into a string.
     *
     * @param deliveryResult The result of a message delivery attempt.
     * @param logLevel The logging level (e.g., INFO, DEBUG).
     * @return A formatted string representation of the delivery result.
     */
    fun format(
        deliveryResult: DeliveryResult,
        logLevel: Level
    ): String = buildString {
        val metadata = deliveryResult.recordMetadata
        append("Delivery result (")
        append("partition: ${metadata.partition()}")
        append(" | offset: ${metadata.offset()}")
        append(" | timestamp: ${metadata.timestamp()} | correlation-metadata: ${deliveryResult.correlationMetadata}")
        conditionallyAppendField(this, deliveryResult.exception != null, "exception") {
            deliveryResult.exception?.message ?: ""
        }
        append(" )")
    }

    /**
     * Default instance of [OutgoingMessageStringFormatter].
     */
    companion object {
        val Default = OutgoingMessageStringFormatter()
    }
}
