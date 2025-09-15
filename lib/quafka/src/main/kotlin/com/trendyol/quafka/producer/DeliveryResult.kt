package com.trendyol.quafka.producer

import org.apache.kafka.clients.producer.RecordMetadata
import org.slf4j.event.Level

/**
 * Represents the result of a message delivery to Kafka.
 *
 * This class encapsulates the metadata of the delivery, any exceptions that occurred,
 * correlation metadata, and a formatter for generating string representations of the result.
 *
 * @property recordMetadata Metadata about the delivered record, such as topic, partition, and offset.
 * @property exception The exception that occurred during delivery, if any. `null` if the delivery was successful.
 * @property correlationMetadata An optional string used to correlate the message, typically for tracking purposes.
 * @property outgoingMessageStringFormatter A formatter for creating string representations of the delivery result.
 */
data class DeliveryResult(
    val recordMetadata: RecordMetadata,
    val exception: Throwable?,
    val correlationMetadata: String?,
    val outgoingMessageStringFormatter: OutgoingMessageStringFormatter
) {
    /**
     * Checks if the message delivery failed.
     *
     * @return `true` if an exception occurred during delivery, `false` otherwise.
     */
    fun failed(): Boolean = exception != null

    /**
     * Generates a string representation of the delivery result with the specified log level.
     *
     * @param logLevel The logging level (e.g., INFO, DEBUG).
     * @return A formatted string representation of the delivery result.
     */
    fun toString(
        logLevel: Level = Level.INFO
    ): String = outgoingMessageStringFormatter.format(
        deliveryResult = this,
        logLevel = logLevel
    )

    /**
     * Generates a default string representation of the delivery result with INFO log level.
     *
     * @return A formatted string representation of the delivery result.
     */
    override fun toString(): String = toString(logLevel = Level.INFO)
}
