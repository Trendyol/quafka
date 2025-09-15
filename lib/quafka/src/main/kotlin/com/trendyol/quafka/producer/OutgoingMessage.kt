package com.trendyol.quafka.producer

import com.trendyol.quafka.common.QuafkaHeader
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.header.Header
import org.slf4j.event.Level
import java.util.UUID

/**
 * Represents an outgoing Kafka message, encapsulating all necessary details for production.
 *
 * This class provides functionality for constructing a Kafka message, generating correlation metadata,
 * formatting the message for logging, and converting it to a [ProducerRecord].
 *
 * @param TKey The type of the message key.
 * @param TValue The type of the message value.
 * @property topic The name of the Kafka topic to which the message will be sent.
 * @property partition The specific partition of the topic, or `null` to allow Kafka to assign a partition automatically.
 * @property key The key of the message, used for partitioning and message grouping.
 * @property value The value (payload) of the message.
 * @property headers Additional metadata for the message, represented as a collection of [Header].
 * @property correlationMetadata A unique identifier for tracking the message lifecycle.
 * @property timestamp The timestamp of the message, or `null` to use the Kafka producer's default timestamp.
 */
class OutgoingMessage<TKey, TValue> private constructor(
    val topic: String,
    val partition: Int?,
    val key: TKey?,
    val value: TValue?,
    val headers: Iterable<Header>,
    val correlationMetadata: String,
    val timestamp: Long?
) {
    fun withKey(newKey: TKey?): OutgoingMessage<TKey, TValue> =
        OutgoingMessage(
            topic = topic,
            partition = partition,
            key = newKey,
            value = value,
            headers = headers,
            correlationMetadata = correlationMetadata,
            timestamp = timestamp
        )

    fun withValue(newValue: TValue?): OutgoingMessage<TKey, TValue> =
        OutgoingMessage(
            topic = topic,
            partition = partition,
            key = key,
            value = newValue,
            headers = headers,
            correlationMetadata = correlationMetadata,
            timestamp = timestamp
        )

    fun withHeaders(newHeaders: Iterable<Header>): OutgoingMessage<TKey, TValue> =
        OutgoingMessage(
            topic = topic,
            partition = partition,
            key = key,
            value = value,
            headers = newHeaders.map {
                it as? QuafkaHeader ?: QuafkaHeader(it.key(), it.value())
            },
            correlationMetadata = correlationMetadata,
            timestamp = timestamp
        )

    fun withPartition(newPartition: Int?): OutgoingMessage<TKey, TValue> =
        OutgoingMessage(
            topic = topic,
            partition = newPartition,
            key = key,
            value = value,
            headers = headers,
            correlationMetadata = correlationMetadata,
            timestamp = timestamp
        )

    fun withTimestamp(newTimestamp: Long?): OutgoingMessage<TKey, TValue> =
        OutgoingMessage(
            topic = topic,
            partition = partition,
            key = key,
            value = value,
            headers = headers,
            correlationMetadata = correlationMetadata,
            timestamp = newTimestamp
        )

    fun withCorrelationMetadata(newMetadata: String): OutgoingMessage<TKey, TValue> =
        OutgoingMessage(
            topic = topic,
            partition = partition,
            key = key,
            value = value,
            headers = headers,
            correlationMetadata = newMetadata,
            timestamp = timestamp
        )

    fun copy(
        topic: String = this.topic,
        partition: Int? = this.partition,
        key: TKey? = this.key,
        value: TValue? = this.value,
        headers: Iterable<Header> = this.headers,
        correlationMetadata: String = this.correlationMetadata,
        timestamp: Long? = this.timestamp
    ): OutgoingMessage<TKey, TValue> = OutgoingMessage<TKey, TValue>(
        topic = topic,
        partition = partition,
        key = key,
        value = value,
        headers = headers.map { it as? QuafkaHeader ?: QuafkaHeader(it.key(), it.value()) },
        correlationMetadata = correlationMetadata,
        timestamp = timestamp
    )

    /**
     * The default formatter for the outgoing message, used for logging and debugging purposes.
     */
    private val outgoingMessageStringFormatter: OutgoingMessageStringFormatter = OutgoingMessageStringFormatter.Default

    /**
     * Companion object containing utility methods for creating and managing outgoing messages.
     */
    companion object {
        /**
         * Generates a new unique correlation metadata identifier.
         *
         * @return A unique string identifier.
         */
        fun newCorrelationMetadata(): String = UUID.randomUUID().toString()

        /**
         * Factory method for creating a new [OutgoingMessage] instance with default values.
         *
         * @param TKey The type of the message key.
         * @param TValue The type of the message value.
         * @param topic The name of the Kafka topic.
         * @param partition The specific partition of the topic, or `null` for automatic assignment.
         * @param key The key of the message.
         * @param value The value (payload) of the message.
         * @param headers Additional metadata for the message, defaults to an empty list.
         * @param correlationMetadata A unique identifier for tracking the message, defaults to a new UUID.
         * @param timestamp The timestamp of the message, defaults to `null` for Kafka's default timestamp.
         * @return A new instance of [OutgoingMessage].
         */
        fun <TKey, TValue> create(
            topic: String,
            partition: Int? = null,
            key: TKey?,
            value: TValue?,
            headers: Iterable<Header> = emptyList(),
            correlationMetadata: String = newCorrelationMetadata(),
            timestamp: Long? = null
        ): OutgoingMessage<TKey, TValue> = OutgoingMessage(
            topic = topic,
            partition = partition,
            timestamp = timestamp,
            key = key,
            value = value,
            headers = headers.map {
                it as? QuafkaHeader ?: QuafkaHeader(it.key(), it.value())
            },
            correlationMetadata = correlationMetadata
        )
    }

    /**
     * Retrieves the appropriate message formatter.
     *
     * @param messageFormatter A custom formatter, or `null` to use the default formatter.
     * @return The selected [OutgoingMessageStringFormatter].
     */
    private fun getMessageFormatter(
        messageFormatter: OutgoingMessageStringFormatter? = null
    ): OutgoingMessageStringFormatter {
        if (messageFormatter == null) {
            return this.outgoingMessageStringFormatter
        }

        if (this.outgoingMessageStringFormatter != OutgoingMessageStringFormatter.Default) {
            return this.outgoingMessageStringFormatter
        }

        return messageFormatter
    }

    /**
     * Formats the message for logging.
     *
     * @param logLevel The log level for formatting (e.g., INFO, DEBUG).
     * @param addValue If `true`, includes the message value in the log output.
     * @param addHeaders If `true`, includes the message headers in the log output.
     * @param outgoingMessageStringFormatter A custom formatter, or `null` to use the default formatter.
     * @return A formatted string representation of the message.
     */
    fun toString(
        logLevel: Level = Level.INFO,
        addValue: Boolean = false,
        addHeaders: Boolean = false,
        outgoingMessageStringFormatter: OutgoingMessageStringFormatter? = null
    ): String = getMessageFormatter(outgoingMessageStringFormatter).format(
        outgoingMessage = this,
        logLevel = logLevel,
        addValue = addValue,
        addHeaders = addHeaders
    )

    /**
     * Overrides the default `toString()` method to provide a formatted representation of the message.
     *
     * @return A formatted string representation of the message.
     */
    override fun toString(): String = toString(
        logLevel = Level.INFO,
        addValue = false,
        addHeaders = false,
        outgoingMessageStringFormatter = this.outgoingMessageStringFormatter
    )

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other !is OutgoingMessage<*, *>) return false

        return topic == other.topic &&
            partition == other.partition &&
            key == other.key &&
            value == other.value &&
            headers == other.headers &&
            correlationMetadata == other.correlationMetadata &&
            timestamp == other.timestamp
    }

    override fun hashCode(): Int {
        var result = topic.hashCode()
        result = 31 * result + (partition ?: 0)
        result = 31 * result + (key?.hashCode() ?: 0)
        result = 31 * result + (value?.hashCode() ?: 0)
        result = 31 * result + headers.hashCode()
        result = 31 * result + correlationMetadata.hashCode()
        result = 31 * result + (timestamp?.hashCode() ?: 0)
        return result
    }

    /**
     * Converts the [OutgoingMessage] to a Kafka [ProducerRecord].
     *
     * @return A [ProducerRecord] representing the outgoing message.
     */
    internal fun toProducerRecord(): ProducerRecord<TKey, TValue> = ProducerRecord<TKey, TValue>(
        topic,
        partition,
        timestamp,
        key,
        value,
        headers.toList()
    )
}
