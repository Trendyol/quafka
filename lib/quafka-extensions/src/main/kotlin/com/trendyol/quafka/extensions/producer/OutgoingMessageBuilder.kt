package com.trendyol.quafka.extensions.producer

import com.trendyol.quafka.common.addHeader
import com.trendyol.quafka.extensions.serialization.*
import com.trendyol.quafka.producer.OutgoingMessage
import org.apache.kafka.common.header.Header

/**
 * Builder for constructing [OutgoingMessage] instances with automatic serialization.
 *
 * This class provides a fluent API for creating Kafka producer messages with type-safe
 * serialization. It automatically handles serialization of keys and values using either
 * a [MessageSerializer] implementation.
 *
 * The builder supports:
 * - Automatic key and value serialization
 * - Custom partitioning
 * - Header management
 * - Correlation metadata
 * - Custom timestamps
 *
 * @param TKey The serialized type of the message key (e.g., String, ByteArray).
 * @param TValue The serialized type of the message value (e.g., String, ByteArray).
 * @property serializer The serialization implementation to use for keys and values.
 *
 * @see MessageSerializer
 * @see OutgoingMessage
 *
 * @sample
 * ```kotlin
 * // Using MessageSerializer (for producers only)
 * val messageBuilder = OutgoingMessageBuilder.create(JsonSerializer.byteArray(objectMapper))
 *
 * val message = messageBuilder.new(
 *     topic = "orders",
 *     key = orderId,
 *     value = OrderCreated(orderId, amount)
 * )
 * .withHeader(header("trace-id", traceId))
 * .withPartition(0)
 * .build()
 *
 * producer.send(message)
 * ```
 */
open class OutgoingMessageBuilder<TKey, TValue>(private val serializer: MessageSerializer<TValue>) {

    companion object {
        inline fun <TKey, reified TValue> create(
            serializer: MessageSerializer<TValue>
        ): OutgoingMessageBuilder<TKey, TValue> = OutgoingMessageBuilder(serializer)
    }

    /**
     * Creates a new message builder with the specified topic, key, and value.
     *
     * The key and value are automatically serialized using the configured [MessageSerializer].
     *
     * @param topic The Kafka topic to send the message to.
     * @param key The message key (will be serialized).
     * @param value The message value (will be serialized).
     * @return A [FluentOutgoingMessageBuilder] for further configuration.
     */
    open fun new(
        topic: String,
        key: TKey,
        value: Any?
    ): FluentOutgoingMessageBuilder<TKey, TValue> = FluentOutgoingMessageBuilder(
        topic,
        key,
        serializeValue(value)
    )

    /**
     * Serializes a value object using the configured serializer.
     *
     * @param value The value object to serialize.
     * @return The serialized value.
     */
    open fun serializeValue(value: Any?): TValue = serializer.serialize(value)

    /**
     * Fluent builder for configuring an outgoing message.
     *
     * This inner class provides a chainable API for setting message properties
     * such as partition, headers, timestamp, and correlation metadata.
     *
     * @param TKey The serialized type of the message key.
     * @param TValue The serialized type of the message value.
     * @property topic The Kafka topic for the message.
     * @property key The serialized message key.
     * @property value The serialized message value.
     */
    inner class FluentOutgoingMessageBuilder<TKey, TValue>(val topic: String, val key: TKey?, val value: TValue?) {
        /**
         * The timestamp to use for the message.
         * If null, Kafka will use the current time when the message is sent.
         */
        var timestamp: Long? = null
            private set

        /**
         * A unique identifier for tracking the message through its lifecycle.
         * Automatically generated if not explicitly set.
         */
        var correlationMetadata: String = OutgoingMessage.newCorrelationMetadata()
            private set

        /**
         * The specific partition to send the message to.
         * If null, Kafka will assign a partition automatically based on the key.
         */
        var partition: Int? = null
            private set

        /**
         * The collection of headers to attach to the message.
         * Headers are key-value pairs of metadata.
         */
        var headers: MutableList<Header> = mutableListOf()
            private set

        /**
         * Sets the partition for the message.
         *
         * @param partition The partition number, or null for automatic partitioning.
         * @return This builder instance for method chaining.
         */
        fun withPartition(partition: Int?): FluentOutgoingMessageBuilder<TKey, TValue> {
            this.partition = partition
            return this
        }

        /**
         * Replaces all headers with the provided collection.
         *
         * @param headers The new collection of headers.
         * @return This builder instance for method chaining.
         */
        fun withHeaders(headers: Collection<Header>): FluentOutgoingMessageBuilder<TKey, TValue> {
            this.headers.clear()
            this.headers.addAll(headers)
            return this
        }

        /**
         * Sets the correlation metadata for the message.
         *
         * @param correlationMetadata A unique identifier for tracking the message.
         * @return This builder instance for method chaining.
         */
        fun withCorrelationMetadata(correlationMetadata: String): FluentOutgoingMessageBuilder<TKey, TValue> {
            this.correlationMetadata = correlationMetadata
            return this
        }

        /**
         * Adds or replaces a single header.
         *
         * If a header with the same key already exists, it will be replaced.
         *
         * @param header The header to add.
         * @return This builder instance for method chaining.
         */
        fun withHeader(
            header: Header
        ): FluentOutgoingMessageBuilder<TKey, TValue> {
            this.headers.addHeader(header, override = true)
            return this
        }

        /**
         * Sets the timestamp for the message.
         *
         * @param timestamp The timestamp in milliseconds since epoch, or null to use the current time.
         * @return This builder instance for method chaining.
         */
        fun withTimestamp(timestamp: Long?): FluentOutgoingMessageBuilder<TKey, TValue> {
            this.timestamp = timestamp
            return this
        }

        /**
         * Builds the final [OutgoingMessage] instance.
         *
         * @return A new [OutgoingMessage] ready to be sent to Kafka.
         */
        fun build(): OutgoingMessage<TKey, TValue> = OutgoingMessage.create(
            topic = topic,
            partition = partition,
            timestamp = timestamp,
            key = key,
            value = value,
            headers = this.headers,
            correlationMetadata = this.correlationMetadata
        )
    }
}
