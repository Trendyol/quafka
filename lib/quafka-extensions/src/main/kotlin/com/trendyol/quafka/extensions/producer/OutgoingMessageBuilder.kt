package com.trendyol.quafka.extensions.producer

import com.trendyol.quafka.common.addHeader
import com.trendyol.quafka.extensions.serialization.MessageSerde
import com.trendyol.quafka.producer.OutgoingMessage
import org.apache.kafka.common.header.Header

open class OutgoingMessageBuilder<TKey, TValue>(
    private val messageSerde: MessageSerde<TKey, TValue>
) {
    open fun new(
        topic: String,
        key: Any?,
        value: Any?
    ): FluentOutgoingMessageBuilder<TKey, TValue> = FluentOutgoingMessageBuilder(
        topic,
        serializeKey(key),
        serializeValue(value)
    )

    open fun serializeKey(key: Any?): TKey = messageSerde.serializeKey(key)

    open fun serializeValue(value: Any?): TValue = messageSerde.serializeValue(value)

    inner class FluentOutgoingMessageBuilder<TKey, TValue>(
        val topic: String,
        val key: TKey?,
        val value: TValue?
    ) {
        var timestamp: Long? = null
            private set

        var correlationMetadata: String = OutgoingMessage.newCorrelationMetadata()
            private set

        var partition: Int? = null
            private set

        var headers: MutableList<Header> = mutableListOf()
            private set

        fun withPartition(partition: Int?): FluentOutgoingMessageBuilder<TKey, TValue> {
            this.partition = partition
            return this
        }

        fun withHeaders(headers: Collection<Header>): FluentOutgoingMessageBuilder<TKey, TValue> {
            this.headers.clear()
            this.headers.addAll(headers)
            return this
        }

        fun withCorrelationMetadata(correlationMetadata: String): FluentOutgoingMessageBuilder<TKey, TValue> {
            this.correlationMetadata = correlationMetadata
            return this
        }

        fun withHeader(
            header: Header
        ): FluentOutgoingMessageBuilder<TKey, TValue> {
            this.headers.addHeader(header, override = true)
            return this
        }

        fun withTimestamp(timestamp: Long?): FluentOutgoingMessageBuilder<TKey, TValue> {
            this.timestamp = timestamp
            return this
        }

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
