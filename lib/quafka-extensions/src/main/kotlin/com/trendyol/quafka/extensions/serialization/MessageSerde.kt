package com.trendyol.quafka.extensions.serialization

import com.trendyol.quafka.consumer.IncomingMessage
import com.trendyol.quafka.extensions.common.TopicPartitionProcessException
import org.apache.kafka.common.TopicPartition

interface MessageSerde<TKey, TValue> {
    fun deserializeValue(incomingMessage: IncomingMessage<TKey, TValue>): DeserializationResult

    fun deserializeKey(incomingMessage: IncomingMessage<TKey, TValue>): DeserializationResult

    fun serializeKey(key: Any?): TKey

    fun serializeValue(value: Any?): TValue
}

sealed class DeserializationResult {
    data class Error(
        val topicPartition: TopicPartition,
        val offset: Long,
        val message: String,
        val cause: Throwable? = null
    ) : DeserializationResult() {
        override fun toString(): String =
            "DeserializationError ( topicPartition: $topicPartition | offset: $offset | message: $message | cause: $cause )"

        fun toException(): TopicPartitionProcessException = TopicPartitionProcessException(
            topicPartition = topicPartition,
            offset = offset,
            message = this.toString(),
            cause = cause
        )
    }

    data object Null : DeserializationResult() {
        fun toError(
            topicPartition: TopicPartition,
            offset: Long,
            message: String
        ) = Error(topicPartition, offset, message)
    }

    data class Deserialized(
        val value: Any
    ) : DeserializationResult()
}
