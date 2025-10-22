package com.trendyol.quafka.extensions.serialization

import com.trendyol.quafka.consumer.*
import com.trendyol.quafka.extensions.common.TopicPartitionProcessException

interface MessageSerde<TKey, TValue> {
    fun deserializeValue(incomingMessage: IncomingMessage<TKey, TValue>): DeserializationResult

    fun deserializeKey(incomingMessage: IncomingMessage<TKey, TValue>): DeserializationResult

    fun serializeKey(key: Any?): TKey

    fun serializeValue(value: Any?): TValue
}

sealed class DeserializationResult {
    data class Error(
        val topicPartitionOffset: TopicPartitionOffset,
        val message: String,
        val cause: Throwable? = null
    ) : DeserializationResult() {
        override fun toString(): String =
            "DeserializationError ($topicPartitionOffset | message: $message | cause: $cause )"

        fun toException(): TopicPartitionProcessException = TopicPartitionProcessException(
            topicPartitionOffset = topicPartitionOffset,
            message = this.toString(),
            cause = cause
        )
    }

    data object Null : DeserializationResult() {
        fun toError(
            topicPartitionOffset: TopicPartitionOffset,
            message: String
        ) = Error(topicPartitionOffset, message)
    }

    data class Deserialized(
        val value: Any
    ) : DeserializationResult()
}
