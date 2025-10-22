package com.trendyol.quafka.extensions.serialization.middlewares

import com.trendyol.quafka.extensions.common.pipelines.MiddlewareFn
import com.trendyol.quafka.extensions.consumer.single.pipelines.*
import com.trendyol.quafka.extensions.serialization.*

class DeserializationMiddleware<TEnvelope, TKey, TValue>(
    val messageSerde: MessageSerde<TKey, TValue>,
    val throwExceptionIfValueIsNull: Boolean = true,
    val throwExceptionIfKeyIsNull: Boolean = true
) : SingleMessageBaseMiddleware<TEnvelope, TKey, TValue>() where TEnvelope : SingleMessageEnvelope<TKey, TValue> {
    companion object {
        @PublishedApi
        internal val DeserializedKeyAttributeKey = AttributeKey<Any>("DeserializedKey")

        @PublishedApi
        internal val DeserializedValueAttributeKey = AttributeKey<Any>("DeserializedValue")

        inline fun <reified T> SingleMessageEnvelope<*, *>.getDeserializedValue(): T? = this.attributes.getOrNull(
            DeserializedValueAttributeKey
        ) as T

        inline fun <reified T> SingleMessageEnvelope<*, *>.getDeserializedKey(): T? = this.attributes.getOrNull(
            DeserializedKeyAttributeKey
        ) as T
    }

    override suspend fun execute(
        envelope: TEnvelope,
        next: MiddlewareFn<TEnvelope>
    ) {
        deserializeKey(envelope)?.let { key ->
            envelope.attributes.put(DeserializedKeyAttributeKey, key)
        }
        deserializeValue(envelope)?.let { value -> envelope.attributes.put(DeserializedValueAttributeKey, value) }
        next(envelope)
    }

    private fun deserializeKey(envelope: TEnvelope): Any? = when (
        val deserializationResult = messageSerde
            .deserializeKey(
                envelope.message
            )
    ) {
        is DeserializationResult.Deserialized -> deserializationResult.value
        is DeserializationResult.Error -> throw deserializationResult.toException()
        is DeserializationResult.Null -> if (throwExceptionIfKeyIsNull) {
            throw deserializationResult
                .toError(envelope.message.topicPartitionOffset, "deserialized key cannot be null")
                .toException()
        } else {
            null
        }
    }

    private fun deserializeValue(envelope: TEnvelope): Any? = when (
        val deserializationResult = messageSerde
            .deserializeValue(
                envelope.message
            )
    ) {
        is DeserializationResult.Deserialized -> deserializationResult.value
        is DeserializationResult.Error -> throw deserializationResult.toException()
        is DeserializationResult.Null -> if (throwExceptionIfValueIsNull) {
            throw deserializationResult
                .toError(envelope.message.topicPartitionOffset, "deserialized value cannot be null")
                .toException()
        } else {
            null
        }
    }
}
