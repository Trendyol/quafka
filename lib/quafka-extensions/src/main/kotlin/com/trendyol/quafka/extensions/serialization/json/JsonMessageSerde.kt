package com.trendyol.quafka.extensions.serialization.json

import com.fasterxml.jackson.databind.*
import com.trendyol.quafka.consumer.IncomingMessage
import com.trendyol.quafka.extensions.serialization.*
import com.trendyol.quafka.extensions.serialization.json.typeResolvers.TypeResolver

@Suppress("UNCHECKED_CAST")
abstract class JsonMessageSerde<TKey, TValue>(
    protected val objectMapper: ObjectMapper,
    protected val typeResolver: TypeResolver
) : MessageSerde<TKey, TValue> {
    override fun deserializeValue(
        incomingMessage: IncomingMessage<TKey, TValue>
    ): DeserializationResult = parseValue(incomingMessage).fold(
        onSuccess = { jsonResult ->
            resolveTypeAndBind(jsonResult, incomingMessage)
        },
        onFailure = { ex ->
            DeserializationResult.Error(
                topicPartition = incomingMessage.topicPartition,
                offset = incomingMessage.offset,
                message = "error occurred deserializing value as json",
                cause = ex
            )
        }
    )

    override fun deserializeKey(incomingMessage: IncomingMessage<TKey, TValue>): DeserializationResult = parseKey(
        incomingMessage
    ).fold(
        onSuccess = { value ->
            when (value) {
                null -> DeserializationResult.Null
                else -> DeserializationResult.Deserialized(value)
            }
        },
        onFailure = { ex ->
            DeserializationResult.Error(
                topicPartition = incomingMessage.topicPartition,
                offset = incomingMessage.offset,
                message = "error occurred deserializing key",
                cause = ex
            )
        }
    )

    protected abstract fun parseKey(incomingMessage: IncomingMessage<TKey, TValue>): Result<Any?>

    protected abstract fun parseValue(incomingMessage: IncomingMessage<TKey, TValue>): Result<JsonNode?>

    protected open fun resolveTypeAndBind(jsonNode: JsonNode?, incomingMessage: IncomingMessage<TKey, TValue>): DeserializationResult {
        if (jsonNode == null) {
            return DeserializationResult.Null
        }

        val typeResult = resolveType(jsonNode, incomingMessage)
        if (typeResult.isFailure) {
            return DeserializationResult.Error(
                topicPartition = incomingMessage.topicPartition,
                offset = incomingMessage.offset,
                message = "An error occurred when resolving type",
                cause = typeResult.exceptionOrNull()
            )
        }

        val resolvedType = typeResult.getOrNull()
            ?: return DeserializationResult.Error(
                topicPartition = incomingMessage.topicPartition,
                offset = incomingMessage.offset,
                message = "Type not resolved!!"
            )

        val bindingResult = bindToObject(jsonNode, resolvedType)
        return bindingResult.fold(
            onSuccess = { DeserializationResult.Deserialized(it) },
            onFailure = { ex ->
                DeserializationResult.Error(
                    topicPartition = incomingMessage.topicPartition,
                    offset = incomingMessage.offset,
                    message = "error occurred binding message to object, body: $jsonNode, type: ${resolvedType.name}",
                    cause = ex
                )
            }
        )
    }

    protected open fun bindToObject(
        jsonNode: JsonNode?,
        type: Class<*>?
    ): Result<Any> = runCatching { objectMapper.convertValue(jsonNode, type) }

    protected open fun resolveType(
        jsonNode: JsonNode,
        incomingMessage: IncomingMessage<TKey, TValue>
    ): Result<Class<*>?> = runCatching { typeResolver.resolve(jsonNode, incomingMessage) }
}
