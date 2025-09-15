package com.trendyol.quafka.extensions.serialization.json

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import com.trendyol.quafka.consumer.IncomingMessage
import com.trendyol.quafka.extensions.serialization.json.typeResolvers.TypeResolver

open class StringJsonMessageSerde(
    objectMapper: ObjectMapper,
    typeResolver: TypeResolver
) : JsonMessageSerde<String?, String?>(objectMapper, typeResolver) {
    override fun serializeValue(value: Any?): String? {
        if (value == null) return null
        return objectMapper.writeValueAsString(value)
    }

    override fun serializeKey(key: Any?): String? {
        if (key == null) return null
        return key.toString()
    }

    override fun parseKey(incomingMessage: IncomingMessage<String?, String?>): Result<Any?> = runCatching {
        when (val value = incomingMessage.key) {
            null -> null
            else -> value
        }
    }

    override fun parseValue(incomingMessage: IncomingMessage<String?, String?>): Result<JsonNode?> = runCatching {
        val reader = objectMapper.reader()
        when (val value = incomingMessage.value) {
            null -> null
            else -> reader.readTree(value)
        }
    }
}
