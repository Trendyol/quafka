package com.trendyol.quafka.extensions.serialization.json

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import com.trendyol.quafka.common.HeaderParsers
import com.trendyol.quafka.consumer.IncomingMessage
import com.trendyol.quafka.extensions.serialization.json.typeResolvers.TypeResolver
import java.io.ByteArrayInputStream

open class ByteArrayJsonMessageSerde(
    objectMapper: ObjectMapper,
    typeResolver: TypeResolver
) : JsonMessageSerde<ByteArray?, ByteArray?>(objectMapper, typeResolver) {
    override fun serializeValue(value: Any?): ByteArray? {
        if (value == null) return null
        return objectMapper.writeValueAsBytes(value)
    }

    override fun serializeKey(key: Any?): ByteArray? {
        if (key == null) return null
        return key.toString().toByteArray(HeaderParsers.defaultCharset)
    }

    override fun parseKey(incomingMessage: IncomingMessage<ByteArray?, ByteArray?>): Result<Any?> = runCatching {
        when (val value = incomingMessage.key) {
            null -> null
            else -> value.toString(HeaderParsers.defaultCharset)
        }
    }

    override fun parseValue(incomingMessage: IncomingMessage<ByteArray?, ByteArray?>): Result<JsonNode?> = runCatching {
        val reader = objectMapper.reader()
        when (val value = incomingMessage.value) {
            null -> null
            else -> reader.readTree(ByteArrayInputStream(value))
        }
    }
}
