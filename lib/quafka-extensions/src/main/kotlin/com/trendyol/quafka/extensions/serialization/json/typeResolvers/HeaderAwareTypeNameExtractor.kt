package com.trendyol.quafka.extensions.serialization.json.typeResolvers

import com.fasterxml.jackson.databind.JsonNode
import com.trendyol.quafka.common.*
import com.trendyol.quafka.common.HeaderParsers.asString
import com.trendyol.quafka.consumer.IncomingMessage
import com.trendyol.quafka.extensions.producer.OutgoingMessageBuilder
import com.trendyol.quafka.extensions.serialization.json.*
import com.trendyol.quafka.extensions.serialization.json.typeResolvers.HeaderAwareTypeNameExtractor.Companion.DEFAULT_MESSAGE_TYPE_HEADER_KEY

class HeaderAwareTypeNameExtractor(
    val headerKeys: Collection<String> = DEFAULT_MESSAGE_TYPE_HEADER_KEYS
) : TypeNameExtractor {
    override fun getTypeName(
        jsonNode: JsonNode,
        incomingMessage: IncomingMessage<*, *>
    ): String? {
        for (headerKey in headerKeys) {
            val header = incomingMessage.headers.get(headerKey)?.asString()
            if (header != null) {
                return header
            }
        }
        return null
    }

    companion object {
        const val DEFAULT_MESSAGE_TYPE_HEADER_KEY = "X-MessageType"
        val DEFAULT_MESSAGE_TYPE_HEADER_KEYS = listOf(DEFAULT_MESSAGE_TYPE_HEADER_KEY, "X-EventType")
    }
}

fun <TKey, TValue> OutgoingMessageBuilder<TKey, TValue>.newMessageWithTypeInfo(
    topic: String,
    key: Any?,
    value: Any,
    type: String = value.javaClass.simpleName
) = this.new(topic, key, value).withHeader(header(DEFAULT_MESSAGE_TYPE_HEADER_KEY, type))
