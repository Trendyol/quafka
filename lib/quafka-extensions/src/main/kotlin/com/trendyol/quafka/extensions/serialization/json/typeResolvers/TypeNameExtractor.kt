package com.trendyol.quafka.extensions.serialization.json.typeResolvers

import com.fasterxml.jackson.databind.JsonNode
import com.trendyol.quafka.consumer.IncomingMessage

interface TypeNameExtractor {
    fun getTypeName(
        jsonNode: JsonNode,
        incomingMessage: IncomingMessage<*, *>
    ): String?
}
