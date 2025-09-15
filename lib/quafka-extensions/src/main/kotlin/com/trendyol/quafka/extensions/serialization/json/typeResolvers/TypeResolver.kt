package com.trendyol.quafka.extensions.serialization.json.typeResolvers

import com.fasterxml.jackson.databind.JsonNode
import com.trendyol.quafka.consumer.IncomingMessage

interface TypeResolver {
    fun resolve(
        jsonNode: JsonNode,
        incomingMessage: IncomingMessage<*, *>
    ): Class<*>?
}
