package com.trendyol.quafka.extensions.serialization.json.typeResolvers

import com.fasterxml.jackson.databind.JsonNode
import com.trendyol.quafka.consumer.IncomingMessage

open class MappingBasedTypeResolver(
    private val mapping: MutableMap<String, Class<*>>,
    private val typeNameExtractor: TypeNameExtractor
) : TypeResolver {
    override fun resolve(
        jsonNode: JsonNode,
        incomingMessage: IncomingMessage<*, *>
    ): Class<*>? {
        val typeName = typeNameExtractor.getTypeName(jsonNode, incomingMessage) ?: return null
        return mapping[typeName]
    }
}
