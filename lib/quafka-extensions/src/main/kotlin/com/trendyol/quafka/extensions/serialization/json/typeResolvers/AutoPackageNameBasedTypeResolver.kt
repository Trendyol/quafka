package com.trendyol.quafka.extensions.serialization.json.typeResolvers

import com.fasterxml.jackson.databind.JsonNode
import com.trendyol.quafka.common.QuafkaException
import com.trendyol.quafka.consumer.IncomingMessage

class AutoPackageNameBasedTypeResolver(
    private val typeNameExtractor: TypeNameExtractor
) : TypeResolver {
    override fun resolve(
        jsonNode: JsonNode,
        incomingMessage: IncomingMessage<*, *>
    ): Class<*>? {
        val type = typeNameExtractor.getTypeName(jsonNode, incomingMessage) ?: return null
        return try {
            findByClassName(type)
        } catch (e: ClassNotFoundException) {
            // ignore
            null
        } catch (e: Throwable) {
            throw QuafkaException("Type not found in assembly or mapping, type: $type", e)
        }
    }

    companion object {
        @JvmStatic
        fun findByClassName(className: String): Class<*> = Class.forName(className)
    }
}
