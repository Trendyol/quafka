package com.trendyol.quafka.extensions.serialization.json.typeResolvers

import com.fasterxml.jackson.databind.JsonNode
import com.trendyol.quafka.consumer.IncomingMessage
import io.kotest.core.spec.style.FunSpec
import io.kotest.matchers.shouldBe
import io.mockk.*

class MappingBasedTypeResolverTests :
    FunSpec({

        class TypedClass

        test("should resolve class from mapping using extracted type name") {
            // Arrange
            val mockTypeNameExtractor = mockk<TypeNameExtractor>()
            val mockJsonNode = mockk<JsonNode>()
            val mockIncomingMessage = mockk<IncomingMessage<*, *>>()
            val typeName = "java.lang.String"
            val mapping = mutableMapOf<String, Class<*>>(
                typeName to TypedClass::class.java
            )
            val resolver = MappingBasedTypeResolver(mapping, mockTypeNameExtractor)

            every { mockTypeNameExtractor.getTypeName(mockJsonNode, mockIncomingMessage) } returns typeName

            // Act
            val result = resolver.resolve(mockJsonNode, mockIncomingMessage)

            // Assert
            result shouldBe TypedClass::class.java
        }

        test("should return null when type name is not found in mapping") {
            // Arrange
            val mockTypeNameExtractor = mockk<TypeNameExtractor>()
            val mockJsonNode = mockk<JsonNode>()
            val mockIncomingMessage = mockk<IncomingMessage<*, *>>()
            val typeName = "java.lang.NonExistent"
            val mapping = mutableMapOf<String, Class<*>>()
            val resolver = MappingBasedTypeResolver(mapping, mockTypeNameExtractor)

            every { mockTypeNameExtractor.getTypeName(mockJsonNode, mockIncomingMessage) } returns typeName

            // Act
            val result = resolver.resolve(mockJsonNode, mockIncomingMessage)

            // Assert
            result shouldBe null
        }

        test("should return null when type name is null") {
            // Arrange
            val mockTypeNameExtractor = mockk<TypeNameExtractor>()
            val mockJsonNode = mockk<JsonNode>()
            val mockIncomingMessage = mockk<IncomingMessage<*, *>>()
            val mapping = mutableMapOf<String, Class<*>>()
            val resolver = MappingBasedTypeResolver(mapping, mockTypeNameExtractor)

            every { mockTypeNameExtractor.getTypeName(mockJsonNode, mockIncomingMessage) } returns null

            // Act
            val result = resolver.resolve(mockJsonNode, mockIncomingMessage)

            // Assert
            result shouldBe null
        }
    })
