package com.trendyol.quafka.extensions.serialization.json.typeResolvers

import com.fasterxml.jackson.databind.JsonNode
import com.trendyol.quafka.common.QuafkaException
import com.trendyol.quafka.consumer.IncomingMessage
import io.kotest.assertions.throwables.shouldThrow
import io.kotest.core.spec.style.FunSpec
import io.kotest.matchers.shouldBe
import io.mockk.*

class AutoPackageNameBasedTypeResolverTests :
    FunSpec({

        test("should resolve class using type name extractor") {
            // Arrange
            val mockTypeNameExtractor = mockk<TypeNameExtractor>()
            val resolver = AutoPackageNameBasedTypeResolver(mockTypeNameExtractor)
            val mockJsonNode = mockk<JsonNode>()
            val mockIncomingMessage = mockk<IncomingMessage<*, *>>()
            val typeName = "java.lang.String"

            every { mockTypeNameExtractor.getTypeName(mockJsonNode, mockIncomingMessage) } returns typeName

            // Act
            val result = resolver.resolve(mockJsonNode, mockIncomingMessage)

            // Assert
            result shouldBe String::class.java
        }

        test("should return null when type name is null") {
            // Arrange
            val mockTypeNameExtractor = mockk<TypeNameExtractor>()
            val resolver = AutoPackageNameBasedTypeResolver(mockTypeNameExtractor)
            val mockJsonNode = mockk<JsonNode>()
            val mockIncomingMessage = mockk<IncomingMessage<*, *>>()

            every { mockTypeNameExtractor.getTypeName(mockJsonNode, mockIncomingMessage) } returns null

            // Act
            val result = resolver.resolve(mockJsonNode, mockIncomingMessage)

            // Assert
            result shouldBe null
        }

        test("should return null when class is not found") {
            // Arrange
            val mockTypeNameExtractor = mockk<TypeNameExtractor>()
            val resolver = AutoPackageNameBasedTypeResolver(mockTypeNameExtractor)
            val mockJsonNode = mockk<JsonNode>()
            val mockIncomingMessage = mockk<IncomingMessage<*, *>>()
            val invalidTypeName = "com.invalid.NonExistentClass"

            every { mockTypeNameExtractor.getTypeName(mockJsonNode, mockIncomingMessage) } returns invalidTypeName

            // Act
            val result = resolver.resolve(mockJsonNode, mockIncomingMessage)

            // Assert
            result shouldBe null
        }

        test("should throw QuafkaException for other errors") {
            // Arrange
            val mockTypeNameExtractor = mockk<TypeNameExtractor>()
            val resolver = AutoPackageNameBasedTypeResolver(mockTypeNameExtractor)
            val mockJsonNode = mockk<JsonNode>()
            val mockIncomingMessage = mockk<IncomingMessage<*, *>>()
            val typeName = "java.lang.String"
            val mockError = RuntimeException("Test error")

            mockkObject(AutoPackageNameBasedTypeResolver.Companion)
            every { AutoPackageNameBasedTypeResolver.findByClassName(typeName) } throws mockError
            every { mockTypeNameExtractor.getTypeName(mockJsonNode, mockIncomingMessage) } returns typeName

            // Act & Assert
            val exception = shouldThrow<QuafkaException> {
                resolver.resolve(mockJsonNode, mockIncomingMessage)
            }
            exception.message shouldBe "Type not found in assembly or mapping, type: $typeName"
            exception.cause shouldBe mockError
        }
    })
