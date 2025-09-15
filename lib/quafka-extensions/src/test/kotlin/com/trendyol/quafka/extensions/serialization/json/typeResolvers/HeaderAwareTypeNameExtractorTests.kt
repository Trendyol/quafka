package com.trendyol.quafka.extensions.serialization.json.typeResolvers

import com.fasterxml.jackson.databind.JsonNode
import com.trendyol.quafka.common.*
import com.trendyol.quafka.common.HeaderParsers.asString
import com.trendyol.quafka.consumer.IncomingMessage
import com.trendyol.quafka.extensions.producer.OutgoingMessageBuilder
import com.trendyol.quafka.extensions.serialization.*
import com.trendyol.quafka.extensions.serialization.json.typeResolvers.HeaderAwareTypeNameExtractor.Companion.DEFAULT_MESSAGE_TYPE_HEADER_KEY
import io.kotest.core.spec.style.FunSpec
import io.kotest.matchers.shouldBe
import io.mockk.every
import io.mockk.mockk

class HeaderAwareTypeNameExtractorTests :
    FunSpec({

        test("should put message type to header") {
            data class DummyRequest(
                val id: String
            )

            val serde = mockk<MessageSerde<String, String>>(relaxed = true)

            val outgoingMessageBuilder = OutgoingMessageBuilder(serde)
            val message = outgoingMessageBuilder
                .newMessageWithTypeInfo(
                    "topic",
                    "key",
                    DummyRequest("123")
                ).build()
            message.headers.get(DEFAULT_MESSAGE_TYPE_HEADER_KEY)!!.asString() shouldBe DummyRequest::class.simpleName
        }

        test("should return type name from headers when present") {
            // Arrange
            val mockIncomingMessage = mockk<IncomingMessage<*, *>>()
            val mockJsonNode = mockk<JsonNode>()
            val headers = listOf(header("X-MessageType", "SomeType"))
            val extractor = HeaderAwareTypeNameExtractor()

            every { mockIncomingMessage.headers } returns headers

            // Act
            val result = extractor.getTypeName(mockJsonNode, mockIncomingMessage)

            // Assert
            result shouldBe "SomeType"
        }

        test("should return null when no matching header key is present") {
            // Arrange
            val mockIncomingMessage = mockk<IncomingMessage<*, *>>()
            val mockJsonNode = mockk<JsonNode>()
            val extractor = HeaderAwareTypeNameExtractor()

            every { mockIncomingMessage.headers } returns emptyList()

            // Act
            val result = extractor.getTypeName(mockJsonNode, mockIncomingMessage)

            // Assert
            result shouldBe null
        }
    })
