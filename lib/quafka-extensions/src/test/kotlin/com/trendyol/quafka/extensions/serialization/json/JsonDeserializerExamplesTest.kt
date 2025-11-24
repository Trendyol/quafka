package com.trendyol.quafka.extensions.serialization.json

import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import com.trendyol.quafka.IncomingMessageBuilder
import com.trendyol.quafka.common.HeaderParsers.asString
import com.trendyol.quafka.common.get
import com.trendyol.quafka.extensions.serialization.DeserializationResult
import io.kotest.core.spec.style.FunSpec
import io.kotest.matchers.shouldBe
import io.kotest.matchers.types.shouldBeInstanceOf
import org.apache.kafka.common.header.internals.RecordHeaders

private data class DeserOrder(val orderId: String, val amount: Double)
private data class DeserUser(val userId: String, val name: String)

class JsonDeserializerExamplesTest :
    FunSpec({

        val objectMapper = ObjectMapper()
            .registerKotlinModule()
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)

        test("Example 1: Fixed type resolution - simplest case") {
            // When you have a topic with only one message type
            val deserializer = JsonDeserializer.string(objectMapper) { _, _ -> DeserOrder::class.java }

            val json = """{"orderId":"order-123","amount":99.99}"""
            val incomingMessage = IncomingMessageBuilder<String?, String?>(
                topic = "test-topic",
                partition = 0,
                offset = 0L,
                key = null,
                value = json
            ).build()

            val result = deserializer.deserialize<DeserOrder>(incomingMessage)
            result.shouldBeInstanceOf<DeserializationResult.Deserialized<DeserOrder>>()
            val order = result.value
            order.orderId shouldBe "order-123"
            order.amount shouldBe 99.99
        }

        test("Example 2: Header-based type resolution") {
            // Most common pattern - type info in message headers
            val deserializer = JsonDeserializer.string(objectMapper) { _, message ->
                when (message.headers.get("X-MessageType")?.asString()) {
                    "Order" -> DeserOrder::class.java
                    "User" -> DeserUser::class.java
                    else -> null
                }
            }

            val json = """{"orderId":"order-456","amount":199.99}"""
            val headers = RecordHeaders().add("X-MessageType", "Order".toByteArray())
            val incomingMessage = IncomingMessageBuilder<String?, String?>(
                topic = "test-topic",
                partition = 0,
                offset = 0L,
                key = null,
                value = json,
                headers = headers.toList().toMutableList()
            ).build()

            val result = deserializer.deserialize<DeserOrder>(incomingMessage)
            result.shouldBeInstanceOf<DeserializationResult.Deserialized<DeserOrder>>()
            val order = result.value
            order.orderId shouldBe "order-456"
            order.amount shouldBe 199.99
        }

        test("Example 3: Content-based type resolution using JsonNode") {
            // Inspect the JSON content to determine type
            val deserializer = JsonDeserializer.string(objectMapper) { jsonNode, _ ->
                when (jsonNode.get("@type")?.asText()) {
                    "Order" -> DeserOrder::class.java
                    "User" -> DeserUser::class.java
                    else -> null
                }
            }

            val json = """{"@type":"Order","orderId":"order-789","amount":299.99}"""
            val incomingMessage = IncomingMessageBuilder<String?, String?>(
                topic = "test-topic",
                partition = 0,
                offset = 0L,
                key = null,
                value = json
            ).build()

            val result = deserializer.deserialize<DeserOrder>(incomingMessage)
            result.shouldBeInstanceOf<DeserializationResult.Deserialized<DeserOrder>>()
            val order = result.value
            order.orderId shouldBe "order-789"
        }

        test("Example 4: Using TypeResolvers helper - from mapping") {
            val typeMapping = mapOf(
                "Order" to DeserOrder::class.java,
                "User" to DeserUser::class.java
            )

            val deserializer = JsonDeserializer.string(
                objectMapper,
                typeResolver = TypeResolvers.fromMapping(typeMapping, "X-MessageType")
            )

            val json = """{"userId":"user-789","name":"John Doe"}"""
            val headers = RecordHeaders().add("X-MessageType", "User".toByteArray())
            val incomingMessage = IncomingMessageBuilder<String?, String?>(
                topic = "test-topic",
                partition = 0,
                offset = 0L,
                key = null,
                value = json,
                headers = headers.toList().toMutableList()
            ).build()

            val result = deserializer.deserialize<DeserOrder>(incomingMessage)
            result.shouldBeInstanceOf<DeserializationResult.Deserialized<DeserUser>>()
            val user = result.value
            user.userId shouldBe "user-789"
            user.name shouldBe "John Doe"
        }

        test("Example 5: Using TypeResolvers helper - fixed type") {
            val deserializer = JsonDeserializer.string(
                objectMapper,
                typeResolver = TypeResolvers.fixed(DeserOrder::class.java)
            )

            val json = """{"orderId":"order-111","amount":49.99}"""
            val incomingMessage = IncomingMessageBuilder<String?, String?>(
                topic = "test-topic",
                partition = 0,
                offset = 0L,
                key = null,
                value = json
            ).build()

            val result = deserializer.deserialize<DeserOrder>(incomingMessage)
            result.shouldBeInstanceOf<DeserializationResult.Deserialized<DeserOrder>>()
            val order = result.value
            order.orderId shouldBe "order-111"
            order.amount shouldBe 49.99
        }

        test("Example 6: ByteArray-based messages") {
            val deserializer = JsonDeserializer.byteArray(objectMapper) { _, _ -> DeserOrder::class.java }

            val json = """{"orderId":"order-222","amount":299.99}""".toByteArray()
            val incomingMessage = IncomingMessageBuilder<ByteArray?, ByteArray?>(
                topic = "test-topic",
                partition = 0,
                offset = 0L,
                key = null,
                value = json
            ).build()

            val result = deserializer.deserialize<DeserOrder>(incomingMessage)
            result.shouldBeInstanceOf<DeserializationResult.Deserialized<DeserOrder>>()
            val order = result.value
            order.orderId shouldBe "order-222"
            order.amount shouldBe 299.99
        }

        test("Example 7: Chain multiple resolution strategies") {
            val deserializer = JsonDeserializer.string(
                objectMapper,
                typeResolver = TypeResolvers.chain(
                    // Try header first
                    { _, message ->
                        message.headers.get("X-MessageType")?.asString()?.let {
                            when (it) {
                                "Order" -> DeserOrder::class.java
                                else -> null
                            }
                        }
                    },
                    // Fallback to JSON field
                    { jsonNode, _ ->
                        jsonNode.get("@type")?.asText()?.let {
                            when (it) {
                                "Order" -> DeserOrder::class.java
                                else -> null
                            }
                        }
                    }
                )
            )

            val json = """{"@type":"Order","orderId":"order-333","amount":399.99}"""
            val incomingMessage = IncomingMessageBuilder<String?, String?>(
                topic = "test-topic",
                partition = 0,
                offset = 0L,
                key = null,
                value = json
            ).build()

            val result = deserializer.deserialize<DeserOrder>(incomingMessage)
            result.shouldBeInstanceOf<DeserializationResult.Deserialized<DeserOrder>>()
            val order = result.value
            order.orderId shouldBe "order-333"
        }

        test("Example 8: Error handling - invalid JSON") {
            val deserializer = JsonDeserializer.string(objectMapper) { _, _ -> DeserOrder::class.java }

            val invalidJson = """{"orderId":"order-123","amount":"""
            val incomingMessage = IncomingMessageBuilder<String?, String?>(
                topic = "test-topic",
                partition = 0,
                offset = 0L,
                key = null,
                value = invalidJson
            ).build()

            val result = deserializer.deserialize<DeserOrder>(incomingMessage)
            result.shouldBeInstanceOf<DeserializationResult.Error>()
        }

        test("Example 9: Error handling - type not resolved") {
            val deserializer = JsonDeserializer.string(objectMapper) { _, _ -> null }

            val json = """{"orderId":"order-123","amount":99.99}"""
            val incomingMessage = IncomingMessageBuilder<String?, String?>(
                topic = "test-topic",
                partition = 0,
                offset = 0L,
                key = null,
                value = json
            ).build()

            val result = deserializer.deserialize<DeserOrder>(incomingMessage)
            result.shouldBeInstanceOf<DeserializationResult.Error>()
            result.message shouldBe "Type not resolved!!"
        }
    })
