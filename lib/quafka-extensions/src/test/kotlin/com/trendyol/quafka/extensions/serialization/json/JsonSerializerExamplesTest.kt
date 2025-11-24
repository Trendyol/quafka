package com.trendyol.quafka.extensions.serialization.json

import com.fasterxml.jackson.databind.ObjectMapper
import io.kotest.core.spec.style.FunSpec
import io.kotest.matchers.shouldBe
import io.kotest.matchers.types.shouldBeInstanceOf

private data class SerOrder(val orderId: String, val amount: Double)
private data class SerUser(val userId: String, val name: String)

class JsonSerializerExamplesTest :
    FunSpec({

        val objectMapper = ObjectMapper()

        test("Example 1: String-based serialization") {
            val serializer = JsonSerializer.string(objectMapper)

            val serialized = serializer.serialize(SerOrder("order-123", 99.99))
            serialized shouldBe """{"orderId":"order-123","amount":99.99}"""
        }

        test("Example 2: ByteArray-based serialization") {
            val serializer = JsonSerializer.byteArray(objectMapper)

            val order = SerOrder("order-222", 299.99)

            val serialized = serializer.serialize(order)

            serialized.shouldBeInstanceOf<ByteArray>()
            String(serialized!!) shouldBe """{"orderId":"order-222","amount":299.99}"""
        }

        test("Example 4: Serialize multiple types") {
            val serializer = JsonSerializer.string(objectMapper)

            val order = SerOrder("order-456", 199.99)
            val user = SerUser("user-789", "John Doe")

            serializer.serialize(order) shouldBe """{"orderId":"order-456","amount":199.99}"""
            serializer.serialize(user) shouldBe """{"userId":"user-789","name":"John Doe"}"""
        }

        test("Example 5: Null value serialization") {
            val serializer = JsonSerializer.string(objectMapper)
            val serialized = serializer.serialize(null)
            serialized shouldBe null
        }
    })
