package com.trendyol.quafka.extensions.serialization.json

import com.fasterxml.jackson.core.JsonParseException
import com.trendyol.quafka.consumer.*
import com.trendyol.quafka.extensions.Defaults.objectMapper
import com.trendyol.quafka.extensions.serialization.DeserializationResult
import com.trendyol.quafka.extensions.serialization.json.typeResolvers.TypeResolver
import io.kotest.core.spec.style.FunSpec
import io.kotest.matchers.shouldBe
import io.kotest.matchers.string.shouldStartWith
import io.mockk.*

private data class DummyObject2(
    val id: String
)

class StringJsonMessageSerdeTests :
    FunSpec({
        val typeResolver = mockk<TypeResolver>(relaxed = true)
        val serde = StringJsonMessageSerde(objectMapper, typeResolver)

        test("should return null when serializing a null key") {
            // Arrange
            val key: Any? = null

            // Act
            val result = serde.serializeKey(key)

            // Assert
            result shouldBe null
        }

        test("should serialize key") {
            // Arrange
            val key: Any = 12345

            // Act
            val result = serde.serializeKey(key)

            // Assert
            result shouldBe "12345"
        }

        test("should return null when serializing a null value") {
            // Arrange
            val value: Any? = null

            // Act
            val result = serde.serializeValue(value)

            // Assert
            result shouldBe null
        }

        test("should serialize value") {
            // Arrange
            val value = DummyObject2(id = "12345")

            // Act
            val result = serde.serializeValue(value)

            // Assert
            result shouldBe "{\"id\":\"12345\"}"
        }

        test("should return null when deserializing a null key") {
            // Arrange
            val incomingMessage = mockk<IncomingMessage<String?, String?>>(relaxed = true)
            every { incomingMessage.key } returns null

            // Act
            val result = serde.deserializeKey(incomingMessage)

            // Assert
            result shouldBe DeserializationResult.Null
        }

        test("should return error when deserializing a null key") {
            // Arrange
            val incomingMessage = mockk<IncomingMessage<String?, String?>>(relaxed = true)
            val exception = Exception("parsing error")
            every { incomingMessage.key } throws exception
            every { incomingMessage.topicPartitionOffset } returns TopicPartitionOffset("topic1", 1, 0)

            // Act
            val result = serde.deserializeKey(incomingMessage) as DeserializationResult.Error

            // Assert
            result.topicPartitionOffset shouldBe TopicPartitionOffset("topic1", 1, 0)
            result.message shouldBe "error occurred deserializing key"
            result.cause shouldBe exception
        }

        test("should deserialize key") {
            // Arrange
            val incomingMessage = mockk<IncomingMessage<String?, String?>>(relaxed = true)
            every { incomingMessage.key } returns "testKey"

            // Act
            val result = serde.deserializeKey(incomingMessage)

            // Assert
            result shouldBe DeserializationResult.Deserialized("testKey")
        }

        test("should return null when deserializing a null value") {
            // Arrange
            val incomingMessage = mockk<IncomingMessage<String?, String?>>(relaxed = true)
            every { incomingMessage.value } returns null

            // Act
            val result = serde.deserializeValue(incomingMessage)

            // Assert
            result shouldBe DeserializationResult.Null
        }

        test("should return deserialized value") {
            // Arrange
            val incomingMessage = mockk<IncomingMessage<String?, String?>>(relaxed = true)
            val obj = DummyObject2("test-id")
            val serialized = objectMapper.writeValueAsString(obj)
            every { incomingMessage.value } returns serialized
            every { typeResolver.resolve(any(), any()) } returns DummyObject2::class.java

            // Act
            val result = serde.deserializeValue(incomingMessage)

            // Assert
            result shouldBe DeserializationResult.Deserialized(obj)
        }

        test("should return Error when deserializing invalid JSON") {
            // Arrange
            val incomingMessage = mockk<IncomingMessage<String?, String?>>(relaxed = true)
            every { incomingMessage.value } returns "invalid-json"
            every { incomingMessage.topicPartitionOffset } returns TopicPartitionOffset("topic1", 1, 0)
            every { typeResolver.resolve(any(), any()) } returns DummyObject2::class.java

            // Act
            val result = serde.deserializeValue(incomingMessage) as DeserializationResult.Error

            // Assert
            result.topicPartitionOffset shouldBe TopicPartitionOffset("topic1", 1, 0)
            result.message shouldBe "error occurred deserializing value as json"
            result.cause!!::class shouldBe JsonParseException::class
        }

        test("should return Error when deserialization fails due to type resolution exception") {
            // Arrange
            val incomingMessage = mockk<IncomingMessage<String?, String?>>(relaxed = true)
            val obj = DummyObject2("test-id")
            val serialized = objectMapper.writeValueAsString(obj)
            every { incomingMessage.value } returns serialized
            every { incomingMessage.topicPartitionOffset } returns TopicPartitionOffset("topic1", 1, 0)
            val typeException = Exception("type error")
            every { typeResolver.resolve(any(), any()) } throws typeException

            // Act
            val result = serde.deserializeValue(incomingMessage) as DeserializationResult.Error

            // Assert
            result.topicPartitionOffset shouldBe TopicPartitionOffset("topic1", 1, 0)
            result.message shouldBe "An error occurred when resolving type"
            result.cause shouldBe typeException
        }

        test("should return Error when type resolver returns null during deserialization") {
            // Arrange
            val incomingMessage = mockk<IncomingMessage<String?, String?>>(relaxed = true)
            val obj = DummyObject2("test-id")
            val serialized = objectMapper.writeValueAsString(obj)
            every { incomingMessage.value } returns serialized
            every { incomingMessage.topicPartitionOffset } returns TopicPartitionOffset("topic1", 1, 0)
            every { typeResolver.resolve(any(), any()) } returns null

            // Act
            val result = serde.deserializeValue(incomingMessage) as DeserializationResult.Error

            // Assert
            result.topicPartitionOffset shouldBe TopicPartitionOffset("topic1", 1, 0)
            result.message shouldBe "Type not resolved!!"
            result.cause shouldBe null
        }

        test("should return Error when deserialization fails due to object binding exception") {
            // Arrange
            val incomingMessage = mockk<IncomingMessage<String?, String?>>(relaxed = true)
            val obj = DummyObject2("test-id")
            val serialized = objectMapper.writeValueAsString(obj)
            every { incomingMessage.value } returns serialized
            every { incomingMessage.topicPartitionOffset } returns TopicPartitionOffset("topic1", 1, 0)
            every { typeResolver.resolve(any(), any()) } returns List::class.java

            // Act
            val result = serde.deserializeValue(incomingMessage) as DeserializationResult.Error

            // Assert
            result.topicPartitionOffset shouldBe TopicPartitionOffset("topic1", 1, 0)
            result.message shouldStartWith "error occurred binding message to object"
            result.cause!!::class shouldBe IllegalArgumentException::class
        }
    })
