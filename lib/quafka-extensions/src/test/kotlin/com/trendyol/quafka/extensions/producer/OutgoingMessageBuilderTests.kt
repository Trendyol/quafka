package com.trendyol.quafka.extensions.producer

import com.trendyol.quafka.common.*
import com.trendyol.quafka.extensions.serialization.MessageSerde
import io.kotest.core.spec.style.FunSpec
import io.kotest.matchers.shouldBe
import io.mockk.*

class OutgoingMessageBuilderTests :
    FunSpec({

        lateinit var messageSerde: MessageSerde<String, String>
        lateinit var sut: OutgoingMessageBuilder<String, String>
        beforeEach {
            messageSerde = mockk(relaxed = true)
            sut = OutgoingMessageBuilder(messageSerde)
        }

        test("should set partition in FluentOutgoingMessageBuilder") {
            // Arrange
            val builder = sut
                .new("test-topic", "test-key", "test-value")

            // Act
            val result = builder.withPartition(5)

            // Assert
            result.partition shouldBe 5
        }

        test("should set headers in FluentOutgoingMessageBuilder") {
            // Arrange
            val builder = sut
                .new("test-topic", "test-key", "test-value")
            val headers = listOf(
                header("header1", "value1"),
                header("header2", "value2")
            )

            // Act
            val result = builder.withHeaders(headers)

            // Assert
            result.headers shouldBe headers
        }

        test("should set single header in FluentOutgoingMessageBuilder") {
            // Arrange
            val builder = sut
                .new("test-topic", "test-key", "test-value")

            // Act
            val result = builder.withHeader(header("header1", "value1"))

            // Assert
            result.headers shouldBe listOf(header("header1", "value1"))
        }

        test("should set correlation metadata in FluentOutgoingMessageBuilder") {
            // Arrange
            val builder = sut
                .new("test-topic", "test-key", "test-value")

            // Act
            val result = builder.withCorrelationMetadata("new-metadata")

            // Assert
            result.correlationMetadata shouldBe "new-metadata"
        }

        test("should set timestamp in FluentOutgoingMessageBuilder") {
            // Arrange
            val builder = sut
                .new("test-topic", "test-value", "test-key")

            // Act
            val result = builder.withTimestamp(123456789L)

            // Assert
            result.timestamp shouldBe 123456789L
        }

        test("should build an OutgoingMessage with correct properties") {
            // Arrange
            val value = "test-value"
            val key = "test-key"
            every { messageSerde.serializeKey(key) } returns key
            every { messageSerde.serializeValue(value) } returns value

            val builder = sut
                .new("test-topic", key, value)
                .withPartition(1)
                .withTimestamp(123456789L)
                .withHeaders(listOf(header("header1", "value1")))
                .withCorrelationMetadata("metadata")

            // Act
            val result = builder.build()

            // Assert
            result.topic shouldBe "test-topic"
            result.value shouldBe value
            result.key shouldBe key
            result.partition shouldBe 1
            result.timestamp shouldBe 123456789L
            result.headers shouldBe listOf(header("header1", "value1"))
            result.correlationMetadata shouldBe "metadata"
        }
    })
