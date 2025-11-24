package com.trendyol.quafka.extensions.consumer.batch.pipelines

import com.trendyol.quafka.consumer.*
import com.trendyol.quafka.extensions.common.pipelines.*
import com.trendyol.quafka.extensions.consumer.single.pipelines.*
import io.kotest.core.spec.style.FunSpec
import io.kotest.matchers.shouldBe
import io.mockk.*
import org.apache.kafka.clients.consumer.ConsumerRecord

class PipelineBatchMessageHandlerTest :
    FunSpec({

        test("should execute pipeline with batch messages") {
            // Arrange
            val messages = listOf(
                createIncomingMessage("key1", "value1", 0),
                createIncomingMessage("key2", "value2", 1),
                createIncomingMessage("key3", "value3", 2)
            )
            val consumerContext = mockk<ConsumerContext>(relaxed = true)
            val executionOrder = mutableListOf<String>()

            val handler = PipelineBatchMessageHandler.createPipelineBatchMessageHandler<String, String> {
                use { envelope, next ->
                    executionOrder.add("middleware1-before")
                    next()
                    executionOrder.add("middleware1-after")
                }

                use { envelope, next ->
                    executionOrder.add("middleware2-before")
                    envelope.messages.size shouldBe 3
                    next()
                    executionOrder.add("middleware2-after")
                }

                use { envelope, next ->
                    executionOrder.add("middleware3-before")
                    next()
                    executionOrder.add("middleware3-after")
                }
            }

            // Act
            handler.invoke(messages, consumerContext)

            // Assert
            executionOrder shouldBe listOf(
                "middleware1-before",
                "middleware2-before",
                "middleware3-before",
                "middleware3-after",
                "middleware2-after",
                "middleware1-after"
            )
        }

        test("should handle attributes in batch envelope") {
            // Arrange
            val messages = listOf(
                createIncomingMessage("key1", "value1", 0)
            )
            val consumerContext = mockk<ConsumerContext>(relaxed = true)
            val testKey = AttributeKey<String>("testKey")
            var retrievedValue: String? = null

            val handler = PipelineBatchMessageHandler.createPipelineBatchMessageHandler<String, String> {
                use { envelope, next ->
                    envelope.attributes.put(testKey, "testValue")
                    next()
                }

                use { envelope, next ->
                    retrievedValue = envelope.attributes.getOrNull(testKey)
                    next()
                }
            }

            // Act
            handler.invoke(messages, consumerContext)

            // Assert
            retrievedValue shouldBe "testValue"
        }

        test("should execute middleware with BatchMessageBaseMiddleware") {
            // Arrange
            val messages = listOf(
                createIncomingMessage("key1", "value1", 0)
            )
            val consumerContext = mockk<ConsumerContext>(relaxed = true)
            var middlewareExecuted = false

            val customMiddleware = object : BatchMessageBaseMiddleware<BatchMessageEnvelope<String, String>, String, String>() {
                override suspend fun execute(
                    envelope: BatchMessageEnvelope<String, String>,
                    next: suspend (BatchMessageEnvelope<String, String>) -> Unit
                ) {
                    middlewareExecuted = true
                    next(envelope)
                }
            }

            val handler = PipelineBatchMessageHandler.createPipelineBatchMessageHandler<String, String> {
                useMiddleware(customMiddleware)
            }

            // Act
            handler.invoke(messages, consumerContext)

            // Assert
            middlewareExecuted shouldBe true
        }
    }) {

    companion object {
        private fun createIncomingMessage(key: String, value: String, offset: Long): IncomingMessage<String, String> {
            val consumerRecord = ConsumerRecord(
                "test-topic",
                0,
                offset,
                key,
                value
            )
            return IncomingMessage.create(
                consumerRecord,
                IncomingMessageStringFormatter(),
                "groupId",
                "clientId"
            )
        }
    }
}
