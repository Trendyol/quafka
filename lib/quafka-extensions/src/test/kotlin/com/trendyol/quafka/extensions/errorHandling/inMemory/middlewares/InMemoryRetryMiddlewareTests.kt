package com.trendyol.quafka.extensions.errorHandling.inMemory.middlewares

import com.trendyol.quafka.IncomingMessageBuilder
import com.trendyol.quafka.extensions.consumer.single.pipelines.*
import com.trendyol.quafka.extensions.errorHandling.inMemory.*
import io.kotest.assertions.throwables.shouldThrow
import io.kotest.core.spec.style.FunSpec
import io.kotest.matchers.equals.shouldBeEqual
import io.kotest.matchers.shouldBe

class InMemoryRetryMiddlewareTests :
    FunSpec({

        test("should retry when error occurred") {
            val sut = InMemoryRetryMiddleware<SingleMessageEnvelope<String, String>, String, String>(
                InMemoryRetryErrorHandler.default()
            )

            val context = buildIncomingMessageContext()
            var counter = 0
            val exception = shouldThrow<Exception> {
                sut.execute(context) {
                    counter++
                    throw Exception("an error occurred")
                }
            }

            counter shouldBeEqual 3
            exception.message shouldBe "an error occurred"
        }
    })

private suspend fun buildIncomingMessageContext(): SingleMessageEnvelope<String, String> {
    val context: SingleMessageEnvelope<String, String> =
        IncomingMessageBuilder(
            "topic",
            0,
            0,
            "key",
            "value"
        ).buildSingleIncomingMessageContext()
    return context
}
