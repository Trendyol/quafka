package com.trendyol.quafka.extensions.serialization.middlewares

import com.trendyol.quafka.*
import com.trendyol.quafka.extensions.consumer.single.pipelines.buildSingleIncomingMessageContext
import com.trendyol.quafka.extensions.serialization.DeserializationException
import com.trendyol.quafka.extensions.serialization.DeserializationResult
import com.trendyol.quafka.extensions.serialization.MessageDeserializer
import com.trendyol.quafka.extensions.serialization.middlewares.DeserializationMiddleware.Companion.getDeserializedValue
import io.kotest.assertions.throwables.shouldThrow
import io.kotest.core.spec.style.FunSpec
import io.kotest.matchers.equals.shouldBeEqual
import io.mockk.*

class DeserializationMiddlewareTests :
    FunSpec({

        test("should deserialize and value to context") {
            val deserializer = mockk<MessageDeserializer<String, String>>(relaxed = true)
            val sut = DeserializationMiddleware(deserializer)

            val context = IncomingMessageBuilder(
                "topic",
                0,
                0,
                "key",
                "value"
            ).buildSingleIncomingMessageContext()
            every {
                deserializer.deserialize<Any>(context.message)
            } returns DeserializationResult.Deserialized("deserialized-value")

            sut.execute(context) { }

            context.getDeserializedValue<String>()!! shouldBeEqual "deserialized-value"
        }

        test("should throw exception when deserialized value is null") {
            val deserializer = mockk<MessageDeserializer<String, String>>(relaxed = true)
            val sut = DeserializationMiddleware(deserializer, throwExceptionIfValueIsNull = true)

            val context = IncomingMessageBuilder(
                "topic",
                0,
                0,
                "key",
                "value"
            ).buildSingleIncomingMessageContext()
            every { deserializer.deserialize<Any>(context.message) } returns DeserializationResult.Null

            val exception = shouldThrow<DeserializationException> { sut.execute(context) { } }

            exception.topicPartitionOffset shouldBeEqual context.message.topicPartitionOffset
        }
    })
