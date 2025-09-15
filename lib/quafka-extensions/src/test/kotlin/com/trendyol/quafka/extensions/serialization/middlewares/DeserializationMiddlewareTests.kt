package com.trendyol.quafka.extensions.serialization.middlewares

import com.trendyol.quafka.*
import com.trendyol.quafka.extensions.common.TopicPartitionProcessException
import com.trendyol.quafka.extensions.consumer.single.pipelines.buildSingleIncomingMessageContext
import com.trendyol.quafka.extensions.serialization.DeserializationResult
import com.trendyol.quafka.extensions.serialization.MessageSerde
import com.trendyol.quafka.extensions.serialization.middlewares.DeserializationMiddleware.Companion.getDeserializedKey
import com.trendyol.quafka.extensions.serialization.middlewares.DeserializationMiddleware.Companion.getDeserializedValue
import io.kotest.assertions.throwables.shouldThrow
import io.kotest.core.spec.style.FunSpec
import io.kotest.matchers.equals.shouldBeEqual
import io.mockk.*

class DeserializationMiddlewareTests :
    FunSpec({

        test("should deserialize and assign key and value to context") {
            val deserializer = mockk<MessageSerde<String, String>>(relaxed = true)
            val sut = DeserializationMiddleware(deserializer)

            val context = IncomingMessageBuilder(
                "topic",
                0,
                0,
                "key",
                "value"
            ).buildSingleIncomingMessageContext()
            every {
                deserializer.deserializeValue(context.message)
            } returns DeserializationResult.Deserialized("deserialized-value")
            every { deserializer.deserializeKey(context.message) } returns DeserializationResult.Deserialized("deserialized-key")

            sut.execute(context) { }

            context.getDeserializedValue<String>()!! shouldBeEqual "deserialized-value"
            context.getDeserializedKey<String>()!! shouldBeEqual "deserialized-key"
        }

        test("should throw exception when deserialized value is null") {
            val deserializer = mockk<MessageSerde<String, String>>(relaxed = true)
            val sut = DeserializationMiddleware(deserializer, throwExceptionIfValueIsNull = true)

            val context = IncomingMessageBuilder(
                "topic",
                0,
                0,
                "key",
                "value"
            ).buildSingleIncomingMessageContext()
            every { deserializer.deserializeValue(context.message) } returns DeserializationResult.Null

            val exception = shouldThrow<TopicPartitionProcessException> { sut.execute(context) { } }

            exception.topicPartition shouldBeEqual context.message.topicPartition
            exception.offset shouldBeEqual context.message.offset
        }

        test("should throw exception when deserialized key is null") {
            val deserializer = mockk<MessageSerde<String, String>>(relaxed = true)
            val sut = DeserializationMiddleware(deserializer, throwExceptionIfKeyIsNull = true)

            val context = IncomingMessageBuilder(
                "topic",
                0,
                0,
                "key",
                "value"
            ).buildSingleIncomingMessageContext()
            every { deserializer.deserializeKey(context.message) } returns DeserializationResult.Null

            val exception = shouldThrow<TopicPartitionProcessException> { sut.execute(context) { } }

            exception.topicPartition shouldBeEqual context.message.topicPartition
            exception.offset shouldBeEqual context.message.offset
        }
    })
