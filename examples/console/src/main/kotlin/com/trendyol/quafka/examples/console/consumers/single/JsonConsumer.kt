package com.trendyol.quafka.examples.console.consumers.single

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import com.trendyol.quafka.common.HeaderParsers.asString
import com.trendyol.quafka.common.get
import com.trendyol.quafka.consumer.configuration.QuafkaConsumerBuilder
import com.trendyol.quafka.examples.console.consumers.ConsumerBase
import com.trendyol.quafka.extensions.producer.OutgoingMessageBuilder
import com.trendyol.quafka.extensions.serialization.DeserializationResult
import com.trendyol.quafka.extensions.serialization.json.*
import org.apache.kafka.common.serialization.*

/**
 * JSON consumer demonstrating type-safe deserialization with JsonDeserializer and JsonSerializer.
 *
 * **What it shows:**
 * - Separation of serialization and deserialization concerns
 * - JsonSerializer for producing messages (no typeResolver needed)
 * - JsonDeserializer for consuming messages (with typeResolver)
 * - Four type resolution approaches (lambda, header-based, mapping, chained)
 * - Single-pass JSON parsing (no double-parsing)
 * - Type-safe deserialization with Jackson
 * - Handling DeserializationResult (Deserialized, Error, Null)
 * - Self-contained example (produces and consumes its own messages)
 *
 * **Type resolution strategies:**
 * 1. Lambda resolver - Custom logic for type determination
 * 2. Header resolver - Extract from message headers
 * 3. Mapping resolver - Explicit type mapping (most type-safe)
 * 4. Chain resolver - Fallback through multiple strategies
 *
 * **Use when:** Need type-safe JSON handling with flexible type resolution.
 */
class JsonConsumer(properties: MutableMap<String, Any>, val producerProperties: MutableMap<String, Any>) : ConsumerBase(properties) {
    override suspend fun run() {
        val objectMapper = ObjectMapper().apply {
            registerKotlinModule()
        }

        sendMessage(objectMapper)

        // CONSUMER SETUP - Uses JsonDeserializer (with typeResolver)
        // Option 1: Using lambda directly with custom logic
        val lambdaResolver: TypeResolver<ByteArray?, ByteArray?> = { json, message ->
            message.headers.get("X-EventType")?.asString()?.let { typeName ->
                try {
                    Class.forName("com.trendyol.quafka.examples.console.$typeName")
                } catch (e: ClassNotFoundException) {
                    null
                }
            }
        }

        // Option 2: Using TypeResolvers helper (cleaner!)
        val headerResolver = TypeResolvers.fromHeader(
            "X-MessageType",
            "com.trendyol.quafka.examples.console"
        )

        // Option 3: Using explicit mapping (most type-safe!)
        val mappingResolver = TypeResolvers.fromMapping(
            mapOf(
                "OrderCreated" to OrderCreated::class.java
            ),
            "X-MessageType"
        )

        // Option 4: Using chain for fallback strategies
        val chainResolver = TypeResolvers.chain(
            lambdaResolver,
            headerResolver,
            mappingResolver
        )

        val deserializer = JsonDeserializer.byteArray(objectMapper, typeResolver = chainResolver)

        // Use the mapping-based deserializer for this example (most type-safe)
        val consumer = QuafkaConsumerBuilder<ByteArray?, ByteArray?>(properties)
            .withDeserializer(ByteArrayDeserializer(), ByteArrayDeserializer())
            .subscribe("demo-orders") {
                withSingleMessageHandler { incomingMessage, _ ->
                    when (val deserializationResult = deserializer.deserialize<OrderCreated>(incomingMessage)) {
                        is DeserializationResult.Deserialized<*> -> println("VALUE: " + deserializationResult.value)
                        is DeserializationResult.Error -> print("VALUE ERROR: ${deserializationResult.message}")
                        DeserializationResult.Null -> print("NULL VALUE")
                    }

                    incomingMessage.ack()
                }
            }
            .build()

        consumer.start()
    }

    private suspend fun sendMessage(objectMapper: ObjectMapper) {
        // PRODUCER SETUP - Uses JsonSerializer
        val serializer = JsonSerializer.create<ByteArray?>(objectMapper)
        val messageBuilder = OutgoingMessageBuilder.create<String?, ByteArray?>(serializer)
        val producer = JsonQuafkaProducerBuilder(producerProperties)
            .withSerializer(StringSerializer(), ByteArraySerializer())
            .build()

        // Produce example message
        val outgoingMessage = messageBuilder.newMessageWithTypeInfo(
            "demo-orders",
            "order/23",
            OrderCreated("order/23")
        ).build()
        producer.use { it.send(outgoingMessage) }
    }

    data class OrderCreated(val id: String)
}
