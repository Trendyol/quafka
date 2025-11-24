package com.trendyol.quafka.extensions.serialization.json

import com.trendyol.quafka.consumer.configuration.QuafkaConsumerBuilder
import com.trendyol.quafka.producer.configuration.QuafkaProducerBuilder
import org.apache.kafka.common.serialization.*

private typealias Key = String?
private typealias Value = ByteArray?

/**
 * Creates a pre-configured [QuafkaConsumerBuilder] for JSON message consumption.
 *
 * This builder is configured with:
 * - String key deserializer (for message keys)
 * - ByteArray value deserializer (for JSON message bodies)
 *
 * After creating the consumer, use [JsonDeserializer] to deserialize message values
 * from ByteArray to your domain objects.
 *
 * @param properties Kafka consumer configuration properties.
 * @return A configured [QuafkaConsumerBuilder] for JSON messages.
 *
 * @sample
 * ```kotlin
 * val consumer = JsonQuafkaConsumerBuilder(consumerProps)
 *     .subscribe("orders") {
 *         withSingleMessageHandler { message, _ ->
 *             val deserializer = JsonDeserializer.byteArray(objectMapper, typeResolver)
 *             when (val result = deserializer.deserialize<Order>(message)) {
 *                 is DeserializationResult.Deserialized -> processOrder(result.value)
 *                 is DeserializationResult.Error -> handleError(result)
 *                 DeserializationResult.Null -> handleNull()
 *             }
 *             message.ack()
 *         }
 *     }
 *     .build()
 * ```
 */
@Suppress("FunctionName")
fun JsonQuafkaConsumerBuilder(
    properties: Map<String, Any>
): QuafkaConsumerBuilder<Key, Value> =
    QuafkaConsumerBuilder<Key, Value>(properties)
        .withDeserializer(StringDeserializer(), ByteArrayDeserializer())

/**
 * Creates a pre-configured [QuafkaProducerBuilder] for JSON message production.
 *
 * This builder is configured with:
 * - String key serializer (for message keys)
 * - ByteArray value serializer (for JSON message bodies)
 *
 * Use [JsonSerializer] with [OutgoingMessageBuilder] to serialize your domain objects
 * to ByteArray before sending.
 *
 * @param properties Kafka producer configuration properties.
 * @return A configured [QuafkaProducerBuilder] for JSON messages.
 *
 * @sample
 * ```kotlin
 * val producer = JsonQuafkaProducerBuilder(producerProps)
 *     .build()
 *
 * val serializer = JsonSerializer.byteArray(objectMapper)
 * val messageBuilder = OutgoingMessageBuilder.create<String?, ByteArray?>(serializer)
 *
 * val message = messageBuilder
 *     .newMessageWithTypeInfo("orders", orderId, order)
 *     .build()
 *
 * producer.send(message)
 * ```
 */
@Suppress("FunctionName")
fun JsonQuafkaProducerBuilder(
    properties: Map<String, Any>
): QuafkaProducerBuilder<Key, Value> = QuafkaProducerBuilder<Key, Value>(properties)
    .withSerializer(StringSerializer(), ByteArraySerializer())
