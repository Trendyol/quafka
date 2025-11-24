package com.trendyol.quafka.producer.configuration

import com.trendyol.quafka.common.*
import com.trendyol.quafka.common.HostName
import com.trendyol.quafka.producer.*
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.Serializer
import java.util.UUID
import kotlin.collections.Map
import kotlin.collections.MutableMap
import kotlin.collections.set
import kotlin.collections.toMutableMap

/**
 * Factory function to create a new instance of [QuafkaProducerBuilder].
 *
 * This function initializes a [QuafkaProducerBuilder] with the provided configuration properties.
 *
 * @param TKey The type of the message key.
 * @param TValue The type of the message value.
 * @param properties A map containing configuration properties for the producer.
 * @return A new instance of [QuafkaProducerBuilder] configured with the provided properties.
 */
@Suppress("FunctionName")
fun <TKey, TValue> QuafkaProducerBuilder(
    properties: Map<String, Any>
): QuafkaProducerBuilder<TKey, TValue> = QuafkaProducerBuilderImpl(properties)

private class QuafkaProducerBuilderImpl<TKey, TValue>(properties: Map<String, Any>) : QuafkaProducerBuilder<TKey, TValue> {
    private val properties: MutableMap<String, Any> = properties.toMutableMap()
    private var timeProvider: TimeProvider = SystemTimeProvider
    private var outgoingMessageStringFormatter = OutgoingMessageStringFormatter.Default
    private var keySerializer: Serializer<TKey>? = null
    private var valueSerializer: Serializer<TValue>? = null
    private var producingOptions: ProducingOptions = ProducingOptions(stopOnFirstError = true, throwException = true)

    init {
        withAutoClientId()
    }

    override fun withSerializer(
        keySerializer: Serializer<TKey>,
        valueSerializer: Serializer<TValue>
    ): QuafkaProducerBuilder<TKey, TValue> =
        apply {
            this.keySerializer = keySerializer
            this.valueSerializer = valueSerializer
        }

    override fun withAutoClientId(): QuafkaProducerBuilder<TKey, TValue> =
        withClientId("${HostName()}-${UUID.randomUUID().toString().substring(0, 5)}")

    override fun withClientId(clientId: String): QuafkaProducerBuilderImpl<TKey, TValue> =
        apply {
            this.properties[ProducerConfig.CLIENT_ID_CONFIG] = clientId
        }

    override fun withErrorOptions(producingOptions: ProducingOptions): QuafkaProducerBuilder<TKey, TValue> =
        apply {
            this.producingOptions = producingOptions
        }

    override fun withMessageFormatter(outgoingMessageStringFormatter: OutgoingMessageStringFormatter): QuafkaProducerBuilder<TKey, TValue> =
        apply {
            this.outgoingMessageStringFormatter = outgoingMessageStringFormatter
        }

    override fun withTimeProvider(timeProvider: TimeProvider): QuafkaProducerBuilder<TKey, TValue> =
        apply {
            this.timeProvider = timeProvider
        }

    override fun build(): QuafkaProducer<TKey, TValue> {
        if (keySerializer == null && properties[ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG] === null) {
            throw InvalidConfigurationException(
                "key serializer cannot be null. Please set key serializer using `withSerializer` method " +
                    "or pass deserializer class as a consumer property (${ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG})"
            )
        }

        if (valueSerializer == null && properties[ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG] === null) {
            throw InvalidConfigurationException(
                "value serializer cannot be null. Please set value serializer using `withSerializer` method " +
                    "or pass serializer class as a consumer property (${ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG}) "
            )
        }

        return QuafkaProducer(
            QuafkaProducerOptions(
                properties = properties,
                timeProvider = timeProvider,
                outgoingMessageStringFormatter = outgoingMessageStringFormatter,
                keySerializer = keySerializer,
                valueSerializer = valueSerializer,
                producingOptions = producingOptions
            )
        )
    }
}
