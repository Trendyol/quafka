package com.trendyol.quafka.producer.configuration

import com.trendyol.quafka.common.TimeProvider
import com.trendyol.quafka.producer.*
import org.apache.kafka.common.serialization.Serializer

/**
 * Builder interface for configuring and creating a Quafka producer.
 *
 * This interface provides a fluent API for setting various options such as serializers, client ID,
 * error handling, and message formatting.
 *
 * @param TKey The type of the message key.
 * @param TValue The type of the message value.
 */
interface QuafkaProducerBuilder<TKey, TValue> {
    /**
     * Specifies the serializers for message keys and values.
     *
     * @param keySerializer The serializer for the message key.
     * @param valueSerializer The serializer for the message value.
     * @return The current instance of [QuafkaProducerBuilder].
     */
    fun withSerializer(
        keySerializer: Serializer<TKey>,
        valueSerializer: Serializer<TValue>
    ): QuafkaProducerBuilder<TKey, TValue>

    /**
     * Configures the producer to automatically generate a client ID.
     *
     * @return The current instance of [QuafkaProducerBuilder].
     */
    fun withAutoClientId(): QuafkaProducerBuilder<TKey, TValue>

    /**
     * Sets a custom client ID for the producer.
     *
     * @param clientId The client ID to use for the producer.
     * @return The current instance of [QuafkaProducerBuilder].
     */
    fun withClientId(clientId: String): QuafkaProducerBuilder<TKey, TValue>

    /**
     * Configures error-handling options for the producer.
     *
     * @param producingOptions The [ProducingOptions] to use for error handling.
     * @return The current instance of [QuafkaProducerBuilder].
     */
    fun withErrorOptions(producingOptions: ProducingOptions): QuafkaProducerBuilder<TKey, TValue>

    /**
     * Configures a formatter for outgoing messages.
     *
     * @param outgoingMessageStringFormatter The formatter for outgoing messages.
     * @return The current instance of [QuafkaProducerBuilder].
     */
    fun withMessageFormatter(
        outgoingMessageStringFormatter: OutgoingMessageStringFormatter
    ): QuafkaProducerBuilder<TKey, TValue>

    /**
     * Sets a custom provider for the current instant.
     *
     * @param timeProvider The [TimeProvider] to use.
     * @return The current instance of [QuafkaProducerBuilder].
     */
    fun withTimeProvider(timeProvider: TimeProvider): QuafkaProducerBuilder<TKey, TValue>

    /**
     * Builds and returns the configured [QuafkaProducer].
     *
     * @return The configured [QuafkaProducer].
     */
    fun build(): QuafkaProducer<TKey, TValue>
}

/**
 * Extension function to set the same serializer for both key and value.
 *
 * This is a convenience method for cases where the key and value types are the same.
 * Instead of calling `withSerializer(serializer, serializer)`, you can simply call
 * `withSerializer(serializer)`.
 *
 * @param T The common type for both key and value.
 * @param serializer The serializer to use for both key and value serialization.
 * @return The builder instance for method chaining.
 *
 * @sample
 * ```kotlin
 * val producer = QuafkaProducerBuilder<String, String>(properties)
 *     .withSerializer(StringSerializer())
 *     .build()
 * ```
 */
fun <T> QuafkaProducerBuilder<T, T>.withSerializer(
    serializer: Serializer<T>
): QuafkaProducerBuilder<T, T> = withSerializer(serializer, serializer)
