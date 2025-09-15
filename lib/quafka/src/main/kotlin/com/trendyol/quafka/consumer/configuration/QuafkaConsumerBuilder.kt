package com.trendyol.quafka.consumer.configuration

import com.trendyol.quafka.common.TimeProvider
import com.trendyol.quafka.consumer.*
import com.trendyol.quafka.consumer.configuration.QuafkaConsumerBuilder.SubscriptionParameters
import com.trendyol.quafka.events.EventBus
import io.github.resilience4j.retry.Retry
import kotlinx.coroutines.*
import org.apache.kafka.common.serialization.Deserializer
import kotlin.time.Duration

/**
 * Builder interface for configuring and creating a Quafka consumer.
 * Provides a fluent API for setting various options and subscribing to topics.
 *
 * @param TKey The type of the message key.
 * @param TValue The type of the message value.
 */
interface QuafkaConsumerBuilder<TKey, TValue> : SubscriptionStep<TKey, TValue> {
    /**
     * Specifies the deserializers for message keys and values.
     *
     * @param keyDeserializer The deserializer for the message key.
     * @param valueDeserializer The deserializer for the message value.
     * @return The current instance of [QuafkaConsumerBuilder].
     */
    fun withDeserializer(
        keyDeserializer: Deserializer<TKey>,
        valueDeserializer: Deserializer<TValue>
    ): QuafkaConsumerBuilder<TKey, TValue>

    /**
     * Configures startup options for the consumer.
     *
     * @param blockOnStart If true, the consumer will block during startup until fully initialized.
     * @return The current instance of [QuafkaConsumerBuilder].
     */
    fun withStartupOptions(blockOnStart: Boolean = false): QuafkaConsumerBuilder<TKey, TValue>

    /**
     * Sets the group ID for the consumer.
     *
     * @param groupId The group ID used for Kafka consumer group coordination.
     * @return The current instance of [QuafkaConsumerBuilder].
     */
    fun withGroupId(groupId: String): QuafkaConsumerBuilder<TKey, TValue>

    /**
     * Sets the dispatcher for coroutines in the consumer.
     *
     * @param dispatcher The [CoroutineDispatcher] to use for handling messages.
     * @return The current instance of [QuafkaConsumerBuilder].
     */
    fun withDispatcher(dispatcher: CoroutineDispatcher): QuafkaConsumerBuilder<TKey, TValue>

    /**
     * Sets a custom exception handler for coroutines in the consumer.
     *
     * @param handler The [CoroutineExceptionHandler] to handle coroutine exceptions.
     * @return The current instance of [QuafkaConsumerBuilder].
     */
    fun withCoroutineExceptionHandler(handler: CoroutineExceptionHandler): QuafkaConsumerBuilder<TKey, TValue>

    /**
     * Configures the consumer to automatically generate a client ID.
     *
     * @return The current instance of [QuafkaConsumerBuilder].
     */
    fun withAutoClientId(): QuafkaConsumerBuilder<TKey, TValue>

    /**
     * Sets a custom client ID for the consumer.
     *
     * @param clientId The client ID to use for the consumer.
     * @return The current instance of [QuafkaConsumerBuilder].
     */
    fun withClientId(clientId: String): QuafkaConsumerBuilder<TKey, TValue>

    /**
     * Configures a retry policy for handling connection retries.
     *
     * @param retry The [Retry] policy to apply for connection retries.
     * @return The current instance of [QuafkaConsumerBuilder].
     */
    fun withConnectionRetryPolicy(retry: Retry): QuafkaConsumerBuilder<TKey, TValue>

    /**
     * Sets commit options for the consumer.
     *
     * @param options The [CommitOptions] to configure commit behavior.
     * @return The current instance of [QuafkaConsumerBuilder].
     */
    fun withCommitOptions(options: CommitOptions): QuafkaConsumerBuilder<TKey, TValue>

    /**
     * Configures a custom string formatter for incoming messages.
     *
     * @param incomingMessageStringFormatter The formatter for incoming messages.
     * @return The current instance of [QuafkaConsumerBuilder].
     */
    fun withMessageFormatter(
        incomingMessageStringFormatter: IncomingMessageStringFormatter
    ): QuafkaConsumerBuilder<TKey, TValue>

    /**
     * Sets the timeout for graceful shutdown of the consumer.
     *
     * @param timeout The [Duration] to use for shutdown.
     * @return The current instance of [QuafkaConsumerBuilder].
     */
    fun withGracefulShutdownTimeout(timeout: Duration): QuafkaConsumerBuilder<TKey, TValue>

    /**
     * Sets the duration for polling messages from Kafka.
     *
     * @param pollDuration The [Duration] to use for polling.
     * @return The current instance of [QuafkaConsumerBuilder].
     */
    fun withPollDuration(pollDuration: Duration): QuafkaConsumerBuilder<TKey, TValue>

    /**
     * Sets a custom provider for the current instant.
     *
     * @param timeProvider The [TimeProvider] to use.
     * @return The current instance of [QuafkaConsumerBuilder].
     */
    fun withTimeProvider(timeProvider: TimeProvider): QuafkaConsumerBuilder<TKey, TValue>

    /**
     * Sets an event publisher for handling events.
     *
     * @param eventBus The [EventBus] to use.
     * @return The current instance of [QuafkaConsumerBuilder].
     */
    fun withEventBus(eventBus: EventBus): QuafkaConsumerBuilder<TKey, TValue>

    data class SubscriptionParameters(
        val timeProvider: TimeProvider,
        val incomingMessageStringFormatter: IncomingMessageStringFormatter,
        val eventBus: EventBus
    )
}

/**
 * Interface representing the final step in building the consumer, allowing additional subscriptions or completion.
 *
 * @param TKey The type of the message key.
 * @param TValue The type of the message value.
 */
interface SubscriptionStep<TKey, TValue> {
    /**
     * Subscribes to the specified topics and configures message handling options.
     *
     * @param topics The list of topic names to subscribe to.
     * @param configure A lambda to configure the subscription and message handling options.
     * @return The [SubscriptionStep] for further configuration or building the consumer.
     */
    fun subscribe(
        vararg topics: String,
        configure: SubscriptionHandlerChoiceStep<TKey, TValue>.(
            parameters: SubscriptionParameters
        ) -> SubscriptionOptionsStep<TKey, TValue>
    ): SubscriptionBuildStep<TKey, TValue>

    /**
     * Return build step without any subscription
     */
    fun withoutSubscriptions(): SubscriptionBuildStep<TKey, TValue>
}

interface SubscriptionBuildStep<TKey, TValue> : SubscriptionStep<TKey, TValue> {
    /**
     * Builds and returns the configured [QuafkaConsumer].
     *
     * @return The configured [QuafkaConsumer].
     */
    fun build(): QuafkaConsumer<TKey, TValue>
}
