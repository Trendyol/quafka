package com.trendyol.quafka.consumer.configuration

import com.trendyol.quafka.common.*
import com.trendyol.quafka.consumer.*
import com.trendyol.quafka.consumer.configuration.QuafkaConsumerBuilder.SubscriptionParameters
import com.trendyol.quafka.consumer.internals.PollingConsumerFactory
import com.trendyol.quafka.events.EventBus
import com.trendyol.quafka.logging.LoggerHelper
import io.github.resilience4j.retry.*
import kotlinx.coroutines.*
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.Deserializer
import java.util.UUID
import kotlin.time.*
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.minutes
import kotlin.time.Duration.Companion.seconds

/**
 * Factory function to create an instance of [QuafkaConsumerBuilder].
 *
 * This function initializes a [QuafkaConsumerBuilder] with the provided configuration properties.
 *
 * @param TKey The type of the message key.
 * @param TValue The type of the message value.
 * @param properties A map containing configuration properties for the consumer.
 * @return A new instance of [QuafkaConsumerBuilder] configured with the provided properties.
 */
fun <TKey, TValue> QuafkaConsumerBuilder(properties: Map<String, Any>): QuafkaConsumerBuilder<TKey, TValue> =
    QuafkaConsumerBuilderImpl(properties)

private class QuafkaConsumerBuilderImpl<TKey, TValue>(
    properties: Map<String, Any>
) : QuafkaConsumerBuilder<TKey, TValue>,
    SubscriptionBuildStep<TKey, TValue> {
    private val logger = LoggerHelper.createLogger(QuafkaConsumerBuilderImpl::class.java)
    private val properties: MutableMap<String, Any> = properties.toMutableMap()
    private val subscriptions: MutableMap<String, TopicSubscriptionOptions<TKey, TValue>> = mutableMapOf()
    private var keyDeserializer: Deserializer<TKey>? = null
    private var valueDeserializer: Deserializer<TValue>? = null
    private var timeProvider: TimeProvider = SystemTimeProvider
    private var coroutineExceptionHandler: CoroutineExceptionHandler =
        CoroutineExceptionHandler { coroutineContext, throwable ->
            logger.error(
                "Coroutine $coroutineContext failed with an uncaught exception.",
                throwable
            )
        }
    private var connectionRetryPolicy: Retry = Defaults.ConnectionRetry
    private var incomingMessageStringFormatter = IncomingMessageStringFormatter.Default
    private var blockOnStart: Boolean = false
    private var maxDelayRebalance: Duration = 10.seconds
    private var pollDuration: Duration = 100.milliseconds
    private var commitOptions: CommitOptions = CommitOptions()
    private var dispatcher: CoroutineDispatcher = Dispatchers.IO
    private var eventBus: EventBus = EventBus(dispatcher)
    private var gracefulShutdownTimeout = 30.seconds

    init {
        withAutoClientId()
    }

    override fun withDeserializer(
        keyDeserializer: Deserializer<TKey>,
        valueDeserializer: Deserializer<TValue>
    ): QuafkaConsumerBuilder<TKey, TValue> =
        apply {
            this.keyDeserializer = keyDeserializer
            this.valueDeserializer = valueDeserializer
        }

    override fun withStartupOptions(blockOnStart: Boolean): QuafkaConsumerBuilder<TKey, TValue> = apply {
        this.blockOnStart = blockOnStart
    }

    override fun withGroupId(groupId: String): QuafkaConsumerBuilder<TKey, TValue> = apply {
        this.properties[ConsumerConfig.GROUP_ID_CONFIG] = groupId
    }

    override fun withDispatcher(dispatcher: CoroutineDispatcher): QuafkaConsumerBuilder<TKey, TValue> = apply {
        this.dispatcher = dispatcher
    }

    override fun withCoroutineExceptionHandler(handler: CoroutineExceptionHandler): QuafkaConsumerBuilder<TKey, TValue> = apply {
        this.coroutineExceptionHandler = handler
    }

    override fun withAutoClientId(): QuafkaConsumerBuilder<TKey, TValue> =
        withClientId("${HostName()}-${UUID.randomUUID().toString().substring(0, 5)}")

    override fun withClientId(clientId: String): QuafkaConsumerBuilderImpl<TKey, TValue> =
        apply {
            this.properties[ConsumerConfig.CLIENT_ID_CONFIG] = clientId
        }

    override fun withConnectionRetryPolicy(retry: Retry): QuafkaConsumerBuilder<TKey, TValue> =
        apply {
            this.connectionRetryPolicy = retry
        }

    override fun withCommitOptions(options: CommitOptions): QuafkaConsumerBuilder<TKey, TValue> =
        apply {
            this.commitOptions = options
        }

    override fun withMessageFormatter(incomingMessageStringFormatter: IncomingMessageStringFormatter): QuafkaConsumerBuilder<TKey, TValue> =
        apply {
            this.incomingMessageStringFormatter = incomingMessageStringFormatter
        }

    override fun withGracefulShutdownTimeout(timeout: Duration): QuafkaConsumerBuilder<TKey, TValue> = apply {
        this.gracefulShutdownTimeout = timeout
    }

    override fun withPollDuration(pollDuration: Duration): QuafkaConsumerBuilder<TKey, TValue> =
        apply {
            this.pollDuration = pollDuration
        }

    override fun withTimeProvider(timeProvider: TimeProvider): QuafkaConsumerBuilder<TKey, TValue> =
        apply {
            this.timeProvider = timeProvider
        }

    override fun withEventBus(eventBus: EventBus): QuafkaConsumerBuilder<TKey, TValue> = apply {
        this.eventBus = eventBus
    }

    override fun withoutSubscriptions(): SubscriptionBuildStep<TKey, TValue> = this

    override fun subscribe(
        vararg topics: String,
        configure: SubscriptionHandlerChoiceStep<TKey, TValue>.(parameters: SubscriptionParameters) -> SubscriptionOptionsStep<TKey, TValue>
    ): SubscriptionBuildStep<TKey, TValue> {
        val subscriptionBuilder = SubscriptionBuilder<TKey, TValue>()
        configure(subscriptionBuilder, SubscriptionParameters(timeProvider, incomingMessageStringFormatter, eventBus))
        topics.map { topicName ->
            subscriptions[topicName] = subscriptionBuilder.build(topicName)
        }

        return this
    }

    private fun buildOptions(): QuafkaConsumerOptions<TKey, TValue> {
        if (keyDeserializer == null && properties[ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG] === null) {
            throw InvalidConfigurationException(
                "key deserializer cannot be null. Please set key deserializer using `withDeserializer` method " +
                    "or pass deserializer class as a consumer property (${ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG})"
            )
        }

        if (valueDeserializer == null && properties[ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG] === null) {
            throw InvalidConfigurationException(
                "value deserializer cannot be null. Please set value deserializer using `withDeserializer` method " +
                    "or pass deserializer class as a consumer property (${ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG}) "
            )
        }

        // validate maxDelayForRebalance
        val maxPollIntervalMs =
            properties[ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG]
                ?.toString()
                ?.toLong()
                ?: Defaults.DefaultMaxPollIntervalMs
        val maxDelayForRebalanceMs = maxDelayRebalance.inWholeMilliseconds
        if (maxDelayForRebalanceMs > maxPollIntervalMs) {
            throw InvalidConfigurationException(
                "maxDelayForRebalance ($maxDelayForRebalanceMs ms) value cannot be greater than " +
                    "${ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG} ($maxPollIntervalMs ms) value"
            )
        }

        // disable auto-commit
        this.properties[ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG] = false

        return QuafkaConsumerOptions(
            properties = properties,
            keyDeserializer = keyDeserializer,
            valueDeserializer = valueDeserializer,
            timeProvider = timeProvider,
            coroutineExceptionHandler = coroutineExceptionHandler,
            connectionRetryPolicy = connectionRetryPolicy,
            incomingMessageStringFormatter = incomingMessageStringFormatter,
            blockOnStart = blockOnStart,
            pollDuration = pollDuration,
            commitOptions = commitOptions,
            dispatcher = dispatcher,
            subscriptions = subscriptions.values,
            eventBus = eventBus,
            gracefulShutdownTimeout = gracefulShutdownTimeout
        )
    }

    override fun build(): QuafkaConsumer<TKey, TValue> = QuafkaConsumer(buildOptions(), PollingConsumerFactory())

    companion object {
        private object Defaults {
            /**
             * Default maximum interval for polling messages in milliseconds.
             *
             * This value defines the maximum amount of time (5 minutes) that the consumer can remain idle
             * between poll operations without being considered unresponsive by the Kafka broker.
             */
            val DefaultMaxPollIntervalMs = 5.minutes.inWholeMilliseconds

            /**
             * Default retry policy for connection attempts.
             *
             * Configures a retry mechanism with the following parameters:
             * - Maximum attempts: 3
             * - Wait duration between attempts: 2 seconds
             * - Fail after the maximum number of attempts.
             *
             * The retry policy is named "connectionRetryPolicy" for easier identification and applies to
             * operations involving connections within the [QuafkaConsumerBuilder].
             */
            val ConnectionRetry = Retry.of(
                "connectionRetryPolicy",
                RetryConfig
                    .custom<String>()
                    .maxAttempts(3)
                    .waitDuration(2.seconds.toJavaDuration())
                    .failAfterMaxAttempts(true)
                    .build()
            )
        }
    }
}
