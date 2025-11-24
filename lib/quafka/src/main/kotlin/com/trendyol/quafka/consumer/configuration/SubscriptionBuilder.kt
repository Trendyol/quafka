package com.trendyol.quafka.consumer.configuration

import com.trendyol.quafka.consumer.messageHandlers.*
import com.trendyol.quafka.consumer.messageHandlers.BatchMessageHandlingStrategy.Companion.DEFAULT_BATCH_SIZE
import com.trendyol.quafka.consumer.messageHandlers.BatchMessageHandlingStrategy.Companion.DEFAULT_BATCH_TIMEOUT
import com.trendyol.quafka.consumer.messageHandlers.ResilientHandler
import kotlin.time.Duration
import kotlin.time.Duration.Companion.seconds

/**
 * Adds a **single-message** handler and lets you fine-tune its strategy.
 *
 * You must call either [withSingleMessageHandler] **or** [SubscriptionWithBatchMessageHandlingStep.withBatchMessageHandler] before any other configuration.
 */
interface SubscriptionWithSingleMessageHandlingStep<TKey, TValue> {
    /**
     * @param handler   The function that processes each incoming message.
     * @param configure Optional DSL block to tweak the [SingleMessageHandlingStrategy].
     *
     * @return next step where subscription-level options can be set.
     */
    fun withSingleMessageHandler(
        configure: SingleMessageHandlingStrategy<TKey, TValue>.() -> Unit = {},
        handler: SingleMessageHandler<TKey, TValue>
    ): SubscriptionOptionsStep<TKey, TValue>
}

/**
 * Adds a **batch** handler and lets you fine-tune its strategy.
 *
 * You must call either [SubscriptionWithSingleMessageHandlingStep.withSingleMessageHandler] **or** [withBatchMessageHandler] before any other configuration.
 */
interface SubscriptionWithBatchMessageHandlingStep<TKey, TValue> {
    /**
     * @param handler      The function that processes each batch.
     * @param batchSize    Maximum messages per batch (default = [DEFAULT_BATCH_SIZE]).
     * @param batchTimeout Max wait time for a batch to fill (default = [DEFAULT_BATCH_TIMEOUT]).
     * @param configure    Optional DSL block to tweak the [BatchMessageHandlingStrategy].
     *
     * @return next step where subscription-level options can be set.
     */
    fun withBatchMessageHandler(
        configure: BatchMessageHandlingStrategy<TKey, TValue>.() -> Unit = {},
        batchSize: Int = DEFAULT_BATCH_SIZE,
        batchTimeout: Duration = DEFAULT_BATCH_TIMEOUT,
        handler: BatchMessageHandler<TKey, TValue>
    ): SubscriptionOptionsStep<TKey, TValue>
}

interface SubscriptionHandlerChoiceStep<TKey, TValue> :
    SubscriptionWithSingleMessageHandlingStep<TKey, TValue>,
    SubscriptionWithBatchMessageHandlingStep<TKey, TValue>

/**
 * The default backpressure buffer size.
 *
 * Set to [Int.MAX_VALUE] which effectively disables backpressure by default.
 * Consider setting a lower value based on your application's processing capacity.
 */
const val DEFAULT_BACKPRESSURE_BUFFER_SIZE = Int.MAX_VALUE

/**
 * The default backpressure release timeout.
 *
 * Set to [Duration.ZERO] which means backpressure is released immediately by default.
 * Consider setting a non-zero value to prevent overwhelming the consumer.
 */
val DEFAULT_BACKPRESSURE_RELEASE_TIMEOUT = Duration.ZERO

/**
 * Optional settings that apply after a handler is chosen.
 */
interface SubscriptionOptionsStep<TKey, TValue> {
    /**
     * Configures the automatic acknowledgment behavior.
     *
     * When enabled, messages will be automatically acknowledged after successful processing.
     * When disabled, messages must be manually acknowledged using the message's ack() method.
     *
     * @param value If `true`, messages will be automatically acknowledged after processing.
     * @return The current instance of [SubscriptionBuilder] to allow method chaining.
     */
    fun autoAckAfterProcess(value: Boolean): SubscriptionOptionsStep<TKey, TValue>

    /**
     * Configures the resilient handler for message processing failures.
     *
     * The resilient handler wraps message processing execution and controls how failures are handled
     * (e.g., retry with exponential backoff, skip and log).
     *
     * @param resilientHandler The resilient handler to use for processing failures.
     * @return The current instance of [SubscriptionBuilder] to allow method chaining.
     */
    fun withResilientHandler(resilientHandler: ResilientHandler<TKey, TValue>): SubscriptionOptionsStep<TKey, TValue>

    /**
     * Configures backpressure settings for the consumer.
     *
     * Backpressure helps prevent overwhelming the consumer by limiting the number of
     * unprocessed messages and providing a mechanism to temporarily pause message fetching.
     *
     * @param backpressureBufferSize The maximum number of messages to buffer before applying backpressure.
     * @param backpressureReleaseTimeout The maximum duration to wait before releasing backpressure.
     * @return The current instance of [SubscriptionBuilder] to allow method chaining.
     */
    fun withBackpressure(
        backpressureBufferSize: Int = DEFAULT_BACKPRESSURE_BUFFER_SIZE,
        backpressureReleaseTimeout: Duration = DEFAULT_BACKPRESSURE_RELEASE_TIMEOUT
    ): SubscriptionOptionsStep<TKey, TValue>
}

internal class SubscriptionBuilder<TKey, TValue> :
    SubscriptionOptionsStep<TKey, TValue>,
    SubscriptionHandlerChoiceStep<TKey, TValue> {
    private var strategyFactory: (() -> MessageHandlingStrategy<TKey, TValue>)? = null
    private var autoAck: Boolean = true
    private val defaultSafetyNetHandler: ResilientHandler.Stopping<TKey, TValue> = ResilientHandler.Stopping()
    private var resilientHandler: ResilientHandler<TKey, TValue> = ResilientHandler.WithSafetyNet(
        userHandler = ResilientHandler.Retrying.basic(initialInterval = 30.seconds, maxAttempts = 1000),
        safetyNetHandler = defaultSafetyNetHandler
    )

    private var backpressureBufferSize: Int = DEFAULT_BACKPRESSURE_BUFFER_SIZE
    private var backpressureReleaseTimeout: Duration = DEFAULT_BACKPRESSURE_RELEASE_TIMEOUT

    override fun autoAckAfterProcess(value: Boolean) = apply {
        this.autoAck = value
    }

    override fun withResilientHandler(resilientHandler: ResilientHandler<TKey, TValue>) = apply {
        // Wrap user's handler with safety net for extra protection
        this.resilientHandler = ResilientHandler.WithSafetyNet(
            userHandler = resilientHandler,
            safetyNetHandler = defaultSafetyNetHandler
        )
    }

    override fun withBackpressure(
        backpressureBufferSize: Int,
        backpressureReleaseTimeout: Duration
    ) = apply {
        this.backpressureBufferSize = backpressureBufferSize
        this.backpressureReleaseTimeout = backpressureReleaseTimeout
    }

    override fun withSingleMessageHandler(
        configure: SingleMessageHandlingStrategy<TKey, TValue>.() -> Unit,
        handler: SingleMessageHandler<TKey, TValue>
    ): SubscriptionOptionsStep<TKey, TValue> {
        this.strategyFactory = {
            SingleMessageHandlingStrategy(handler, this.autoAck, this.resilientHandler).apply(configure)
        }
        return this
    }

    override fun withBatchMessageHandler(
        configure: BatchMessageHandlingStrategy<TKey, TValue>.() -> Unit,
        batchSize: Int,
        batchTimeout: Duration,
        handler: BatchMessageHandler<TKey, TValue>
    ): SubscriptionOptionsStep<TKey, TValue> {
        this.strategyFactory = {
            BatchMessageHandlingStrategy(handler, batchSize, batchTimeout, autoAck, resilientHandler).apply(configure)
        }
        return this
    }

    internal fun build(topicName: String): TopicSubscriptionOptions<TKey, TValue> = TopicSubscriptionOptions(
        topic = topicName,
        autoAck = autoAck,
        backpressureBufferSize = backpressureBufferSize,
        backpressureReleaseTimeout = backpressureReleaseTimeout,
        messageHandlingStrategy = strategyFactory!!()
    )
}
