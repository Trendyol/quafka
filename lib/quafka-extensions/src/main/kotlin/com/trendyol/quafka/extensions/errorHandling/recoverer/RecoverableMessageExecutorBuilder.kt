package com.trendyol.quafka.extensions.errorHandling.recoverer

import com.trendyol.quafka.consumer.*
import com.trendyol.quafka.extensions.delaying.MessageDelayer
import com.trendyol.quafka.producer.QuafkaProducer
import kotlin.time.Duration

/**
 * Builder for creating [RecoverableMessageExecutor] instances with customized behavior.
 *
 * This builder follows the fluent API pattern, allowing you to chain configuration methods.
 * All configuration is optional - sensible defaults are provided for all settings.
 *
 * @param TKey The type of the message key.
 * @param TValue The type of the message value.
 * @property topicResolver Resolves topic configurations for incoming messages.
 * @property danglingTopic The fallback topic for messages from unconfigured topics.
 * @property producer The Kafka producer for sending messages to retry/error topics.
 * @property messageDelayer Handles delayed message processing.
 */
class RecoverableMessageExecutorBuilder<TKey, TValue>(
    private val topicResolver: TopicResolver,
    private val danglingTopic: String,
    private val producer: QuafkaProducer<TKey, TValue>,
    private var messageDelayer: MessageDelayer
) {
    private var outgoingMessageModifierDelegate: OutgoingMessageModifierDelegate<TKey, TValue> = { _, _, _ -> this }
    private var retryPolicyProvider: RetryPolicyProvider = createCompositeRetryPolicyProvider()
    private var exceptionDetailsProvider: ExceptionDetailsProvider =
        { throwable: Throwable -> ExceptionDetails(throwable, throwable.message ?: throwable.javaClass.simpleName) }

    /**
     * Sets a custom exception details provider.
     *
     * @param exceptionDetailsProvider Function to extract structured details from exceptions.
     * @return This builder for chaining.
     */
    fun withExceptionDetailsProvider(exceptionDetailsProvider: ExceptionDetailsProvider) = apply {
        this.exceptionDetailsProvider = exceptionDetailsProvider
    }

    /**
     * Sets a custom message delayer.
     *
     * @param messageDelayer The message delayer to use for handling delayed retries.
     * @return This builder for chaining.
     */
    fun withMessageDelayer(messageDelayer: MessageDelayer) = apply {
        this.messageDelayer = messageDelayer
    }

    /**
     * Disables message delaying entirely.
     *
     * Messages will be processed immediately without any delay, even if delay headers are present.
     *
     * @return This builder for chaining.
     */
    fun withoutDelay() = apply {
        this.messageDelayer = NoMessageDelay
    }

    /**
     * Sets a custom retry policy provider.
     *
     * The provider will be used as a fallback after checking topic configuration's retry strategy policies.
     * Uses [createCompositeRetryPolicyProvider] to maintain standard resolution order.
     *
     * @param policyProvider Function that determines the retry policy for each failure.
     * @return This builder for chaining.
     */
    fun withPolicyProvider(policyProvider: RetryPolicyProvider) = apply {
        this.retryPolicyProvider = createCompositeRetryPolicyProvider(
            fallbackProvider = { message, consumerContext, exceptionDetails, topicConfiguration ->
                policyProvider(message, consumerContext, exceptionDetails, topicConfiguration)
            }
        )
    }

    /**
     * Configures retry policies using a registry of conditional policies.
     *
     * Policies are evaluated in the order provided. The first matching policy is used.
     *
     * @param conditionalPolicies The conditional policies to register.
     * @param defaultPolicy The fallback policy when no conditional policies match.
     * @return This builder for chaining.
     */
    fun withPolicies(vararg conditionalPolicies: ConditionalRetryPolicy, defaultPolicy: RetryPolicy = DefaultPolicy) = apply {
        this.retryPolicyProvider = ConditionalRetryPolicyRegistry(defaultPolicy)
            .apply {
                conditionalPolicies.forEach { this.addPolicy(it) }
            }
    }

    /**
     * Sets a custom outgoing message modifier.
     *
     * The modifier can transform messages before they are sent to retry or error topics.
     *
     * @param delegate Function to modify outgoing messages.
     * @return This builder for chaining.
     */
    fun withOutgoingMessageModifier(delegate: OutgoingMessageModifierDelegate<TKey, TValue>) = apply {
        this.outgoingMessageModifierDelegate = delegate
    }

    /**
     * A no-op message delayer that performs no delays.
     */
    object NoMessageDelay : MessageDelayer() {
        /**
         * No-op delay implementation.
         */
        override suspend fun delay(duration: Duration) {
            return
        }

        /**
         * No-op conditional delay implementation.
         */
        override suspend fun delayIfNeeded(
            message: IncomingMessage<*, *>,
            consumerContext: ConsumerContext,
            defaultDelayOptions: DelayOptions
        ) {
            return
        }
    }

    /**
     * Builds the [RecoverableMessageExecutor] with all configured settings.
     *
     * @return A fully configured [RecoverableMessageExecutor] instance.
     */
    fun build(): RecoverableMessageExecutor<TKey, TValue> {
        val failedMessageRouter = FailedMessageRouter(
            exceptionDetailsProvider = exceptionDetailsProvider,
            topicResolver = topicResolver,
            danglingTopic = danglingTopic,
            quafkaProducer = producer,
            policyProvider = retryPolicyProvider,
            outgoingMessageModifier = outgoingMessageModifierDelegate
        )

        val recoverableMessageExecutor = RecoverableMessageExecutor(
            policyProvider = retryPolicyProvider,
            exceptionDetailsProvider = exceptionDetailsProvider,
            failedMessageRouter = failedMessageRouter,
            messageDelayer = messageDelayer,
            topicResolver = topicResolver
        )
        return recoverableMessageExecutor
    }
}
