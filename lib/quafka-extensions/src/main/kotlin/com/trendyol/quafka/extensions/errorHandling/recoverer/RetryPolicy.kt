package com.trendyol.quafka.extensions.errorHandling.recoverer

import com.trendyol.quafka.consumer.*
import io.github.resilience4j.core.IntervalFunction
import io.github.resilience4j.retry.*
import kotlin.time.*
import kotlin.time.Duration.Companion.milliseconds

/**
 * Defines the complete retry strategy for a given exception type using a type-safe sealed class.
 * This approach avoids nullable properties and makes the policy explicit.
 *
 * ## Important Design Decision:
 * **maxOverallAttempts stays in TopicRetryStrategy** (not in RetryPolicy).
 * This is critical for preventing infinite retry loops when exception types change between attempts.
 * When a different exception is thrown, the policy identifier changes, resetting the per-policy
 * attempt counter. However, the overall attempt counter (tracked by TopicRetryStrategy) never
 * resets, providing a safety net against infinite loops.
 */
sealed class RetryPolicy {
    /**
     * A policy indicating that no retries should be performed. The message should fail immediately.
     */
    data object NoRetry : RetryPolicy()

    /**
     * A policy for performing only in-memory retries. If these fail, the process stops.
     * @property identifier A unique name to track this retry session.
     * @property config The configuration for the in-memory retry stage.
     */
    data class InMemory(val identifier: PolicyIdentifier, val config: InMemoryRetryConfig) : RetryPolicy()

    /**
     * A policy for performing only Kafka-based (asynchronous) retries.
     * @property identifier A unique name to track this retry session.
     * @property config The configuration for the Kafka-based retry stage.
     */
    data class NonBlocking(val identifier: PolicyIdentifier, val config: NonBlockingRetryConfig) : RetryPolicy()

    /**
     * A policy that defines a full, two-stage retry mechanism: first in-memory, then Kafka-based.
     * @property identifier A unique name to track this retry session.
     * @property inMemoryConfig The configuration for the in-memory stage.
     * @property nonBlockingConfig The configuration for the Kafka-based stage.
     */
    data class FullRetry(
        val identifier: PolicyIdentifier,
        val inMemoryConfig: InMemoryRetryConfig,
        val nonBlockingConfig: NonBlockingRetryConfig
    ) : RetryPolicy()
}

/**
 * Builder classes for constructing [RetryPolicy] instances.
 *
 * Each builder corresponds to a specific retry policy type and accumulates configuration
 * before converting to the final policy via [toPolicy].
 */
sealed class RetryPolicyBuilder {

    /**
     * Builder for in-memory only retry policies.
     *
     * @property config The in-memory retry configuration, or null if not set.
     */
    data class InMemory(var config: InMemoryRetryConfig? = null) : RetryPolicyBuilder()

    /**
     * Builder for non-blocking (Kafka-based) only retry policies.
     *
     * @property config The non-blocking retry configuration, or null if not set.
     */
    data class NonBlocking(var config: NonBlockingRetryConfig? = null) : RetryPolicyBuilder()

    /**
     * Builder for full retry policies that support both in-memory and non-blocking retries.
     *
     * @property inMemoryConfig The in-memory retry configuration, or null if not set.
     * @property nonBlockingConfig The non-blocking retry configuration, or null if not set.
     */
    data class Full(var inMemoryConfig: InMemoryRetryConfig? = null, var nonBlockingConfig: NonBlockingRetryConfig? = null) :
        RetryPolicyBuilder() {
        fun dontRetry() {
            inMemoryConfig = null
            nonBlockingConfig = null
        }
    }
}

internal fun RetryPolicyBuilder.toPolicy(identifier: PolicyIdentifier): RetryPolicy =
    when (this) {
        is RetryPolicyBuilder.Full -> when {
            inMemoryConfig != null && nonBlockingConfig != null ->
                RetryPolicy.FullRetry(identifier, inMemoryConfig!!, nonBlockingConfig!!)

            inMemoryConfig != null ->
                RetryPolicy.InMemory(identifier, inMemoryConfig!!)

            nonBlockingConfig != null ->
                RetryPolicy.NonBlocking(identifier, nonBlockingConfig!!)

            else ->
                RetryPolicy.NoRetry
        }

        is RetryPolicyBuilder.InMemory ->
            config?.let { RetryPolicy.InMemory(identifier, it) }
                ?: RetryPolicy.NoRetry

        is RetryPolicyBuilder.NonBlocking ->
            config?.let { RetryPolicy.NonBlocking(identifier, it) }
                ?: RetryPolicy.NoRetry
    }

/**
 * A function type that provides a [RetryPolicy] based on the message, context, and exception.
 *
 * This is the primary extension point for implementing custom retry logic.
 * Given a failed message and its exception, this function should return the appropriate
 * retry policy to apply.
 */
typealias RetryPolicyProvider = suspend (
    message: IncomingMessage<*, *>,
    consumerContext: ConsumerContext,
    exceptionDetails: ExceptionDetails,
    topicConfiguration: TopicConfiguration<*>
) -> RetryPolicy

/**
 * A function type that filters messages based on custom criteria.
 *
 * Returns `true` if a policy should apply to the given message/exception combination.
 */
typealias PolicyFilter = suspend (
    message: IncomingMessage<*, *>,
    consumerContext: ConsumerContext,
    exceptionDetails: ExceptionDetails
) -> Boolean

/**
 * Configuration for in-memory (synchronous/blocking) retries using Resilience4j.
 *
 * In-memory retries occur immediately before a message is sent to Kafka for
 * non-blocking retries. They are suitable for transient errors that may resolve quickly.
 *
 * @property retryConfig The Resilience4j [RetryConfig] used for retry logic.
 */
class InMemoryRetryConfig(internal val retryConfig: RetryConfig) {
    internal val retry = Retry.of("default", retryConfig)

    companion object {
        /**
         * Creates a basic in-memory retry configuration with exponential random backoff.
         *
         * @param maxAttempts The maximum number of retry attempts.
         * @param initialDelay The initial delay before the first retry.
         * @return A configured [InMemoryRetryConfig] instance.
         */
        fun basic(maxAttempts: Int, initialDelay: Duration): InMemoryRetryConfig = InMemoryRetryConfig(
            RetryConfig
                .custom<Any>()
                .maxAttempts(maxAttempts)
                .intervalFunction(
                    IntervalFunction.ofExponentialRandomBackoff(
                        initialDelay.toJavaDuration(),
                        2.0
                    )
                ).failAfterMaxAttempts(true)
                .build()
        )
    }
}

/**
 * A function type that calculates the delay duration for a retry attempt.
 *
 * This function is invoked to determine how long to wait before retrying a failed message.
 * The delay can be based on the message content, exception details, or attempt number.
 *
 * @param message The incoming message that failed processing.
 * @param consumerContext The consumer context.
 * @param exceptionDetails Details about the exception that occurred.
 * @param attempt The current attempt number (1-indexed).
 * @return The calculated delay duration.
 */
typealias DelayFn = (
    message: IncomingMessage<*, *>,
    consumerContext: ConsumerContext,
    exceptionDetails: ExceptionDetails,
    attempt: Int
) -> Duration

/**
 * Configuration for Kafka-based (non-blocking) retries.
 *
 * @property maxAttempts Maximum number of non-blocking retry attempts
 * @property delayCalculator Strategy for calculating retry delays
 */
data class NonBlockingRetryConfig private constructor(val maxAttempts: Int, private val delayFn: DelayFn) {
    companion object {
        /**
         * Creates a non-blocking retry configuration with exponential random backoff.
         *
         * The delay increases exponentially with each attempt, with random jitter to prevent
         * thundering herd problems.
         *
         * @param maxAttempts The maximum number of retry attempts.
         * @param initialDelay The initial delay before the first retry.
         * @param maxDelay The maximum delay cap for retries.
         * @param multiplier The exponential growth multiplier (default: 2.0).
         * @return A configured [NonBlockingRetryConfig] instance.
         */
        fun exponentialRandomBackoff(
            maxAttempts: Int,
            initialDelay: Duration,
            maxDelay: Duration,
            multiplier: Double = 2.0
        ): NonBlockingRetryConfig {
            val intervalFn = IntervalFunction.ofExponentialRandomBackoff(
                initialDelay.inWholeMilliseconds,
                multiplier,
                maxDelay.inWholeMilliseconds
            )
            val delayFn: DelayFn = { _, _, _, attempt ->
                intervalFn.apply(attempt).milliseconds
            }
            return NonBlockingRetryConfig(maxAttempts, delayFn)
        }

        /**
         * Creates a non-blocking retry configuration with a custom delay function.
         *
         * Use this when you need complete control over delay calculation logic.
         *
         * @param maxAttempts The maximum number of retry attempts.
         * @param delayFn A custom function to calculate the delay for each attempt.
         * @return A configured [NonBlockingRetryConfig] instance.
         */
        fun custom(
            maxAttempts: Int,
            delayFn: DelayFn
        ) = NonBlockingRetryConfig(maxAttempts, delayFn)
    }

    /**
     * Calculates the delay for the current retry attempt.
     *
     * @param message The incoming message that failed processing.
     * @param consumerContext The consumer context.
     * @param exceptionDetails Details about the exception that occurred.
     * @param attempt The current attempt number (1-indexed).
     * @return The calculated delay duration.
     */
    fun calculateDelay(
        message: IncomingMessage<*, *>,
        consumerContext: ConsumerContext,
        exceptionDetails: ExceptionDetails,
        attempt: Int
    ): Duration = delayFn(message, consumerContext, exceptionDetails, attempt)
}

internal val DefaultPolicy = RetryPolicy.InMemory(PolicyIdentifier("default"), InMemoryRetryConfig.basic(3, 100.milliseconds))

/**
 * Pairs a condition/filter with a retry policy.
 *
 * The policy is applied only when the filter returns `true` for a given message/exception.
 *
 * @property filter The condition that must be met for this policy to apply.
 * @property policy The retry policy to apply when the filter matches.
 */
data class ConditionalRetryPolicy(val filter: PolicyFilter, val policy: RetryPolicy)

/**
 * A registry that manages multiple conditional retry policies and selects the appropriate
 * policy based on message and exception characteristics.
 *
 * Policies are evaluated in the order they were registered. The first matching policy is used.
 * If no policies match, the [defaultPolicy] is returned.
 *
 * This registry uses [createCompositeRetryPolicyProvider] internally to maintain the standard
 * policy resolution order: topic strategy policies → conditional policies → default policy.
 *
 * @property defaultPolicy The fallback policy to use when no conditional policies match.
 */
class ConditionalRetryPolicyRegistry(private val defaultPolicy: RetryPolicy = DefaultPolicy) : RetryPolicyProvider {
    private val policies = mutableListOf<ConditionalRetryPolicy>()

    /**
     * Registers a conditional retry policy.
     *
     * @param policy The conditional policy to add to the registry.
     */
    fun addPolicy(policy: ConditionalRetryPolicy) {
        policies.add(policy)
    }

    /**
     * Registers a retry policy with a custom filter function.
     *
     * @param filter The condition that must be met for this policy to apply.
     * @param policy The retry policy to apply when the filter matches.
     */
    fun addPolicy(filter: PolicyFilter, policy: RetryPolicy) {
        addPolicy(ConditionalRetryPolicy(filter, policy))
    }

    private val conditionalPolicyProvider: NullableRetryPolicyProvider = { message, consumerContext, exceptionDetails, _ ->
        policies.firstOrNull { it.filter.invoke(message, consumerContext, exceptionDetails) }?.policy
    }

    private val compositeProvider by lazy {
        createCompositeRetryPolicyProvider(
            fallbackProvider = conditionalPolicyProvider,
            defaultPolicy = defaultPolicy
        )
    }

    /**
     * Selects the appropriate retry policy for the given message and exception.
     *
     * Resolution order:
     * 1. Topic configuration's retry strategy policies (via [TopicRetryStrategy.toPolicyProvider])
     * 2. Registered conditional policies (evaluated in registration order)
     * 3. Default policy
     *
     * @param message The incoming message that failed processing.
     * @param consumerContext The consumer context.
     * @param exceptionDetails Details about the exception that occurred.
     * @param topicConfiguration The topic configuration for this message.
     * @return The selected [RetryPolicy].
     */
    override suspend fun invoke(
        message: IncomingMessage<*, *>,
        consumerContext: ConsumerContext,
        exceptionDetails: ExceptionDetails,
        topicConfiguration: TopicConfiguration<*>
    ): RetryPolicy = compositeProvider(message, consumerContext, exceptionDetails, topicConfiguration)
}

/**
 * A function type for providing an optional retry policy.
 *
 * Unlike [RetryPolicyProvider], this can return null to indicate "no policy available",
 * allowing the resolution chain to continue to the next provider.
 */
internal typealias NullableRetryPolicyProvider = suspend (
    message: IncomingMessage<*, *>,
    consumerContext: ConsumerContext,
    exceptionDetails: ExceptionDetails,
    topicConfiguration: TopicConfiguration<*>
) -> RetryPolicy?

/**
 * Creates a composite retry policy provider that checks multiple sources in order.
 *
 * Resolution order:
 * 1. Topic configuration's retry strategy policies (via [TopicRetryStrategy.toPolicyProvider])
 * 2. Fallback provider (if policy from step 1 is null)
 * 3. Default policy (if all previous steps return null)
 *
 * This function encapsulates the standard policy resolution flow used throughout the framework.
 *
 * @param fallbackProvider The secondary policy provider to check if topic strategy doesn't provide a policy. Can return null.
 * @param defaultPolicy The final fallback policy if no other sources provide a policy.
 * @return A [RetryPolicyProvider] that implements the resolution chain.
 */
internal fun createCompositeRetryPolicyProvider(
    fallbackProvider: NullableRetryPolicyProvider? = null,
    defaultPolicy: RetryPolicy = DefaultPolicy
): RetryPolicyProvider = { message, consumerContext, exceptionDetails, topicConfiguration ->
    topicConfiguration.retry.toPolicyProvider(message, consumerContext, exceptionDetails)
        ?: fallbackProvider?.invoke(message, consumerContext, exceptionDetails, topicConfiguration)
        ?: defaultPolicy
}
