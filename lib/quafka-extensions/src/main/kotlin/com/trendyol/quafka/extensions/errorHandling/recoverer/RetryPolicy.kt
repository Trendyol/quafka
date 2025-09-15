package com.trendyol.quafka.extensions.errorHandling.recoverer

import com.trendyol.quafka.consumer.*
import io.github.resilience4j.core.IntervalFunction
import io.github.resilience4j.retry.*
import kotlin.time.*
import kotlin.time.Duration.Companion.milliseconds

/**
 * Defines the complete retry strategy for a given exception type using a type-safe sealed class.
 * This approach avoids nullable properties and makes the policy explicit.
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
    data class InMemoryOnly(
        val identifier: String,
        val config: InMemoryRetryConfig
    ) : RetryPolicy()

    /**
     * A policy for performing only Kafka-based (asynchronous) retries.
     * @property identifier A unique name to track this retry session.
     * @property config The configuration for the Kafka-based retry stage.
     */
    data class NonBlockingOnly(
        val identifier: String,
        val config: NonBlockingRetryConfig
    ) : RetryPolicy()

    /**
     * A policy that defines a full, two-stage retry mechanism: first in-memory, then Kafka-based.
     * @property identifier A unique name to track this retry session.
     * @property inMemoryConfig The configuration for the in-memory stage.
     * @property nonBlockingConfig The configuration for the Kafka-based stage.
     */
    data class FullRetry(
        val identifier: String,
        val inMemoryConfig: InMemoryRetryConfig,
        val nonBlockingConfig: NonBlockingRetryConfig
    ) : RetryPolicy()
}

typealias RetryPolicyProvider = suspend (
    message: IncomingMessage<*, *>,
    consumerContext: ConsumerContext,
    exceptionDetails: ExceptionDetails
) -> RetryPolicy

typealias PolicyFilter = suspend (
    message: IncomingMessage<*, *>,
    consumerContext: ConsumerContext,
    exceptionDetails: ExceptionDetails
) -> Boolean

class InMemoryRetryConfig(
    internal val retry: Retry
) {
    companion object {
        fun basic(maxAttempts: Int, initialDelay: Duration): InMemoryRetryConfig =
            of(
                "basic",
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

        fun of(name: String, retryConfig: RetryConfig): InMemoryRetryConfig = InMemoryRetryConfig(
            Retry.of(name, retryConfig)
        )
    }
}

typealias DelayFn = (
    message: IncomingMessage<*, *>,
    consumerContext: ConsumerContext,
    exceptionDetails: ExceptionDetails,
    attempt: Int
) -> Duration

data class NonBlockingRetryConfig(
    val maxAttempts: Int,
    val delayFn: DelayFn
) {
    companion object {
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

        fun custom(
            maxAttempts: Int,
            delayFn: DelayFn
        ) = NonBlockingRetryConfig(maxAttempts, delayFn)
    }

    fun calculateDelay(
        message: IncomingMessage<*, *>,
        consumerContext: ConsumerContext,
        exceptionDetails: ExceptionDetails,
        attempt: Int
    ): Duration = delayFn(message, consumerContext, exceptionDetails, attempt)
}

val DefaultPolicy = RetryPolicy.InMemoryOnly("", InMemoryRetryConfig.basic(3, 100.milliseconds))

data class ConditionalRetryPolicy(
    val filter: PolicyFilter,
    val policy: RetryPolicy
)

class ConditionalRetryPolicyRegistry(
    private val defaultPolicy: RetryPolicy = DefaultPolicy
) : RetryPolicyProvider {
    private val policies = mutableListOf<ConditionalRetryPolicy>()

    fun addPolicy(policy: ConditionalRetryPolicy) {
        policies.add(policy)
    }

    fun addPolicy(filter: PolicyFilter, policy: RetryPolicy) {
        addPolicy(ConditionalRetryPolicy(filter, policy))
    }

    override suspend fun invoke(
        message: IncomingMessage<*, *>,
        consumerContext: ConsumerContext,
        exceptionDetails: ExceptionDetails
    ): RetryPolicy {
        return policies.firstOrNull { it.filter.invoke(message, consumerContext, exceptionDetails) }?.policy ?: return defaultPolicy
    }
}
