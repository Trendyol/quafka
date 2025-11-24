package com.trendyol.quafka.extensions.errorHandling.inMemory

import com.trendyol.quafka.logging.LoggerHelper
import io.github.resilience4j.core.IntervalFunction
import io.github.resilience4j.kotlin.retry.executeSuspendFunction
import io.github.resilience4j.retry.Retry
import io.github.resilience4j.retry.RetryConfig
import org.slf4j.Logger
import java.time.Duration

/**
 * Handler for performing in-memory retries using Resilience4j.
 *
 * This class provides a simplified API for in-memory retry logic without the full
 * error recovery pipeline. It's useful for simple retry scenarios that don't require
 * Kafka-based retry topics.
 *
 * @param TKey The type of the message key (unused, kept for API consistency).
 * @param TValue The type of the message value (unused, kept for API consistency).
 * @property config The Resilience4j retry configuration.
 */
class InMemoryRetryErrorHandler<TKey, TValue> private constructor(private val config: RetryConfig) {
    companion object {
        val logger: Logger = LoggerHelper.createLogger(InMemoryRetryErrorHandler::class.java)

        /**
         * Creates a new handler with the specified retry configuration.
         *
         * @param TKey The type of the message key.
         * @param TValue The type of the message value.
         * @param retryConfig The Resilience4j retry configuration.
         * @return A new [InMemoryRetryErrorHandler] instance.
         */
        fun <TKey, TValue> new(retryConfig: RetryConfig): InMemoryRetryErrorHandler<TKey, TValue> =
            InMemoryRetryErrorHandler(retryConfig)

        /**
         * Creates a handler with default retry configuration.
         *
         * Default settings:
         * - Maximum 3 attempts
         * - Exponential random backoff starting at 50ms with 2x multiplier
         *
         * @param TKey The type of the message key.
         * @param TValue The type of the message value.
         * @param configure Optional lambda to customize the retry configuration.
         * @return A new [InMemoryRetryErrorHandler] instance.
         */
        fun <TKey, TValue> default(
            configure: (builder: RetryConfig.Builder<RetryConfig>) -> RetryConfig = {
                it.build()
            }
        ): InMemoryRetryErrorHandler<TKey, TValue> =
            InMemoryRetryErrorHandler(
                configure(
                    RetryConfig
                        .custom<RetryConfig>()
                        .maxAttempts(3)
                        .intervalFunction(
                            IntervalFunction.ofExponentialRandomBackoff(
                                // initialInterval =
                                Duration.ofMillis(50),
                                // multiplier =
                                2.0
                            )
                        )
                )
            )
    }

    private val defaultRetry: Retry =
        Retry.of("InMemoryRetryStep", config).let {
            it.eventPublisher.onRetry { event ->
                logger.warn(
                    "an error occurred while handling message, " +
                        "retrying after: ${event.waitInterval}, " +
                        "step: ${event.numberOfRetryAttempts}",
                    event.lastThrowable
                )
            }
            it
        }

    /**
     * Executes the given action with retry logic.
     *
     * If the action throws an exception, it will be retried according to the configured
     * retry policy. Retry events (attempts and delays) are logged at WARN level.
     *
     * @param T The return type of the action.
     * @param action The suspending function to execute with retries.
     * @return The result of the action if successful.
     * @throws Exception If all retry attempts are exhausted.
     */
    suspend fun <T> execute(action: suspend () -> T): T = defaultRetry.executeSuspendFunction {
        action()
    }
}
