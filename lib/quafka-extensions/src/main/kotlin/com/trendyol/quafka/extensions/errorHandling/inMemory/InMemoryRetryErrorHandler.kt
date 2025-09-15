package com.trendyol.quafka.extensions.errorHandling.inMemory

import com.trendyol.quafka.logging.LoggerHelper
import io.github.resilience4j.core.IntervalFunction
import io.github.resilience4j.kotlin.retry.executeSuspendFunction
import io.github.resilience4j.retry.Retry
import io.github.resilience4j.retry.RetryConfig
import org.slf4j.Logger
import java.time.Duration

class InMemoryRetryErrorHandler<TKey, TValue> private constructor(
    private val config: RetryConfig
) {
    companion object {
        val logger: Logger = LoggerHelper.createLogger(InMemoryRetryErrorHandler::class.java)

        fun <TKey, TValue> new(retryConfig: RetryConfig): InMemoryRetryErrorHandler<TKey, TValue> =
            InMemoryRetryErrorHandler(retryConfig)

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

    suspend fun <T> execute(action: suspend () -> T): T = defaultRetry.executeSuspendFunction {
        action()
    }
}
