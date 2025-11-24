package com.trendyol.quafka.consumer.messageHandlers

import com.trendyol.quafka.common.isFatal
import com.trendyol.quafka.common.rethrowIfFatalOrCancelled
import com.trendyol.quafka.consumer.ConsumerContext
import com.trendyol.quafka.consumer.Events
import com.trendyol.quafka.consumer.Events.toDetail
import com.trendyol.quafka.consumer.IncomingMessage
import com.trendyol.quafka.consumer.ackAll
import com.trendyol.quafka.logging.LoggerHelper
import com.trendyol.quafka.logging.enrichWithConsumerContext
import io.github.resilience4j.core.IntervalFunction
import io.github.resilience4j.kotlin.retry.executeSuspendFunction
import io.github.resilience4j.retry.Retry
import io.github.resilience4j.retry.RetryConfig
import org.slf4j.Logger
import org.slf4j.event.Level
import kotlin.time.Duration
import kotlin.time.Duration.Companion.minutes

/**
 * Defines a resilient message handling strategy for Kafka message processing.
 * This interface provides a mechanism to execute message handlers with different resilience strategies
 * like retry or skip-and-log.
 *
 * The resilient handler wraps the actual message handler execution and controls how errors are managed.
 * It's the last line of defense in error handling, activated when primary error handling mechanisms
 * have failed or are not sufficient.
 *
 * @param TKey The type of the message key (e.g., String, Int, Custom Type)
 * @param TValue The type of the message value (e.g., String, ByteArray, Custom Type)
 */
sealed class ResilientHandler<TKey, TValue> {
    protected val logger: Logger =
        LoggerHelper.createLogger(
            clazz = ResilientHandler::class.java
        )

    /**
     * Handles the processing of messages with a resilient strategy when errors occur.
     * This method implements the specific error handling behavior defined by each implementation.
     *
     * @param context The consumer context containing metadata about the consumer, including
     *                consumer group, client id, and other configuration details
     * @param incomingMessages Collection of messages to be processed. These messages may contain
     *                        both the raw Kafka message and additional metadata
     * @param handler The message processing function that might throw an exception. This is typically
     *               the original message handler that failed and needs resilient handling
     * @throws Throwable If the resilient handler itself encounters an unrecoverable error or
     *                  if the error is considered fatal
     */
    abstract suspend fun handle(
        context: ConsumerContext,
        incomingMessages: Collection<IncomingMessage<TKey, TValue>>,
        handler: suspend (
            context: ConsumerContext,
            incomingMessages: Collection<IncomingMessage<TKey, TValue>>
        ) -> Unit
    )

    /**
     * Implementation of [ResilientHandler] that retries failed operations with exponential backoff.
     * This handler will continuously retry non-fatal errors with an exponentially increasing delay between attempts.
     *
     * @param TKey The type of the message key
     * @param TValue The type of the message value
     * @property retryConfig The retry configuration to use
     */
    class Retrying<TKey, TValue> private constructor(private val retryConfig: RetryConfig) : ResilientHandler<TKey, TValue>() {
        private val retry = Retry.of("handlerRetry", retryConfig)

        override suspend fun handle(
            context: ConsumerContext,
            incomingMessages: Collection<IncomingMessage<TKey, TValue>>,
            handler: suspend (
                context: ConsumerContext,
                incomingMessages: Collection<IncomingMessage<TKey, TValue>>
            ) -> Unit
        ) {
            retry.executeSuspendFunction {
                handler(context, incomingMessages)
            }
        }

        companion object {
            /**
             * Creates a basic retrying handler with custom retry configuration.
             *
             * This allows full control over retry behavior including max attempts,
             * backoff strategy, and retry predicates.
             *
             * Example:
             * ```kotlin
             * val handler = ResilientHandler.Retrying.basic<String, String>(
             *     initialInterval = 30.seconds,
             *     maxAttempts = 5,
             *     multiplier = 2.0
             * )
             * ```
             *
             * @param initialInterval The initial delay duration between retry attempts
             * @param maxAttempts Maximum number of retry attempts (default: Int.MAX_VALUE)
             * @param multiplier The multiplier for exponential backoff (default: 2.0)
             * @param randomizationFactor Randomization factor for backoff (default: 0.5)
             * @return A new Retrying handler instance
             */
            fun <TKey, TValue> basic(
                initialInterval: Duration = 1.minutes,
                maxAttempts: Int = Int.MAX_VALUE,
                multiplier: Double = 2.0,
                randomizationFactor: Double = 0.5
            ): Retrying<TKey, TValue> {
                val config = RetryConfig
                    .custom<String>()
                    .maxAttempts(maxAttempts)
                    .retryOnException { !it.isFatal() }
                    .intervalFunction(
                        IntervalFunction.ofExponentialRandomBackoff(
                            initialInterval.inWholeMilliseconds,
                            multiplier,
                            randomizationFactor
                        )
                    ).failAfterMaxAttempts(true)
                    .build()

                return Retrying(config)
            }
        }
    }

    /**
     * Implementation of [ResilientHandler] that logs errors and skips failed messages.
     * When a non-fatal error occurs, this handler will log the error details and acknowledge the messages
     * to prevent them from being reprocessed.
     *
     * @param TKey The type of the message key
     * @param TValue The type of the message value
     */
    class Skipping<TKey, TValue> : ResilientHandler<TKey, TValue>() {
        override suspend fun handle(
            context: ConsumerContext,
            incomingMessages: Collection<IncomingMessage<TKey, TValue>>,
            handler: suspend (
                context: ConsumerContext,
                incomingMessages: Collection<IncomingMessage<TKey, TValue>>
            ) -> Unit
        ) {
            try {
                handler(context, incomingMessages)
            } catch (exception: Throwable) {
                exception.rethrowIfFatalOrCancelled()
                logger
                    .atError()
                    .enrichWithConsumerContext(context)
                    .setCause(exception)
                    .log(
                        "Unhandled error occurred when handling error, messages will be skipped. | {}",
                        incomingMessages.map {
                            it.toString(
                                logLevel = Level.ERROR,
                                addValue = false,
                                addHeaders = false
                            )
                        }
                    )
                incomingMessages.ackAll()
            }
        }
    }

    /**
     * Implementation of [ResilientHandler] that stops the consumer on first error.
     * When any error occurs during message processing, this handler will log the error,
     * publish a WorkerFailed event, and rethrow the exception to stop the consumer.
     *
     * This is useful when you want fail-fast behavior - any processing error should
     * immediately stop the consumer for investigation or manual intervention.
     *
     * @param TKey The type of the message key
     * @param TValue The type of the message value
     */
    class Stopping<TKey, TValue> : ResilientHandler<TKey, TValue>() {
        override suspend fun handle(
            context: ConsumerContext,
            incomingMessages: Collection<IncomingMessage<TKey, TValue>>,
            handler: suspend (
                context: ConsumerContext,
                incomingMessages: Collection<IncomingMessage<TKey, TValue>>
            ) -> Unit
        ) {
            try {
                handler(context, incomingMessages)
            } catch (exception: Throwable) {
                exception.rethrowIfFatalOrCancelled()
                logger
                    .atError()
                    .enrichWithConsumerContext(context)
                    .setCause(exception)
                    .log(
                        "Error occurred during message processing, stopping consumer. | {}",
                        incomingMessages.joinToString {
                            it.toString(
                                logLevel = Level.ERROR,
                                addValue = false,
                                addHeaders = true
                            )
                        }
                    )
                // Publish WorkerFailed event
                context.consumerOptions.eventBus.publish(
                    Events.WorkerFailed(
                        context.topicPartition,
                        exception,
                        context.consumerOptions.toDetail()
                    )
                )
                throw exception
            }
        }
    }

    /**
     * Wraps a user-provided resilient handler with an ultimate safety net.
     * If the user's handler fails, the safety net handler will be used as a last resort.
     *
     * This ensures that even if a custom implementation has bugs or fails unexpectedly,
     * the consumer won't crash and will continue processing messages.
     *
     * @param TKey The type of the message key
     * @param TValue The type of the message value
     * @property userHandler The user's custom resilient handler
     * @property safetyNetHandler The fallback handler to use if userHandler fails (default: Retrying)
     */
    internal class WithSafetyNet<TKey, TValue>(
        private val userHandler: ResilientHandler<TKey, TValue>,
        private val safetyNetHandler: ResilientHandler<TKey, TValue>
    ) : ResilientHandler<TKey, TValue>() {
        override suspend fun handle(
            context: ConsumerContext,
            incomingMessages: Collection<IncomingMessage<TKey, TValue>>,
            handler: suspend (
                context: ConsumerContext,
                incomingMessages: Collection<IncomingMessage<TKey, TValue>>
            ) -> Unit
        ) {
            try {
                // First, try the user's handler
                userHandler.handle(context, incomingMessages, handler)
            } catch (ex: Throwable) {
                ex.rethrowIfFatalOrCancelled()
                logger.error(
                    "User's resilient handler failed, using safety net handler. | {}",
                    incomingMessages.joinToString { it.toString(Level.ERROR, addValue = true, addHeaders = true) },
                    ex
                )
                // If user's handler fails, safety net takes over
                safetyNetHandler.handle(context, incomingMessages, handler)
            }
        }
    }
}
