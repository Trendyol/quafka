package com.trendyol.quafka.consumer.errorHandlers

import com.trendyol.quafka.common.*
import com.trendyol.quafka.consumer.*
import com.trendyol.quafka.consumer.Events.toDetail
import com.trendyol.quafka.logging.*
import io.github.resilience4j.core.IntervalFunction
import io.github.resilience4j.kotlin.retry.executeSuspendFunction
import io.github.resilience4j.retry.*
import org.slf4j.Logger
import org.slf4j.event.Level
import kotlin.time.Duration
import kotlin.time.Duration.Companion.minutes

/**
 * Defines a fallback error handling strategy for Kafka message processing.
 * This interface provides a mechanism to handle errors that occur during message processing
 * with different strategies like retry or skip-and-log.
 *
 * The fallback handler is the last line of defense in error handling, activated when primary error
 * handling mechanisms have failed or are not sufficient.
 *
 * @param TKey The type of the message key (e.g., String, Int, Custom Type)
 * @param TValue The type of the message value (e.g., String, ByteArray, Custom Type)
 */
sealed class FallbackErrorHandler<TKey, TValue> {
    protected val logger: Logger =
        LoggerHelper.createLogger(
            clazz = FallbackErrorHandler::class.java
        )

    /**
     * Attempts to handle the processing of messages with a fallback strategy when errors occur.
     * This method implements the specific error handling behavior defined by each implementation.
     *
     * @param context The consumer context containing metadata about the consumer, including
     *                consumer group, client id, and other configuration details
     * @param incomingMessages Collection of messages to be processed. These messages may contain
     *                        both the raw Kafka message and additional metadata
     * @param handler The message processing function that might throw an exception. This is typically
     *               the original message handler that failed and needs fallback handling
     * @throws Throwable If the fallback handler itself encounters an unrecoverable error or
     *                  if the error is considered fatal
     */
    suspend fun handle(
        context: ConsumerContext,
        incomingMessages: Collection<IncomingMessage<TKey, TValue>>,
        handler: suspend (
            context: ConsumerContext,
            incomingMessages: Collection<IncomingMessage<TKey, TValue>>
        ) -> Unit
    ) {
        try {
            tryToHandle(context, incomingMessages, handler)
        } catch (ex: Throwable) {
            ex.rethrowIfFatalOrCancelled()
            logger.error(
                "Fatal error: Message could not be handled, stopping consumer. | {}",
                incomingMessages.joinToString { it.toString(Level.ERROR, addValue = false, addHeaders = true) },
                ex
            )
            context.consumerOptions.eventBus.publish(
                Events.WorkerFailed(
                    context.topicPartition,
                    ex,
                    context.consumerOptions.toDetail()
                )
            )
            throw ex
        }
    }

    protected abstract suspend fun tryToHandle(
        context: ConsumerContext,
        incomingMessages: Collection<IncomingMessage<TKey, TValue>>,
        handler: suspend (
            context: ConsumerContext,
            incomingMessages: Collection<IncomingMessage<TKey, TValue>>
        ) -> Unit
    )

    /**
     * Implementation of [FallbackErrorHandler] that retries failed operations with exponential backoff.
     * This handler will continuously retry non-fatal errors with an exponentially increasing delay between attempts.
     *
     * @param TKey The type of the message key
     * @param TValue The type of the message value
     * @property initialInterval The initial delay duration between retry attempts (default: 1 minute).
     *                          This interval will increase exponentially with each retry attempt.
     */
    class RetryOnFailure<TKey, TValue>(
        private val initialInterval: Duration = 1.minutes
    ) : FallbackErrorHandler<TKey, TValue>() {
        private val retry = Retry.of(
            "handlerRetry",
            RetryConfig
                .custom<String>()
                .maxAttempts(Int.MAX_VALUE)
                .retryOnException {
                    !it.isFatal()
                }.intervalFunction(
                    IntervalFunction.ofExponentialRandomBackoff(
                        this.initialInterval.inWholeMilliseconds,
                        2.0,
                        0.5
                    )
                ).failAfterMaxAttempts(true)
                .build()
        )

        override suspend fun tryToHandle(
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
    }

    /**
     * Implementation of [FallbackErrorHandler] that logs errors and skips failed messages.
     * When a non-fatal error occurs, this handler will log the error details and acknowledge the messages
     * to prevent them from being reprocessed.
     *
     * @param TKey The type of the message key
     * @param TValue The type of the message value
     */
    class LogAndSkipOnFailure<TKey, TValue> : FallbackErrorHandler<TKey, TValue>() {
        override suspend fun tryToHandle(
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
}
