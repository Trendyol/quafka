package com.trendyol.quafka.extensions.errorHandling.recoverer

import com.trendyol.quafka.common.rethrowIfFatalOrCancelled
import com.trendyol.quafka.consumer.*
import com.trendyol.quafka.extensions.delaying.MessageDelayer
import com.trendyol.quafka.logging.*
import io.github.resilience4j.kotlin.retry.executeSuspendFunction
import org.slf4j.Logger
import org.slf4j.event.Level

/**
 * Executes message processing with comprehensive error recovery capabilities.
 *
 * This class is the main entry point for handling message processing with retry logic.
 * It coordinates:
 * - Delaying messages based on configured delays
 * - In-memory retry attempts using Resilience4j
 * - Routing failed messages to appropriate retry or error topics
 *
 * @param TKey The type of the message key.
 * @param TValue The type of the message value.
 * @property policyProvider Determines the retry policy for each failure.
 * @property exceptionDetailsProvider Extracts structured details from exceptions.
 * @property failedMessageRouter Routes failed messages to retry or error topics.
 * @property messageDelayer Handles delayed message processing.
 * @property topicResolver Resolves topic configurations for incoming messages.
 */
class RecoverableMessageExecutor<TKey, TValue>(
    private val policyProvider: RetryPolicyProvider,
    private val exceptionDetailsProvider: ExceptionDetailsProvider,
    private val failedMessageRouter: FailedMessageRouter<TKey, TValue>,
    private val messageDelayer: MessageDelayer,
    private val topicResolver: TopicResolver
) {
    private val logger: Logger = LoggerHelper.createLogger(clazz = RecoverableMessageExecutor::class.java)

    /**
     * Executes the given block with full error recovery support.
     *
     * The execution flow:
     * 1. Delays the message if needed (based on delay headers)
     * 2. Executes the block with in-memory retry attempts
     * 3. If all in-memory retries fail, routes the message to appropriate retry/error topics
     *
     * @param incomingMessage The message to process.
     * @param consumerContext The consumer context.
     * @param block The processing logic to execute (your message handler).
     */
    suspend fun execute(
        incomingMessage: IncomingMessage<TKey, TValue>,
        consumerContext: ConsumerContext,
        block: suspend () -> Unit
    ) {
        messageDelayer.delayIfNeeded(incomingMessage, consumerContext)
        val result = executeWithInMemoryRetries(incomingMessage, consumerContext, block)
        result.exceptionOrNull()?.let { ex ->
            failedMessageRouter.handle(incomingMessage, consumerContext, ex)
        }
    }

    private suspend fun executeWithInMemoryRetries(
        incomingMessage: IncomingMessage<TKey, TValue>,
        consumerContext: ConsumerContext,
        block: suspend () -> Unit
    ): Result<Unit> =
        try {
            block()
            Result.success(Unit)
        } catch (ex: Throwable) {
            ex.rethrowIfFatalOrCancelled()
            handleException(incomingMessage, consumerContext, ex, block)
        }

    private suspend fun handleException(
        incomingMessage: IncomingMessage<TKey, TValue>,
        consumerContext: ConsumerContext,
        exception: Throwable,
        block: suspend () -> Unit
    ): Result<Unit> {
        val exceptionDetails = exceptionDetailsProvider(exception)

        return when (val resolvedTopic = topicResolver.resolve(incomingMessage)) {
            is TopicResolver.Topic.ResolvedTopic ->
                handleKnownTopic(
                    resolvedTopic.configuration,
                    incomingMessage,
                    consumerContext,
                    exceptionDetails,
                    exception,
                    block
                )

            TopicResolver.Topic.UnknownTopic ->
                Result.failure(exception)
        }
    }

    private suspend fun handleKnownTopic(
        topicConfiguration: TopicConfiguration<*>,
        incomingMessage: IncomingMessage<TKey, TValue>,
        consumerContext: ConsumerContext,
        exceptionDetails: ExceptionDetails,
        exception: Throwable,
        block: suspend () -> Unit
    ): Result<Unit> {
        val policy =
            policyProvider(incomingMessage, consumerContext, exceptionDetails, topicConfiguration)

        return when (policy) {
            is RetryPolicy.FullRetry ->
                executeInMemoryRetryIfNeeded(
                    incomingMessage,
                    consumerContext,
                    exception,
                    policy.identifier,
                    policy.inMemoryConfig,
                    block
                )

            is RetryPolicy.InMemory ->
                executeInMemoryRetryIfNeeded(
                    incomingMessage,
                    consumerContext,
                    exception,
                    policy.identifier,
                    policy.config,
                    block
                )

            RetryPolicy.NoRetry ->
                Result.failure(exception)

            is RetryPolicy.NonBlocking ->
                Result.failure(exception)
        }
    }

    private suspend fun executeInMemoryRetryIfNeeded(
        incomingMessage: IncomingMessage<TKey, TValue>,
        consumerContext: ConsumerContext,
        exception: Throwable,
        identifier: PolicyIdentifier,
        retryConfig: InMemoryRetryConfig,
        block: suspend () -> Unit
    ): Result<Unit> {
        val identifierInHeader = incomingMessage.headers.getRetryPolicyIdentifier()

        // already in non-blocking retry loop, skip in-memory retry
        if (identifierInHeader == identifier) {
            if (logger.isDebugEnabled) {
                logger
                    .atDebug()
                    .enrichWithConsumerContext(consumerContext)
                    .log(
                        "Skipping in-memory retry for identifier '$identifier', " +
                            "already in non-blocking retry loop. | message = {} ",
                        incomingMessage.toString(
                            Level.DEBUG,
                            addValue = false,
                            addHeaders = true
                        )
                    )
            }
            return Result.failure(exception)
        }

        return try {
            retryConfig.retry.executeSuspendFunction {
                block()
            }
            Result.success(Unit)
        } catch (ex: Throwable) {
            Result.failure(ex)
        }
    }
}
