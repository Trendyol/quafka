package com.trendyol.quafka.extensions.errorHandling.recoverer

import com.trendyol.quafka.common.rethrowIfFatalOrCancelled
import com.trendyol.quafka.consumer.*
import com.trendyol.quafka.extensions.delaying.MessageDelayer
import com.trendyol.quafka.logging.*
import io.github.resilience4j.kotlin.retry.executeSuspendFunction
import org.slf4j.Logger
import org.slf4j.event.Level

class RecoverableMessageExecutor<TKey, TValue>(
    private val policyProvider: RetryPolicyProvider,
    private val exceptionDetailsProvider: ExceptionDetailsProvider,
    private val failedMessageRouter: FailedMessageRouter<TKey, TValue>,
    private val messageDelayer: MessageDelayer
) {
    private val logger: Logger = LoggerHelper.createLogger(clazz = RecoverableMessageExecutor::class.java)

    suspend fun execute(
        incomingMessage: IncomingMessage<TKey, TValue>,
        consumerContext: ConsumerContext,
        block: suspend () -> Unit
    ) {
        messageDelayer.delayIfNeeded(incomingMessage, consumerContext)
        executeWithInMemoryRetries(incomingMessage, consumerContext, block)
            .onFailure {
                failedMessageRouter.handle(incomingMessage, consumerContext, it)
            }
    }

    private suspend fun executeWithInMemoryRetries(
        incomingMessage: IncomingMessage<TKey, TValue>,
        consumerContext: ConsumerContext,
        block: suspend () -> Unit
    ): Result<Unit> = runCatching {
        block()
    }.onFailure { ex ->
        ex.rethrowIfFatalOrCancelled()
        handleException(incomingMessage, consumerContext, ex, block)
    }

    private suspend fun handleException(
        incomingMessage: IncomingMessage<TKey, TValue>,
        consumerContext: ConsumerContext,
        exception: Throwable,
        block: suspend () -> Unit
    ): Result<Unit> {
        val exceptionReport = exceptionDetailsProvider(exception)
        return when (val policy = policyProvider(incomingMessage, consumerContext, exceptionReport)) {
            is RetryPolicy.FullRetry -> {
                runCatching {
                    retry(
                        incomingMessage,
                        policy.inMemoryConfig,
                        policy.identifier,
                        consumerContext,
                        exception,
                        block
                    )
                }
            }

            is RetryPolicy.InMemoryOnly -> {
                runCatching {
                    retry(incomingMessage, policy.config, policy.identifier, consumerContext, exception, block)
                }
            }

            RetryPolicy.NoRetry -> Result.failure(exception)
            is RetryPolicy.NonBlockingOnly -> Result.failure(exception)
        }
    }

    private suspend fun retry(
        incomingMessage: IncomingMessage<TKey, TValue>,
        retryConfig: InMemoryRetryConfig,
        identifier: String,
        consumerContext: ConsumerContext,
        exception: Throwable,
        block: suspend () -> Unit
    ) {
        val identifierInHeader = incomingMessage.headers.getRetryIdentifier()
        if (identifierInHeader == identifier) {
            logger
                .atDebug()
                .enrichWithConsumerContext(consumerContext)
                .log(
                    "Skipping in-memory retry for identifier '$identifier', already in non-blocking retry loop. | message = {} ",
                    incomingMessage.toString(Level.DEBUG, addValue = false, addHeaders = true)
                )
            throw exception
        }

        retryConfig.retry.executeSuspendFunction {
            block()
        }
    }
}
