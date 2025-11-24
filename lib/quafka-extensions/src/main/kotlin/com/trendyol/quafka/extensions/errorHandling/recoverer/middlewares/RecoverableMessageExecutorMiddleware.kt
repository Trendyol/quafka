package com.trendyol.quafka.extensions.errorHandling.recoverer.middlewares

import com.trendyol.quafka.extensions.common.pipelines.MiddlewareFn
import com.trendyol.quafka.extensions.consumer.single.pipelines.*
import com.trendyol.quafka.extensions.errorHandling.recoverer.RecoverableMessageExecutor

/**
 * Middleware that wraps message processing with recoverable error handling.
 *
 * This middleware intercepts message processing and delegates to a [RecoverableMessageExecutor]
 * to provide retry and error recovery capabilities. Any exceptions thrown during message
 * processing will be caught and handled according to configured retry policies.
 *
 * @param TEnvelope The envelope type containing the message and context.
 * @param TKey The type of the message key.
 * @param TValue The type of the message value.
 * @property recoverableMessageExecutor The executor that handles retry logic and error routing.
 */
class RecoverableMessageExecutorMiddleware<TEnvelope, TKey, TValue>(
    private val recoverableMessageExecutor: RecoverableMessageExecutor<TKey, TValue>
) : SingleMessageBaseMiddleware<TEnvelope, TKey, TValue>() where TEnvelope : SingleMessageEnvelope<TKey, TValue> {
    /**
     * Executes the middleware, wrapping the next handler with error recovery.
     *
     * @param envelope The message envelope containing message and context.
     * @param next The next middleware/handler in the pipeline.
     */
    override suspend fun execute(
        envelope: TEnvelope,
        next: MiddlewareFn<TEnvelope>
    ) {
        recoverableMessageExecutor.execute(envelope.message, envelope.consumerContext) {
            next(envelope)
        }
    }
}
