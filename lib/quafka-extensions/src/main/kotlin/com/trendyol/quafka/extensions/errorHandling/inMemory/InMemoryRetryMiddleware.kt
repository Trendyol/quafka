package com.trendyol.quafka.extensions.errorHandling.inMemory

import com.trendyol.quafka.extensions.common.pipelines.MiddlewareFn
import com.trendyol.quafka.extensions.consumer.single.pipelines.*

/**
 * Middleware that wraps message processing with in-memory retry logic.
 *
 * This is a simpler alternative to [InMemoryRetryMiddleware] that only
 * provides in-memory retries without Kafka-based retry topics or error routing.
 *
 * If the message handler throws an exception, the middleware will retry the operation
 * according to the configured retry policy. If all retries are exhausted, the exception
 * is propagated.
 *
 * @param TEnvelope The envelope type containing the message and context.
 * @param TKey The type of the message key.
 * @param TValue The type of the message value.
 * @property inMemoryRetryHandler The handler that performs the retry logic.
 */
class InMemoryRetryMiddleware<TEnvelope, TKey, TValue>(val inMemoryRetryHandler: InMemoryRetryErrorHandler<TKey, TValue>) :
    SingleMessageBaseMiddleware<TEnvelope, TKey, TValue>() where TEnvelope : SingleMessageEnvelope<TKey, TValue> {
    /**
     * Executes the middleware, wrapping the next handler with retry logic.
     *
     * @param envelope The message envelope containing message and context.
     * @param next The next middleware/handler in the pipeline.
     */
    override suspend fun execute(
        envelope: TEnvelope,
        next: MiddlewareFn<TEnvelope>
    ) {
        inMemoryRetryHandler.execute {
            next(envelope)
        }
    }
}
