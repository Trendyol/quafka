package com.trendyol.quafka.extensions.errorHandling.inMemory

import com.trendyol.quafka.extensions.common.pipelines.MiddlewareFn
import com.trendyol.quafka.extensions.consumer.single.pipelines.*

class InMemoryRetryMiddleware<TEnvelope, TKey, TValue>(
    val inMemoryRetryHandler: InMemoryRetryErrorHandler<TKey, TValue>
) : SingleMessageBaseMiddleware<TEnvelope, TKey, TValue>() where TEnvelope : SingleMessageEnvelope<TKey, TValue> {
    override suspend fun execute(
        envelope: TEnvelope,
        next: MiddlewareFn<TEnvelope>
    ) {
        inMemoryRetryHandler.execute {
            next(envelope)
        }
    }
}
