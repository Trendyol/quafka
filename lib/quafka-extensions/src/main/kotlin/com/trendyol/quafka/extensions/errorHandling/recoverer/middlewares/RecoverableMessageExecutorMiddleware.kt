package com.trendyol.quafka.extensions.errorHandling.recoverer.middlewares

import com.trendyol.quafka.extensions.common.pipelines.MiddlewareFn
import com.trendyol.quafka.extensions.consumer.single.pipelines.*
import com.trendyol.quafka.extensions.errorHandling.recoverer.RecoverableMessageExecutor

class RecoverableMessageExecutorMiddleware<TEnvelope, TKey, TValue>(
    private val recoverableMessageExecutor: RecoverableMessageExecutor<TKey, TValue>
) : SingleMessageBaseMiddleware<TEnvelope, TKey, TValue>() where TEnvelope : SingleMessageEnvelope<TKey, TValue> {
    override suspend fun execute(
        envelope: TEnvelope,
        next: MiddlewareFn<TEnvelope>
    ) {
        recoverableMessageExecutor.execute(envelope.message, envelope.consumerContext) {
            next(envelope)
        }
    }
}
