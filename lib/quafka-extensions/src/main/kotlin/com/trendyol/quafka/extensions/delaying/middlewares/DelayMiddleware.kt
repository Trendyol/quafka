package com.trendyol.quafka.extensions.delaying.middlewares

import com.trendyol.quafka.extensions.common.pipelines.MiddlewareFn
import com.trendyol.quafka.extensions.consumer.single.pipelines.SingleMessageBaseMiddleware
import com.trendyol.quafka.extensions.consumer.single.pipelines.SingleMessageEnvelope
import com.trendyol.quafka.extensions.delaying.MessageDelayer

class DelayMiddleware<TEnvelope, TKey, TValue>(private val messageDelayer: MessageDelayer) :
    SingleMessageBaseMiddleware<TEnvelope, TKey, TValue>() where TEnvelope : SingleMessageEnvelope<TKey, TValue> {
    override suspend fun execute(
        envelope: TEnvelope,
        next: MiddlewareFn<TEnvelope>
    ) {
        messageDelayer.delayIfNeeded(envelope.message, envelope.consumerContext)
        next(envelope)
    }
}
