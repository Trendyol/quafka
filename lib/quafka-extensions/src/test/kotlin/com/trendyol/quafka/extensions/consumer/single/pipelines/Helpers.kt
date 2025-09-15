package com.trendyol.quafka.extensions.consumer.single.pipelines

import com.trendyol.quafka.*
import com.trendyol.quafka.common.TimeProvider
import com.trendyol.quafka.consumer.*

suspend fun <TKey, TValue> IncomingMessageBuilder<TKey, TValue>.buildSingleIncomingMessageContext(
    timeProvider: TimeProvider = FakeTimeProvider.default()
): SingleMessageEnvelope<TKey, TValue> {
    val incomingMessage = this.build()
    return buildSingleIncomingMessageContext(incomingMessage, timeProvider)
}

suspend fun <TKey, TValue> buildSingleIncomingMessageContext(
    incomingMessage: IncomingMessage<TKey, TValue>,
    timeProvider: TimeProvider = FakeTimeProvider.default()
): SingleMessageEnvelope<TKey, TValue> = SingleMessageEnvelope(
    message = incomingMessage,
    consumerContext = incomingMessage.buildConsumerContext(timeProvider = timeProvider)
)
