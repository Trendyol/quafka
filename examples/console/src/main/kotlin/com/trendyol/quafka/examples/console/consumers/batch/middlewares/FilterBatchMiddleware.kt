package com.trendyol.quafka.examples.console.consumers.batch.middlewares

import com.trendyol.quafka.consumer.IncomingMessage
import com.trendyol.quafka.extensions.consumer.batch.pipelines.*

/**
 * Middleware that filters messages in a batch based on a predicate.
 * Only messages that satisfy the predicate will be passed to the next middleware.
 */
class FilterBatchMiddleware<TKey, TValue>(private val predicate: suspend (IncomingMessage<TKey, TValue>) -> Boolean) :
    BatchMessageBaseMiddleware<BatchMessageEnvelope<TKey, TValue>, TKey, TValue>() {

    override suspend fun execute(
        envelope: BatchMessageEnvelope<TKey, TValue>,
        next: suspend (BatchMessageEnvelope<TKey, TValue>) -> Unit
    ) {
        val filteredMessages = envelope.messages.filter { predicate(it) }

        if (filteredMessages.isNotEmpty()) {
            val filteredEnvelope = BatchMessageEnvelope(
                filteredMessages,
                envelope.consumerContext,
                envelope.attributes
            )
            next(filteredEnvelope)
        }
    }
}
