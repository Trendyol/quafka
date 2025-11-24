package com.trendyol.quafka.examples.console.consumers.batch.middlewares

import com.trendyol.quafka.extensions.consumer.batch.pipelines.*
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import kotlin.system.measureTimeMillis

/**
 * Middleware that measures the execution time of batch message processing.
 */
class MeasureBatchMiddleware<TKey, TValue> : BatchMessageBaseMiddleware<BatchMessageEnvelope<TKey, TValue>, TKey, TValue>() {
    private val logger: Logger = LoggerFactory.getLogger(this::class.java)

    override suspend fun execute(
        envelope: BatchMessageEnvelope<TKey, TValue>,
        next: suspend (BatchMessageEnvelope<TKey, TValue>) -> Unit
    ) {
        val batchSize = envelope.messages.size
        val elapsed = measureTimeMillis {
            next(envelope)
        }
        logger.info("Batch processing completed: $batchSize messages processed in $elapsed ms (${elapsed / batchSize} ms/message)")
    }
}
