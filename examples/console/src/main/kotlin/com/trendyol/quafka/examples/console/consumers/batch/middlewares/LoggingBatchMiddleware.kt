package com.trendyol.quafka.examples.console.consumers.batch.middlewares

import com.trendyol.quafka.common.rethrowIfFatalOrCancelled
import com.trendyol.quafka.extensions.consumer.batch.pipelines.*
import org.slf4j.Logger
import org.slf4j.LoggerFactory

/**
 * Middleware that logs batch message processing.
 */
class LoggingBatchMiddleware<TKey, TValue>(private val logger: Logger = LoggerFactory.getLogger(LoggingBatchMiddleware::class.java)) :
    BatchMessageBaseMiddleware<BatchMessageEnvelope<TKey, TValue>, TKey, TValue>() {

    override suspend fun execute(
        envelope: BatchMessageEnvelope<TKey, TValue>,
        next: suspend (BatchMessageEnvelope<TKey, TValue>) -> Unit
    ) {
        logger.info("Starting batch processing: ${envelope.messages.size} messages")
        try {
            next(envelope)
            logger.info("Successfully processed batch of ${envelope.messages.size} messages")
        } catch (e: Throwable) {
            e.rethrowIfFatalOrCancelled()
            logger.error("Error processing batch of ${envelope.messages.size} messages", e)
            throw e
        }
    }
}
