package com.trendyol.quafka.consumer.messageHandlers

import com.trendyol.quafka.consumer.*
import com.trendyol.quafka.consumer.errorHandlers.FallbackErrorHandler
import com.trendyol.quafka.logging.*
import kotlinx.coroutines.ensureActive
import org.slf4j.Logger
import org.slf4j.event.Level
import kotlin.time.Duration
import kotlin.time.Duration.Companion.seconds

class BatchMessageHandlingStrategy<TKey, TValue>(
    private val messageHandler: BatchMessageHandler<TKey, TValue>,
    batchSize: Int,
    batchTimeout: Duration,
    private val autoAck: Boolean,
    private val fallbackErrorHandler: FallbackErrorHandler<TKey, TValue> = FallbackErrorHandler.RetryOnFailure()
) : MessageHandlingStrategy<TKey, TValue>,
    BatchMessageHandler<TKey, TValue> {
    private val logger: Logger = LoggerHelper.createLogger(clazz = this.javaClass)

    internal var batchSize: Int = batchSize
        private set

    internal var batchTimeout: Duration = batchTimeout
        private set

    companion object {
        val DEFAULT_BATCH_SIZE = 20
        val DEFAULT_BATCH_TIMEOUT: Duration = 5.seconds
    }

    /**
     * Updates batch processing options.
     *
     * @param batchSize The size of each batch. Must be greater than 0.
     * @param batchTimeout The maximum duration to wait for a full batch.
     * @return The current instance of [BatchMessageHandlingStrategy].
     */
    fun withBatchOptions(batchSize: Int = DEFAULT_BATCH_SIZE, batchTimeout: Duration = DEFAULT_BATCH_TIMEOUT) = apply {
        this.batchSize = batchSize
        this.batchTimeout = batchTimeout
    }

    /**
     * Processes a batch of messages using the provided batch [incomingMessages].
     *
     * If an error occurs during processing, the error handler is invoked. If auto-acknowledgment is enabled,
     * all messages in the batch are acknowledged after processing.
     *
     * @param incomingMessages The collection of messages to process.
     */
    override suspend fun invoke(
        incomingMessages: Collection<IncomingMessage<TKey, TValue>>,
        consumerContext: ConsumerContext
    ) {
        this.fallbackErrorHandler.handle(
            consumerContext,
            incomingMessages
        ) { _, _ ->
            try {
                messageHandler.invoke(incomingMessages, consumerContext)
            } catch (exception: Throwable) {
                consumerContext.coroutineContext.ensureActive()
                logger
                    .atWarn()
                    .enrichWithConsumerContext(consumerContext)
                    .setCause(exception)
                    .log(
                        "Error occurred when processing batch messages, error will be handled... | {}",
                        incomingMessages.map { it.toString(logLevel = Level.WARN, addValue = false, addHeaders = true) }
                    )

                throw exception
            }

            if (autoAck) {
                incomingMessages.ackAll()
            }
        }
    }
}
