package com.trendyol.quafka.consumer.messageHandlers

import com.trendyol.quafka.consumer.*
import com.trendyol.quafka.consumer.errorHandlers.*
import com.trendyol.quafka.logging.*
import kotlinx.coroutines.ensureActive
import org.slf4j.Logger
import org.slf4j.event.Level

class SingleMessageHandlingStrategy<TKey, TValue>(
    private val messageHandler: SingleMessageHandler<TKey, TValue>,
    private val autoAck: Boolean,
    private val fallbackErrorHandler: FallbackErrorHandler<TKey, TValue> = FallbackErrorHandler.RetryOnFailure()
) : MessageHandlingStrategy<TKey, TValue>,
    SingleMessageHandler<TKey, TValue> {
    private val logger: Logger = LoggerHelper.createLogger(clazz = this.javaClass)

    /**
     * Processes a single [incomingMessage] using the provided single message [incomingMessage].
     *
     * If an error occurs during processing, the error handler is invoked. If auto-acknowledgment is enabled,
     * the message is acknowledged after processing.
     *
     * @param incomingMessage The message to process.
     */
    override suspend fun invoke(incomingMessage: IncomingMessage<TKey, TValue>, consumerContext: ConsumerContext) {
        fallbackErrorHandler.handle(
            consumerContext,
            listOf(incomingMessage)
        ) { _, _ ->
            try {
                messageHandler.invoke(incomingMessage, consumerContext)
            } catch (exception: Throwable) {
                consumerContext.coroutineContext.ensureActive()
                logger
                    .atWarn()
                    .enrichWithConsumerContext(consumerContext)
                    .setCause(exception)
                    .log(
                        "Error occurred when processing message, error will be handled... | {}",
                        incomingMessage.toString(logLevel = Level.WARN, addValue = false, addHeaders = true)
                    )
                throw exception
            }

            if (autoAck) {
                incomingMessage.ack()
            }
        }
    }
}
