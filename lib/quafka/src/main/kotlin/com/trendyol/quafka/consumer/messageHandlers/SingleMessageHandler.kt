package com.trendyol.quafka.consumer.messageHandlers

import com.trendyol.quafka.consumer.*

/**
 * A suspend function type alias for processing a single incoming message.
 *
 * This handler processes an individual message consumed from a Kafka topic.
 * It receives both the message and the consumer's runtime context for processing.
 *
 * @param TKey The type of the message key.
 * @param TValue The type of the message value.
 * @param incomingMessage The message to be processed, including its key, value, and metadata.
 * @param consumerContext The runtime context of the consumer, providing details such as the coroutine scope,
 *                        partition information, and other operational metadata.
 *
 * @throws Exception If the message processing fails. Exceptions should be managed by the caller
 *                   or by a higher-level error handling mechanism.
 *
 * @see BatchMessageHandler For processing multiple messages in a batch.
 * @see ConsumerContext For details about the runtime context provided to the handler.
 *
 * Example usage:
 * ```kotlin
 * val handler: SingleMessageHandler<String, Event> = { message, context ->
 *     logger.info("Processing message: ${message.value}")
 *     // Business logic here
 * }
 * ```
 */
typealias SingleMessageHandler<TKey, TValue> = suspend (
    incomingMessage: IncomingMessage<TKey, TValue>,
    consumerContext: ConsumerContext
) -> Unit
