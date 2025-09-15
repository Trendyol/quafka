package com.trendyol.quafka.consumer.messageHandlers

import com.trendyol.quafka.consumer.*

/**
 * A suspend function type alias for processing a batch of incoming messages.
 *
 * This handler processes a collection of messages consumed from a Kafka topic in a single invocation.
 * Batch processing can optimize throughput by reducing the per-message processing overhead.
 *
 * @param TKey The type of the message key.
 * @param TValue The type of the message value.
 * @param incomingMessages A collection of messages to be processed. Each message contains its key, value, and metadata.
 * @param consumerContext The runtime context of the consumer, which includes the coroutine scope,
 *                        partition details, and other operational information.
 *
 * @throws Exception If batch processing fails. Exceptions should be managed by the caller
 *                   or by a higher-level error handling mechanism.
 *
 * @see SingleMessageHandler For processing individual messages.
 * @see ConsumerContext For details about the runtime context provided to the handler.
 *
 * Example usage:
 * ```kotlin
 * val batchHandler: BatchMessageHandler<String, String> = { messages, context ->
 *     logger.info("Processing batch of ${messages.size} messages")
 *     // Batch processing logic here
 * }
 * ```
 */
typealias BatchMessageHandler<TKey, TValue> = suspend (
    incomingMessages: Collection<IncomingMessage<TKey, TValue>>,
    consumerContext: ConsumerContext
) -> Unit
