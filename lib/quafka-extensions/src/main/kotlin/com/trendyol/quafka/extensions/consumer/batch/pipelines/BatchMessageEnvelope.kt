package com.trendyol.quafka.extensions.consumer.batch.pipelines

import com.trendyol.quafka.consumer.*
import com.trendyol.quafka.extensions.common.pipelines.Attributes

/**
 * Envelope containing a batch of messages for pipeline processing.
 *
 * This envelope wraps a collection of messages along with the consumer context
 * and provides a thread-safe [Attributes] container for sharing data between middleware.
 *
 * **Key Features:**
 * - Immutable message collection
 * - Consumer context for acknowledgments
 * - Shared attributes for middleware communication
 * - Thread-safe attribute access
 *
 * @param TKey The type of the message key.
 * @param TValue The type of the message value.
 * @property messages The collection of incoming messages in this batch.
 * @property consumerContext The consumer context providing acknowledgment capabilities.
 * @property attributes A thread-safe container for arbitrary key-value pairs shared between middleware.
 *
 * @see Attributes
 * @see IncomingMessage
 * @see ConsumerContext
 *
 * @sample
 * ```kotlin
 * // In middleware
 * use { envelope, next ->
 *     // Access messages
 *     logger.info("Processing batch of ${envelope.messages.size} messages")
 *
 *     // Store batch-level metadata
 *     envelope.attributes.put(AttributeKey("batchId"), UUID.randomUUID())
 *
 *     next()
 * }
 * ```
 */
open class BatchMessageEnvelope<TKey, TValue>(
    val messages: Collection<IncomingMessage<TKey, TValue>>,
    val consumerContext: ConsumerContext,
    val attributes: Attributes = Attributes()
)
