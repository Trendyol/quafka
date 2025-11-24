package com.trendyol.quafka.extensions.consumer.single.pipelines

import com.trendyol.quafka.consumer.*
import com.trendyol.quafka.extensions.common.pipelines.Attributes

/**
 * Envelope containing a single message for pipeline processing.
 *
 * This envelope wraps an incoming message along with the consumer context
 * and provides a thread-safe [Attributes] container for sharing data between middleware.
 *
 * **Key Features:**
 * - Single message access
 * - Consumer context for acknowledgments
 * - Shared attributes for middleware communication
 * - Thread-safe attribute access
 *
 * @param TKey The type of the message key.
 * @param TValue The type of the message value.
 * @property message The incoming message being processed.
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
 *     // Access message
 *     logger.info("Processing message: ${envelope.message.key}")
 *
 *     // Store deserialized object
 *     val order = deserialize(envelope.message)
 *     envelope.attributes.put(AttributeKey("deserializedValue"), order)
 *
 *     next()
 *
 *     // Access stored value in later middleware
 *     val storedOrder = envelope.attributes.get(AttributeKey("deserializedValue"))
 * }
 * ```
 */
open class SingleMessageEnvelope<TKey, TValue>(
    val message: IncomingMessage<TKey, TValue>,
    val consumerContext: ConsumerContext,
    val attributes: Attributes = Attributes()
)
