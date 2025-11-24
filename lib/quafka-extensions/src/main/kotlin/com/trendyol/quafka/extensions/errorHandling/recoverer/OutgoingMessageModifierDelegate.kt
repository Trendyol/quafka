package com.trendyol.quafka.extensions.errorHandling.recoverer

import com.trendyol.quafka.consumer.*
import com.trendyol.quafka.producer.OutgoingMessage

/**
 * A function type for modifying outgoing messages before they are sent to retry or error topics.
 *
 * This allows customization of messages being sent to retry/error topics, such as:
 * - Adding custom headers
 * - Transforming the message payload
 * - Enriching with additional metadata
 *
 * @param TKey The type of the message key.
 * @param TValue The type of the message value.
 * @receiver The outgoing message being sent to a retry/error topic.
 * @param message The original incoming message that failed.
 * @param consumerContext The consumer context when the failure occurred.
 * @param exceptionDetails Details about the exception that occurred.
 * @return The modified outgoing message. Can be the same instance or a new one.
 */
typealias OutgoingMessageModifierDelegate<TKey, TValue> = suspend OutgoingMessage<TKey, TValue>.(
    message: IncomingMessage<TKey, TValue>,
    consumerContext: ConsumerContext,
    exceptionDetails: ExceptionDetails
) -> OutgoingMessage<TKey, TValue>
