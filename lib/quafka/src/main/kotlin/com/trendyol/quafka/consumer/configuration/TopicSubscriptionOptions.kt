package com.trendyol.quafka.consumer.configuration

import com.trendyol.quafka.consumer.messageHandlers.MessageHandlingStrategy
import kotlin.time.Duration

/**
 * Subscription configuration options for a Kafka consumer.
 *
 * This data class encapsulates various settings related to subscribing to a Kafka topic,
 * including topic name, backpressure controls, auto-acknowledgment behavior, and message handling options.
 *
 * @param TKey The type of the key in the Kafka messages (e.g., String, Int, Custom Type)
 * @param TValue The type of the value in the Kafka messages (e.g., String, ByteArray, Custom Type)
 * @property topic The name of the Kafka topic to subscribe to
 * @property autoAck If `true`, messages are automatically acknowledged after successful processing.
 *                  If `false`, manual acknowledgment is required using the message's ack() method.
 * @property backpressureBufferSize The maximum number of messages to buffer before applying backpressure.
 *                                  When this limit is reached, the consumer will stop fetching new messages
 *                                  until the buffer is processed below this threshold.
 * @property backpressureReleaseTimeout The maximum duration to wait before releasing backpressure.
 *                                      After this timeout, the consumer will resume fetching messages
 *                                      even if the buffer is still full.
 * @property messageHandlingStrategy Defines how messages should be processed (e.g., batch or single message processing).
 *                                  This strategy determines the processing behavior and error handling approach.
 */
data class TopicSubscriptionOptions<TKey, TValue>(
    val topic: String,
    val autoAck: Boolean,
    val backpressureBufferSize: Int,
    val backpressureReleaseTimeout: Duration,
    val messageHandlingStrategy: MessageHandlingStrategy<TKey, TValue>
)
