package com.trendyol.quafka.extensions.errorHandling.testing

import com.trendyol.quafka.consumer.ConsumerContext
import com.trendyol.quafka.consumer.IncomingMessage
import com.trendyol.quafka.extensions.errorHandling.recoverer.ExceptionDetails
import com.trendyol.quafka.producer.OutgoingMessage

/**
 * Fake message router that captures routing decisions without actually sending to Kafka.
 *
 * Useful for testing retry logic in isolation.
 *
 * Example:
 * ```kotlin
 * val router = FakeMessageRouter()
 * // ... trigger retry logic
 * assertEquals(1, router.getRetryMessages().size)
 * assertEquals("topic.retry", router.getRetryMessages().first().topic)
 * ```
 */
class FakeMessageRouter<TKey, TValue> {
    private val retryMessages = mutableListOf<CapturedMessage<TKey, TValue>>()
    private val errorMessages = mutableListOf<CapturedMessage<TKey, TValue>>()
    private val delayMessages = mutableListOf<CapturedMessage<TKey, TValue>>()

    /**
     * Represents a captured outgoing message with context.
     */
    data class CapturedMessage<TKey, TValue>(
        val outgoingMessage: OutgoingMessage<TKey, TValue>,
        val originalMessage: IncomingMessage<TKey, TValue>,
        val consumerContext: ConsumerContext,
        val exceptionDetails: ExceptionDetails,
        val routingType: RoutingType
    )

    enum class RoutingType {
        RETRY,
        ERROR,
        DELAY
    }

    /**
     * Capture a message being routed to retry topic.
     */
    fun captureRetry(
        outgoingMessage: OutgoingMessage<TKey, TValue>,
        originalMessage: IncomingMessage<TKey, TValue>,
        consumerContext: ConsumerContext,
        exceptionDetails: ExceptionDetails
    ) {
        retryMessages.add(
            CapturedMessage(
                outgoingMessage,
                originalMessage,
                consumerContext,
                exceptionDetails,
                RoutingType.RETRY
            )
        )
    }

    /**
     * Capture a message being routed to error/DLQ topic.
     */
    fun captureError(
        outgoingMessage: OutgoingMessage<TKey, TValue>,
        originalMessage: IncomingMessage<TKey, TValue>,
        consumerContext: ConsumerContext,
        exceptionDetails: ExceptionDetails
    ) {
        errorMessages.add(
            CapturedMessage(
                outgoingMessage,
                originalMessage,
                consumerContext,
                exceptionDetails,
                RoutingType.ERROR
            )
        )
    }

    /**
     * Capture a message being routed to delay topic.
     */
    fun captureDelay(
        outgoingMessage: OutgoingMessage<TKey, TValue>,
        originalMessage: IncomingMessage<TKey, TValue>,
        consumerContext: ConsumerContext,
        exceptionDetails: ExceptionDetails
    ) {
        delayMessages.add(
            CapturedMessage(
                outgoingMessage,
                originalMessage,
                consumerContext,
                exceptionDetails,
                RoutingType.DELAY
            )
        )
    }

    /**
     * Get all messages routed to retry topics.
     */
    fun getRetryMessages(): List<CapturedMessage<TKey, TValue>> = retryMessages.toList()

    /**
     * Get all messages routed to error/DLQ topics.
     */
    fun getErrorMessages(): List<CapturedMessage<TKey, TValue>> = errorMessages.toList()

    /**
     * Get all messages routed to delay topics.
     */
    fun getDelayMessages(): List<CapturedMessage<TKey, TValue>> = delayMessages.toList()

    /**
     * Get all captured messages regardless of routing type.
     */
    fun getAllMessages(): List<CapturedMessage<TKey, TValue>> =
        retryMessages + errorMessages + delayMessages

    /**
     * Clear all captured messages.
     */
    fun clear() {
        retryMessages.clear()
        errorMessages.clear()
        delayMessages.clear()
    }

    /**
     * Assert that exactly N retry messages were captured.
     */
    fun assertRetryCount(expected: Int) {
        require(retryMessages.size == expected) {
            "Expected $expected retry messages, but got ${retryMessages.size}"
        }
    }

    /**
     * Assert that exactly N error messages were captured.
     */
    fun assertErrorCount(expected: Int) {
        require(errorMessages.size == expected) {
            "Expected $expected error messages, but got ${errorMessages.size}"
        }
    }

    /**
     * Assert that exactly N delay messages were captured.
     */
    fun assertDelayCount(expected: Int) {
        require(delayMessages.size == expected) {
            "Expected $expected delay messages, but got ${delayMessages.size}"
        }
    }

    /**
     * Assert that no messages were captured.
     */
    fun assertNoMessages() {
        require(getAllMessages().isEmpty()) {
            "Expected no messages, but got ${getAllMessages().size}"
        }
    }
}
