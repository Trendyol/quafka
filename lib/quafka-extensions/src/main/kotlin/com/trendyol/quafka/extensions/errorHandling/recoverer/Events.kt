package com.trendyol.quafka.extensions.errorHandling.recoverer

import com.trendyol.quafka.consumer.*
import com.trendyol.quafka.events.QuafkaEvent
import com.trendyol.quafka.producer.OutgoingMessage
import kotlin.time.Duration

/**
 * Container object for all recoverer-related events published to the event bus.
 *
 * These events allow external systems to monitor and react to message retry and error handling activities.
 */
object RecovererEvent {
    /**
     * Event published when a message from an unknown/unconfigured topic is sent to the dangling topic.
     *
     * This typically indicates a configuration issue where a topic is being consumed but
     * has no retry configuration defined.
     *
     * @property message The original incoming message that failed.
     * @property exceptionDetails Details about the exception that occurred.
     * @property outgoingMessage The message sent to the dangling topic.
     * @property consumerContext The consumer context when the failure occurred.
     */
    data class DanglingMessagePublished(
        val message: IncomingMessage<*, *>,
        val exceptionDetails: ExceptionDetails,
        val outgoingMessage: OutgoingMessage<*, *>,
        val consumerContext: ConsumerContext
    ) : QuafkaEvent

    /**
     * Event published when a message is sent to a retry topic for reprocessing.
     *
     * @property message The original incoming message that failed.
     * @property exceptionDetails Details about the exception that occurred.
     * @property outgoingMessage The message sent to the retry topic.
     * @property consumerContext The consumer context when the failure occurred.
     * @property attempt The current attempt number for this specific policy.
     * @property overallAttempt The total number of attempts across all policies.
     * @property policyIdentifier The identifier of the retry policy being applied.
     * @property delay The delay duration before the message will be reprocessed.
     */
    data class RetryMessagePublished(
        val message: IncomingMessage<*, *>,
        val exceptionDetails: ExceptionDetails,
        val outgoingMessage: OutgoingMessage<*, *>,
        val consumerContext: ConsumerContext,
        val attempt: Int,
        val overallAttempt: Int,
        val policyIdentifier: PolicyIdentifier,
        val delay: Duration
    ) : QuafkaEvent

    /**
     * Event published when a message is sent to the dead-letter/error topic.
     *
     * This occurs when all retry attempts are exhausted or the error is non-retryable.
     *
     * @property message The original incoming message that failed.
     * @property exceptionDetails Details about the exception that occurred.
     * @property outgoingMessage The message sent to the error topic.
     * @property consumerContext The consumer context when the failure occurred.
     */
    data class ErrorMessagePublished(
        val message: IncomingMessage<*, *>,
        val exceptionDetails: ExceptionDetails,
        val outgoingMessage: OutgoingMessage<*, *>,
        val consumerContext: ConsumerContext
    ) : QuafkaEvent

    /**
     * Event published when a message is sent to a delay topic before being forwarded to a retry topic.
     *
     * This is used in exponential backoff strategies where messages are temporarily held
     * in a delay topic before being forwarded to the final retry topic.
     *
     * @property message The original incoming message that failed.
     * @property exceptionDetails Details about the exception that occurred.
     * @property outgoingMessage The message sent to the delay topic.
     * @property consumerContext The consumer context when the failure occurred.
     * @property attempt The current attempt number for this specific policy.
     * @property overallAttempt The total number of attempts across all policies.
     * @property policyIdentifier The identifier of the retry policy being applied.
     * @property delay The delay duration before the message will be forwarded.
     * @property forwardingTopic The topic to which the message will be forwarded after the delay.
     */
    data class DelayMessagePublished(
        val message: IncomingMessage<*, *>,
        val exceptionDetails: ExceptionDetails,
        val outgoingMessage: OutgoingMessage<*, *>,
        val consumerContext: ConsumerContext,
        val attempt: Int,
        val overallAttempt: Int,
        val policyIdentifier: PolicyIdentifier,
        val delay: Duration,
        val forwardingTopic: String
    ) : QuafkaEvent
}
