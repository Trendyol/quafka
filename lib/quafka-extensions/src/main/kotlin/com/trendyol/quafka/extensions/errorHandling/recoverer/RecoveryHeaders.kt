package com.trendyol.quafka.extensions.errorHandling.recoverer

import com.trendyol.quafka.common.*
import com.trendyol.quafka.common.HeaderParsers.asInstant
import com.trendyol.quafka.common.HeaderParsers.asInt
import com.trendyol.quafka.common.HeaderParsers.asString
import com.trendyol.quafka.consumer.IncomingMessage
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.header.Header
import java.time.Instant

/**
 * Constants and utility functions for recovery-related Kafka message headers.
 *
 * These headers track retry attempts, exception details, and message routing information
 * as messages flow through the retry system.
 */
internal object RecoveryHeaders {
    const val PUBLISHED_AT = "X-PublishedAt"
    const val EXCEPTION_AT = "X-ExceptionAt"
    const val EXCEPTION = "X-Exception"
    const val EXCEPTION_DETAIL = "X-ExceptionDetail"
    const val ORIGINAL_TOPIC = "X-OriginalTopic"
    const val ORIGINAL_OFFSET = "X-OriginalOffset"
    const val ORIGINAL_PUBLISHED_AT = "X-OriginalPublishedAt"
    const val ORIGINAL_PARTITION = "X-OriginalPartition"
    const val FROM_TOPIC_PARTITION_OFFSET = "X-FromTopicPartitionOffset"
    const val RETRY_ATTEMPTS = "X-RetryAttempts"
    const val RETRY_POLICY_IDENTIFIER = "X-RetryPolicyIdentifier"
    const val OVERALL_RETRY_ATTEMPTS = "X-OverallRetryAttempts"
    const val FORWARDING_TOPIC = "X-ForwardingTopic"
}

/**
 * Returns the original topic name if the message has been retried, otherwise returns the current topic.
 *
 * @return The original topic name, or current topic if not a retry.
 */
fun IncomingMessage<*, *>.originalTopicOrTopic(): String =
    this.headers.get(RecoveryHeaders.ORIGINAL_TOPIC)?.asString() ?: this.topic

/**
 * Extracts the retry policy identifier from message headers.
 *
 * @return The [PolicyIdentifier] from headers, or [PolicyIdentifier.none] if not present.
 */
fun Iterable<Header>.getRetryPolicyIdentifier(): PolicyIdentifier =
    get(RecoveryHeaders.RETRY_POLICY_IDENTIFIER)?.asString()?.let { PolicyIdentifier.of(it) } ?: PolicyIdentifier.none()

/**
 * Extracts the forwarding topic from message headers.
 *
 * @return The forwarding topic name, or null if not present.
 */
fun Iterable<Header>.getForwardingTopic(): String? =
    get(RecoveryHeaders.FORWARDING_TOPIC)?.asString()

/**
 * Extracts the retry attempt count from message headers.
 *
 * @param defaultValue The value to return if the header is not present.
 * @return The retry attempt count.
 */
fun Iterable<Header>.getRetryAttemptOrDefault(defaultValue: Int = 0): Int = get(RecoveryHeaders.RETRY_ATTEMPTS)?.asInt() ?: defaultValue

/**
 * Extracts the overall retry attempt count from message headers.
 *
 * @param defaultValue The value to return if the header is not present.
 * @return The overall retry attempt count.
 */
fun Iterable<Header>.getOverallRetryAttemptOrDefault(defaultValue: Int = 0): Int =
    get(RecoveryHeaders.OVERALL_RETRY_ATTEMPTS)?.asInt() ?: defaultValue

/**
 * Adds or updates the forwarding topic header.
 *
 * @param topic The topic to forward the message to after delay.
 * @return This header list for chaining.
 */
fun MutableList<Header>.forwardingTopic(
    topic: String
): MutableList<Header> = this
    .addHeader(header(RecoveryHeaders.FORWARDING_TOPIC, topic), true)

/**
 * Adds exception details to message headers.
 *
 * @param exceptionDetail The exception details to add.
 * @param now The current timestamp.
 * @return This header list for chaining.
 */
fun MutableList<Header>.addException(
    exceptionDetail: ExceptionDetails,
    now: Instant
): MutableList<Header> = this
    .addHeader(header(RecoveryHeaders.EXCEPTION, exceptionDetail.exception.stackTraceToString()), true)
    .addHeader(header(RecoveryHeaders.EXCEPTION_AT, now), true)
    .addHeader(header(RecoveryHeaders.EXCEPTION_DETAIL, exceptionDetail.detail), true)

/**
 * Adds origin topic information to message headers.
 *
 * Preserves the original topic, partition, and offset information from when
 * the message was first consumed.
 *
 * @param incomingMessage The message being processed.
 * @param now The current timestamp.
 * @return This header list for chaining.
 */
fun MutableList<Header>.addOriginTopicInformation(
    incomingMessage: IncomingMessage<*, *>,
    now: Instant
): MutableList<Header> = this
    .addHeader(
        header(RecoveryHeaders.FROM_TOPIC_PARTITION_OFFSET, incomingMessage.consumerRecord.topicPartitionOffset()),
        true
    ).addHeader(header(RecoveryHeaders.ORIGINAL_TOPIC, incomingMessage.topic), false)
    .addHeader(header(RecoveryHeaders.ORIGINAL_PARTITION, incomingMessage.partition), false)
    .addHeader(header(RecoveryHeaders.ORIGINAL_OFFSET, incomingMessage.offset), false)
    .addHeader(
        header(
            RecoveryHeaders.ORIGINAL_PUBLISHED_AT,
            get(RecoveryHeaders.PUBLISHED_AT)?.asInstant() ?: now
        ),
        false
    )

/**
 * Adds or updates retry attempt tracking headers.
 *
 * @param attempts The current retry attempt number for this policy.
 * @param overallAttempts The total retry attempt number across all policies.
 * @param identifier The policy identifier being applied.
 * @return This header list for chaining.
 */
fun MutableList<Header>.addRetryAttempt(
    attempts: Int,
    overallAttempts: Int,
    identifier: PolicyIdentifier
): MutableList<Header> = this
    .addHeader(header(RecoveryHeaders.RETRY_ATTEMPTS, attempts), true)
    .addHeader(header(RecoveryHeaders.OVERALL_RETRY_ATTEMPTS, overallAttempts), true)
    .addHeader(header(RecoveryHeaders.RETRY_POLICY_IDENTIFIER, identifier.value), true)

/**
 * Adds or updates the published-at timestamp header.
 *
 * @param instant The timestamp when the message is being published.
 * @return This header list for chaining.
 */
fun MutableList<Header>.addPublishedAt(
    instant: Instant
): MutableList<Header> = this
    .addHeader(header(RecoveryHeaders.PUBLISHED_AT, instant), true)

/**
 * Formats the topic, partition, and offset as a readable string.
 */
private fun ConsumerRecord<*, *>.topicPartitionOffset(): String = "${topic()} [${partition()}] @${offset()}"
