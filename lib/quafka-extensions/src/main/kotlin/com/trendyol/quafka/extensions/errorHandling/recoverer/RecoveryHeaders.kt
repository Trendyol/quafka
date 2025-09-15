package com.trendyol.quafka.extensions.errorHandling.recoverer

import com.trendyol.quafka.common.*
import com.trendyol.quafka.common.HeaderParsers.asInstant
import com.trendyol.quafka.common.HeaderParsers.asInt
import com.trendyol.quafka.common.HeaderParsers.asString
import com.trendyol.quafka.consumer.IncomingMessage
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.header.Header
import java.time.Instant

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
    const val RETRY_ATTEMPT = "X-RetryAttempt"
    const val RETRY_IDENTIFIER = "X-RetryIdentifier"
    const val OVERALL_RETRY_ATTEMPT = "X-OverallRetryAttempt"
    const val FORWARDING_TOPIC = "X-ForwardingTopic"
}

fun IncomingMessage<*, *>.originalTopicOrTopic(): String =
    this.headers.get(RecoveryHeaders.ORIGINAL_TOPIC)?.asString() ?: this.topic

fun Iterable<Header>.getRetryIdentifier(): String? =
    get(RecoveryHeaders.RETRY_IDENTIFIER)?.asString()

fun Iterable<Header>.getForwardingTopic(): String? =
    get(RecoveryHeaders.FORWARDING_TOPIC)?.asString()

fun Iterable<Header>.getRetryAttemptOrDefault(defaultValue: Int = 0): Int = get(RecoveryHeaders.RETRY_ATTEMPT)?.asInt() ?: defaultValue

fun Iterable<Header>.getOverallRetryAttemptOrDefault(defaultValue: Int = 0): Int =
    get(RecoveryHeaders.OVERALL_RETRY_ATTEMPT)?.asInt() ?: defaultValue

fun MutableList<Header>.forwardingTopic(
    topic: String
): MutableList<Header> = this
    .addHeader(header(RecoveryHeaders.FORWARDING_TOPIC, topic), true)

fun MutableList<Header>.addException(
    exceptionDetail: ExceptionDetails,
    now: Instant
): MutableList<Header> = this
    .addHeader(header(RecoveryHeaders.EXCEPTION, exceptionDetail.exception.stackTraceToString()), true)
    .addHeader(header(RecoveryHeaders.EXCEPTION_AT, now), true)
    .addHeader(header(RecoveryHeaders.EXCEPTION_DETAIL, exceptionDetail.detail), true)

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

fun MutableList<Header>.addRetryAttempt(
    attempt: Int,
    overallAttempt: Int,
    identifier: String
): MutableList<Header> = this
    .addHeader(header(RecoveryHeaders.RETRY_ATTEMPT, attempt), true)
    .addHeader(header(RecoveryHeaders.OVERALL_RETRY_ATTEMPT, overallAttempt), true)
    .addHeader(header(RecoveryHeaders.RETRY_IDENTIFIER, identifier), true)

fun MutableList<Header>.addPublishedAt(
    instant: Instant
): MutableList<Header> = this
    .addHeader(header(RecoveryHeaders.PUBLISHED_AT, instant), true)

private fun ConsumerRecord<*, *>.topicPartitionOffset(): String = "${topic()} [${partition()}] @${offset()}"
