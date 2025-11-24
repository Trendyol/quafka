package com.trendyol.quafka.extensions.delaying

import com.trendyol.quafka.common.*
import com.trendyol.quafka.common.HeaderParsers.asInstant
import com.trendyol.quafka.common.HeaderParsers.asInt
import com.trendyol.quafka.common.HeaderParsers.asString
import com.trendyol.quafka.common.HeaderParsers.key
import com.trendyol.quafka.consumer.*
import com.trendyol.quafka.extensions.common.toEnum
import com.trendyol.quafka.extensions.delaying.DelayStrategy.*
import com.trendyol.quafka.logging.*
import com.trendyol.quafka.producer.OutgoingMessage
import org.apache.kafka.common.header.Header
import org.slf4j.Logger
import org.slf4j.event.Level
import java.time.Instant
import kotlin.math.max
import kotlin.time.*
import kotlin.time.Duration.Companion.seconds

object DelayHeaders {
    const val DELAY_SECONDS = "X-DelaySeconds"
    const val DELAY_STRATEGY = "X-DelayStrategy"
    const val DELAY_PUBLISHED_AT = "X-DelayPublishedAt"
    private val allHeaders = setOf(DELAY_SECONDS, DELAY_STRATEGY, DELAY_PUBLISHED_AT)

    fun Iterable<Header>.hasRetryHeaders(): Boolean = allHeaders.all { h -> this.any { it.key == h } }

    fun <TKey, TValue> OutgoingMessage<TKey, TValue>.withDelay(
        delayStrategy: DelayStrategy,
        duration: Duration,
        now: Instant
    ): OutgoingMessage<TKey, TValue> {
        val headers = this.headers
            .toMutableList()
            .withDelay(delayStrategy, duration, now)
        return this.copy(headers = headers)
    }

    fun MutableList<Header>.withDelay(
        delayStrategy: DelayStrategy,
        duration: Duration,
        now: Instant
    ): MutableList<Header> = this
        .addHeader(header(DELAY_STRATEGY, delayStrategy.toString()))
        .addHeader(header(DELAY_SECONDS, max(duration.inWholeSeconds, 1)))
        .addHeader(header(DELAY_PUBLISHED_AT, now), true)

    fun MutableList<Header>.clearDelay() = this
        .remove(allHeaders)
}

/**
 * Represents the delay strategy for processing a message.
 *
 * - [UNTIL_TARGET_TIME]: Calculates the delay based on the message's `publishedAt` timestamp.
 *   The message will be delayed only if `publishedAt + delayDuration` is in the future.
 *   If that time has already passed, the message is processed immediately with no delay.
 *
 * - [FOR_FIXED_DURATION]: Always delays the message by the given duration,
 *   regardless of the `publishedAt` timestamp.
 */
enum class DelayStrategy {
    /**
     * Delays the message only until `publishedAt + delayDuration`.
     * If the current time is already past that point, no delay is applied.
     *
     * Useful for event-time-based delay logic.
     */
    UNTIL_TARGET_TIME,

    /**
     * Delays the message for a fixed duration, regardless of when it was published.
     *
     * Useful for uniform throttling or backoff behavior.
     */
    FOR_FIXED_DURATION
}

open class MessageDelayer(val delayFn: suspend (duration: Duration) -> Unit = { duration -> kotlinx.coroutines.delay(duration) }) {
    data class DelayOptions(val duration: Duration, val strategy: DelayStrategy, val publishedAt: Instant) {
        fun hasDelay() = duration != Duration.ZERO

        companion object {
            fun parse(incomingMessage: IncomingMessage<*, *>): DelayOptions? {
                val duration = incomingMessage.headers
                    .get(DelayHeaders.DELAY_SECONDS)
                    ?.asInt()
                    ?.seconds ?: return null
                val strategy =
                    incomingMessage.headers
                        .get(DelayHeaders.DELAY_STRATEGY)
                        ?.asString()
                        .toEnum<DelayStrategy>(UNTIL_TARGET_TIME)

                val publishedAt = incomingMessage.headers
                    .get(DelayHeaders.DELAY_PUBLISHED_AT)
                    ?.asInstant() ?: incomingMessage.getTimestampAsInstant()
                return DelayOptions(duration, strategy, publishedAt)
            }

            val NoDelay = DelayOptions(Duration.ZERO, UNTIL_TARGET_TIME, Instant.now())
        }
    }

    companion object {
        val logger: Logger = LoggerHelper.createLogger(MessageDelayer::class.java)
    }

    open suspend fun delayIfNeeded(
        message: IncomingMessage<*, *>,
        consumerContext: ConsumerContext,
        defaultDelayOptions: DelayOptions = DelayOptions.NoDelay
    ) {
        val delayOptions = DelayOptions.parse(message) ?: defaultDelayOptions
        if (!delayOptions.hasDelay()) {
            return
        }

        when (delayOptions.strategy) {
            UNTIL_TARGET_TIME -> delayDiff(message, consumerContext, delayOptions)
            FOR_FIXED_DURATION -> logAndDelay(message, consumerContext, delayOptions.duration)
        }
    }

    private suspend fun delayDiff(
        message: IncomingMessage<*, *>,
        consumerContext: ConsumerContext,
        delayOptions: DelayOptions
    ) {
        val now = consumerContext.consumerOptions.timeProvider.now()
        val diffBetweenNowAndPublishAt = java.time.Duration
            .between(delayOptions.publishedAt, now)
            .toKotlinDuration()
        if (diffBetweenNowAndPublishAt >= delayOptions.duration) {
            return
        }

        val remainingDelay = delayOptions.duration - diffBetweenNowAndPublishAt
        if (remainingDelay.isInfinite() || remainingDelay.isNegative()) {
            return
        }
        logAndDelay(message, consumerContext, remainingDelay)
    }

    private suspend fun logAndDelay(
        message: IncomingMessage<*, *>,
        consumerContext: ConsumerContext,
        duration: Duration
    ) {
        if (logger.isTraceEnabled) {
            logger
                .atTrace()
                .enrichWithConsumerContext(consumerContext)
                .log(
                    "message will be delayed ${duration.inWholeMilliseconds} ms | {}",
                    message.toString(Level.TRACE)
                )
        }
        delay(duration)
    }

    open suspend fun delay(duration: Duration) {
        delayFn(duration)
    }
}
