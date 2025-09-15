package com.trendyol.quafka.extensions.delaying

import com.trendyol.quafka.*
import com.trendyol.quafka.common.header
import com.trendyol.quafka.consumer.IncomingMessage
import com.trendyol.quafka.extensions.delaying.DelayHeaders.DELAY_SECONDS
import com.trendyol.quafka.extensions.delaying.DelayHeaders.DELAY_STRATEGY
import io.kotest.core.spec.style.FunSpec
import io.kotest.matchers.shouldBe
import java.time.Instant
import kotlin.time.Duration
import kotlin.time.Duration.Companion.seconds

class MessageDelayerTests :
    FunSpec({
        class SutMessageDelayer : MessageDelayer() {
            var delayed: Boolean = false
            var delayedDuration: Duration = Duration.ZERO

            override suspend fun delay(duration: Duration) {
                delayed = true
                delayedDuration = duration
            }
        }

        lateinit var sut: SutMessageDelayer
        beforeEach {
            sut = SutMessageDelayer()
        }

        test("should not delay when no header specified") {
            val incomingMessage =
                buildDelayableIncomingMessage(
                    publishedAt = "2025-01-04T09:01:29.00Z",
                    delaySeconds = null,
                    delayStrategy = null
                )
            sut.delayIfNeeded(incomingMessage, incomingMessage.buildConsumerContext(FakeTimeProvider(Instant.parse("2025-01-04T09:01:30.00Z"))))
            sut.delayed shouldBe false
        }

        test("should delay only remaining diff when strategy is RELATIVE") {
            val incomingMessage =
                buildDelayableIncomingMessage(
                    publishedAt = "2025-01-04T09:01:29.00Z",
                    delaySeconds = 10,
                    delayStrategy = DelayStrategy.UNTIL_TARGET_TIME
                )

            sut.delayIfNeeded(incomingMessage, incomingMessage.buildConsumerContext(FakeTimeProvider(Instant.parse("2025-01-04T09:01:30.00Z"))))
            sut.delayed shouldBe true
            sut.delayedDuration shouldBe 9.seconds
        }

        test("should delay absolute value when strategy is ABSOLUTE") {
            val incomingMessage =
                buildDelayableIncomingMessage(
                    publishedAt = "2025-01-04T09:01:29.00Z",
                    delaySeconds = 10,
                    delayStrategy = DelayStrategy.FOR_FIXED_DURATION
                )

            sut.delayIfNeeded(incomingMessage, incomingMessage.buildConsumerContext(FakeTimeProvider(Instant.parse("2025-01-04T09:01:30.00Z"))))
            sut.delayed shouldBe true
            sut.delayedDuration shouldBe 10.seconds
        }

        test("should not delay when strategy is RELATIVE and executing time is greater than after delayed time") {
            val incomingMessage =
                buildDelayableIncomingMessage(
                    publishedAt = "2025-01-04T09:01:30.00Z",
                    delaySeconds = 10,
                    delayStrategy = DelayStrategy.UNTIL_TARGET_TIME
                )
            sut.delayIfNeeded(incomingMessage, incomingMessage.buildConsumerContext(FakeTimeProvider(Instant.parse("2025-01-04T09:01:40.00Z"))))
            sut.delayed shouldBe false
        }
    })

private suspend fun buildDelayableIncomingMessage(
    publishedAt: String,
    delaySeconds: Int? = null,
    delayStrategy: DelayStrategy? = null
): IncomingMessage<String, String> {
    val headers = mutableListOf(
        header(DelayHeaders.DELAY_PUBLISHED_AT, publishedAt)
    )
    if (delaySeconds != null) {
        headers.add(header(DELAY_SECONDS, delaySeconds.seconds.inWholeSeconds.toString()))
        if (delayStrategy != null) {
            headers.add(header(DELAY_STRATEGY, delayStrategy.toString()))
        }
    }
    val message =
        IncomingMessageBuilder(
            "topic",
            0,
            0,
            "key",
            "value",
            headers = headers
        ).build()

    return message
}
