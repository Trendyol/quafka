package com.trendyol.quafka

import com.trendyol.quafka.common.TimeProvider
import java.time.Instant

class FakeTimeProvider(
    private val instant: Instant
) : TimeProvider {
    companion object {
        fun parse(instant: String): TimeProvider = FakeTimeProvider(Instant.parse(instant))

        fun default(value: String = "2024-12-15T13:00:00.00Z"): TimeProvider = parse(value)

        fun from(instant: Instant): TimeProvider = FakeTimeProvider(instant)
    }

    override fun now(): Instant = instant
}
