package com.trendyol.quafka.common

import java.time.*

/**
 * Provides the current time in UTC.
 *
 * This interface can be implemented to provide custom time sources, enabling testability and flexibility.
 */
interface TimeProvider {
    /**
     * Retrieves the current UTC time.
     *
     * @return The current [Instant] in UTC.
     */
    fun now(): Instant
}

/**
 * Default implementation of [TimeProvider] that uses the system clock to provide the current UTC time.
 */
object SystemTimeProvider : TimeProvider {
    /**
     * Retrieves the current UTC time using the system clock.
     *
     * @return The current [Instant] in UTC.
     */
    override fun now(): Instant = Instant.now(Clock.systemUTC())
}
