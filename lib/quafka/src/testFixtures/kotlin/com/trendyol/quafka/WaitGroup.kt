package com.trendyol.quafka

import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.withTimeoutOrNull
import java.time.Duration
import java.util.concurrent.atomic.AtomicInteger

class WaitGroup(initial: Int = 0) {
    private val counter = AtomicInteger(initial)
    private val waiter = Channel<Unit>()

    suspend fun add(delta: Int = 1) {
        val counterValue = counter.addAndGet(delta)
        when {
            counterValue == 0 -> waiter.send(Unit)
            counterValue < 0 -> return
        }
    }

    suspend fun done() = add(-1)

    suspend fun wait() = waiter.receive()

    /**
     * Wait for the counter to reach zero with a timeout.
     *
     * @param timeout The maximum time to wait
     * @return true if the counter reached zero within the timeout, false otherwise
     */
    suspend fun wait(timeout: Duration): Boolean = withTimeoutOrNull(timeout.toMillis()) {
        waiter.receive()
        true
    } ?: false

    /**
     * Get the current counter value.
     *
     * @return The current counter value
     */
    fun getCount(): Int = counter.get()

    /**
     * Reset the counter to the specified value.
     *
     * @param value The value to reset the counter to
     */
    fun reset(value: Int = 0) {
        counter.set(value)
    }
}
