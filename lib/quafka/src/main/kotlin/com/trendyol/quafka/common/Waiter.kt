package com.trendyol.quafka.common

import java.util.concurrent.*

internal class Waiter(counter: Int = 1) {
    private val countDownLatch = CountDownLatch(counter)

    fun done() {
        countDownLatch.countDown()
    }

    @JvmName("WAIT")
    fun wait() {
        countDownLatch.await()
    }
}
