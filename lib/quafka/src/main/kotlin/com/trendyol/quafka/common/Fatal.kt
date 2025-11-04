package com.trendyol.quafka.common
import kotlin.coroutines.cancellation.CancellationException

fun Throwable.isFatal(): Boolean =
    when (this) {
        is Error, is CancellationException -> true
        else -> false
    }

fun Throwable.rethrowIfFatalOrCancelled() {
    if (this.isFatal()) throw this
}
