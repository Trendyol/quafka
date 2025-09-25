package com.trendyol.quafka.common
import kotlinx.coroutines.ensureActive
import kotlin.coroutines.*
import kotlin.coroutines.cancellation.CancellationException

fun Throwable.isFatal(): Boolean =
    when (this) {
        is Error, is CancellationException -> true
        else -> false
    }

suspend fun Throwable.rethrowIfFatalOrCancelled(context: CoroutineContext? = null) {
    (context ?: coroutineContext).ensureActive()
    if (isFatal()) {
        throw this
    }
}

fun Throwable.rethrowIfFatal() {
    if (this.isFatal()) {
        throw this
    }
}
