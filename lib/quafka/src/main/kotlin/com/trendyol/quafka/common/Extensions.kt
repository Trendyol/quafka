package com.trendyol.quafka.common

import kotlinx.coroutines.*
import kotlinx.coroutines.channels.*
import kotlinx.coroutines.selects.whileSelect
import kotlin.time.Duration

@OptIn(ObsoleteCoroutinesApi::class, ExperimentalCoroutinesApi::class)
internal fun <T> ReceiveChannel<T>.chunked(
    scope: CoroutineScope,
    size: Int,
    timeout: Duration
): ReceiveChannel<List<T>> = scope.produce {
    while (true) {
        val chunk = ArrayList<T>()
        val ticker = if (timeout > Duration.ZERO) ticker(timeout.inWholeMilliseconds) else null
        try {
            whileSelect {
                ticker?.onReceive {
                    // Timeout reached; stop collecting more items
                    false
                }
                this@chunked.onReceive {
                    chunk += it
                    // Continue if the chunk is not full
                    chunk.size < size
                }
            }
        } catch (e: ClosedReceiveChannelException) {
            return@produce
        } finally {
            ticker?.cancel()
            if (chunk.isNotEmpty()) {
                send(chunk.toList())
            }
        }
    }
}
