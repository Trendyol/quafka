package com.trendyol.quafka.common

import kotlinx.coroutines.*
import kotlinx.coroutines.channels.*
import kotlinx.coroutines.selects.whileSelect
import kotlin.time.Duration

/**
 * Splits a [ReceiveChannel] into chunks of a specified size or when a timeout is reached.
 *
 * This internal function groups items from the channel into batches, either when the batch
 * reaches the specified size or when the timeout expires, whichever comes first.
 *
 * **Key Features:**
 * - Emits chunks of items up to the specified size
 * - Timeout-based chunk emission if size not reached
 * - Handles channel closure gracefully
 * - Non-empty chunks are always emitted
 *
 * @param T The type of items in the channel.
 * @param scope The [CoroutineScope] in which to produce the chunked channel.
 * @param size The maximum number of items per chunk.
 * @param timeout The maximum time to wait before emitting a chunk, even if not full.
 * @return A [ReceiveChannel] emitting lists of items grouped into chunks.
 *
 * @sample
 * ```kotlin
 * val channel = produce {
 *     repeat(100) { send(it) }
 * }
 *
 * val chunked = channel.chunked(scope, size = 10, timeout = 100.milliseconds)
 * for (chunk in chunked) {
 *     println("Processing chunk of ${chunk.size} items")
 * }
 * ```
 */
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
