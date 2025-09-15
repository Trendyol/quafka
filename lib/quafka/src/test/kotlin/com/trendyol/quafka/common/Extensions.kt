package com.trendyol.quafka.common

import io.kotest.core.spec.style.FunSpec
import io.kotest.matchers.collections.shouldContainExactly
import kotlinx.coroutines.*
import kotlinx.coroutines.channels.produce
import kotlinx.coroutines.flow.*
import kotlin.time.Duration
import kotlin.time.Duration.Companion.milliseconds

@OptIn(ExperimentalCoroutinesApi::class)
internal class ChunkedExtensionTest :
    FunSpec({

        context("chunked") {

            test("should collect items into chunks of specified size") {
                // Arrange
                val inputChannel = produce {
                    repeat(10) { send(it) }
                    close()
                }

                // Act
                val chunkedChannel = inputChannel.chunked(this, size = 3, timeout = Duration.ZERO)

                // Assert
                val chunks = chunkedChannel.consumeAsFlow().toList()
                chunks shouldContainExactly listOf(
                    listOf(0, 1, 2),
                    listOf(3, 4, 5),
                    listOf(6, 7, 8),
                    listOf(9)
                )
            }

            test("should emit a chunk when timeout is reached") {
                // Arrange
                val inputChannel = produce {
                    send(1)
                    delay(500)
                    send(2)
                }

                // Act
                val chunkedChannel = inputChannel.chunked(this, size = 3, timeout = 200.milliseconds)

                // Assert
                val chunks = chunkedChannel.consumeAsFlow().toList()
                chunks shouldContainExactly listOf(
                    listOf(1),
                    listOf(2)
                )
            }

            test("should handle empty input channel gracefully") {
                // Arrange
                val inputChannel = produce<Int> { close() }

                // Act
                val chunkedChannel = inputChannel.chunked(this, size = 3, timeout = Duration.ZERO)

                // Assert
                val chunks = chunkedChannel.consumeAsFlow().toList()
                chunks shouldContainExactly emptyList()
            }

            test("should handle partial chunks when channel closes") {
                // Arrange
                val inputChannel = produce {
                    send(1)
                    send(2)
                    close()
                }

                // Act
                val chunkedChannel = inputChannel.chunked(this, size = 5, timeout = Duration.ZERO)

                // Assert
                val chunks = chunkedChannel.consumeAsFlow().toList()
                chunks shouldContainExactly listOf(
                    listOf(1, 2)
                )
            }

            test("should cancel the ticker when channel is closed") {
                // Arrange
                val inputChannel = produce<Int> {
                    send(1)
                    delay(100)
                    send(2)
                    close()
                }

                // Act
                val chunkedChannel = inputChannel.chunked(this, size = 5, timeout = 500.milliseconds)

                // Assert
                val chunks = chunkedChannel.consumeAsFlow().toList()
                chunks shouldContainExactly listOf(
                    listOf(1, 2)
                )
            }
        }
    })
