package com.trendyol.quafka.common

import io.kotest.core.spec.style.FunSpec
import io.kotest.matchers.shouldBe
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit

internal class WaiterTests :
    FunSpec({

        test("should complete immediately when counter is 0") {
            // Arrange
            val waiter = Waiter(0)

            // Act & Assert
            waiter.wait() // Should not block
        }

        test("should block until all counts are done") {
            // Arrange
            val waiter = Waiter(3)
            val executor = Executors.newFixedThreadPool(3)

            try {
                // Act
                var completed = false
                executor.submit {
                    waiter.wait()
                    completed = true
                }

                Thread.sleep(100) // Give time for wait to start
                completed shouldBe false

                waiter.done()
                waiter.done()
                Thread.sleep(100)
                completed shouldBe false

                waiter.done()
                Thread.sleep(100)
                completed shouldBe true
            } finally {
                executor.shutdown()
            }
        }

        test("should allow multiple threads to wait") {
            // Arrange
            val waiter = Waiter(1)
            val executor = Executors.newFixedThreadPool(3)
            val results = mutableListOf<Boolean>()

            try {
                // Act
                val future1 = executor.submit {
                    waiter.wait()
                    synchronized(results) { results.add(true) }
                }
                val future2 = executor.submit {
                    waiter.wait()
                    synchronized(results) { results.add(true) }
                }

                Thread.sleep(100) // Give time for waits to start
                results.size shouldBe 0

                waiter.done()

                // Assert
                future1.get(1, TimeUnit.SECONDS)
                future2.get(1, TimeUnit.SECONDS)
                results.size shouldBe 2
            } finally {
                executor.shutdown()
            }
        }

        test("should handle done calls after counter reaches zero") {
            // Arrange
            val waiter = Waiter(1)

            // Act
            waiter.done()
            waiter.done() // Extra done call
            waiter.done() // Extra done call

            // Assert
            waiter.wait() // Should complete immediately
        }

        test("should work with maximum counter value") {
            // Arrange
            val waiter = Waiter(Int.MAX_VALUE)
            val executor = Executors.newFixedThreadPool(2)

            try {
                // Act
                var completed = false
                executor.submit {
                    waiter.wait()
                    completed = true
                }

                Thread.sleep(100)
                completed shouldBe false

                // Count down Int.MAX_VALUE times would be impractical
                // This test just verifies that initialization works
            } finally {
                executor.shutdown()
            }
        }
    })
