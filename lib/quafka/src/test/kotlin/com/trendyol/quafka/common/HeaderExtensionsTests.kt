package com.trendyol.quafka.common

import io.kotest.core.spec.style.FunSpec
import io.kotest.matchers.collections.shouldContainExactly
import io.kotest.matchers.shouldBe
import org.apache.kafka.common.header.Header

class HeaderExtensionsTests :
    FunSpec({

        context("addHeader") {
            test("should add a new header when no existing header with the same key exists") {
                // Arrange
                val headers = mutableListOf<Header>()
                val newHeader = header("key1", "value1")

                // Act
                headers.addHeader(newHeader)

                // Assert
                headers shouldContainExactly listOf(newHeader)
            }

            test("should replace an existing header when the override flag is true") {
                // Arrange
                val headers = mutableListOf(
                    header("key1", "value1")
                )
                val newHeader = header("key1", "value2")

                // Act
                headers.addHeader(newHeader, override = true)

                // Assert
                headers shouldContainExactly listOf(newHeader)
            }

            test("should not replace an existing header when the override flag is false") {
                // Arrange
                val headers = mutableListOf(
                    header("key1", "value1")
                )
                val newHeader = header("key1", "value2")

                // Act
                headers.addHeader(newHeader, override = false)

                // Assert
                headers shouldContainExactly listOf(header("key1", "value1"))
            }

            test("should handle adding multiple headers with different keys") {
                // Arrange
                val headers = mutableListOf<Header>()
                val header1 = header("key1", "value1")
                val header2 = header("key2", "value2")

                // Act
                headers.addHeader(header1)
                headers.addHeader(header2)

                // Assert
                headers shouldContainExactly listOf(header1, header2)
            }
        }

        context("get") {
            test("should return the last header with the specified key") {
                // Arrange
                val headers = listOf(
                    header("key1", "value1"),
                    header("key1", "value2"),
                    header("key2", "value3")
                )

                // Act
                val result = headers.get("key1")

                // Assert
                result shouldBe header("key1", "value2")
            }

            test("should return null when no header with the specified key exists") {
                // Arrange
                val headers = listOf(
                    header("key1", "value1"),
                    header("key2", "value2")
                )

                // Act
                val result = headers.get("key3")

                // Assert
                result shouldBe null
            }
        }

        context("toMap") {
            test("should convert headers to a map keyed by header key") {
                // Arrange
                val headers = listOf(
                    header("key1", "value1"),
                    header("key2", "value2"),
                    header("key3", "value3")
                )

                // Act
                val result = headers.toMap()

                // Assert
                result shouldBe mapOf(
                    "key1" to header("key1", "value1"),
                    "key2" to header("key2", "value2"),
                    "key3" to header("key3", "value3")
                )
            }

            test("should keep only the last occurrence of each key") {
                // Arrange
                val headers = listOf(
                    header("key1", "value1"),
                    header("key1", "value2"),
                    header("key2", "value3")
                )

                // Act
                val result = headers.toMap()

                // Assert
                result shouldBe mapOf(
                    "key1" to header("key1", "value2"),
                    "key2" to header("key2", "value3")
                )
            }
        }
    })
