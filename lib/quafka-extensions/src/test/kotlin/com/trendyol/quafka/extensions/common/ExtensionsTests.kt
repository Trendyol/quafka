package com.trendyol.quafka.extensions.common

import io.kotest.core.spec.style.FunSpec
import io.kotest.matchers.shouldBe

class ExtensionsTests :
    FunSpec({

        test("should return matching enum value when string matches an enum name") {
            // Arrange
            val input = "VALUE_ONE"
            val defaultValue = SampleEnum.VALUE_TWO

            // Act
            val result = input.toEnum(defaultValue)

            // Assert
            result shouldBe SampleEnum.VALUE_ONE
        }

        test("should return default value when string does not match any enum name") {
            // Arrange
            val input = "NON_EXISTENT"
            val defaultValue = SampleEnum.VALUE_TWO

            // Act
            val result = input.toEnum(defaultValue)

            // Assert
            result shouldBe defaultValue
        }

        test("should return default value when input is null") {
            // Arrange
            val input: String? = null
            val defaultValue = SampleEnum.VALUE_THREE

            // Act
            val result = input.toEnum(defaultValue)

            // Assert
            result shouldBe defaultValue
        }

        test("should perform case-insensitive matching when ignoreCase is true") {
            // Arrange
            val input = "value_one"
            val defaultValue = SampleEnum.VALUE_TWO

            // Act
            val result = input.toEnum(defaultValue, ignoreCase = true)

            // Assert
            result shouldBe SampleEnum.VALUE_ONE
        }

        test("should return default value when case-sensitive matching is enforced and case does not match") {
            // Arrange
            val input = "value_one"
            val defaultValue = SampleEnum.VALUE_TWO

            // Act
            val result = input.toEnum(defaultValue, ignoreCase = false)

            // Assert
            result shouldBe defaultValue
        }
    })

private enum class SampleEnum {
    VALUE_ONE,
    VALUE_TWO,
    VALUE_THREE
}
