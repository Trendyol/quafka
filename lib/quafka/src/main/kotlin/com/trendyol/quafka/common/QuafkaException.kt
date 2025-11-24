package com.trendyol.quafka.common

/**
 * Base exception class for Quafka-related errors.
 *
 * This class serves as a custom exception for Quafka, encapsulating error details and providing a foundation
 * for more specific exceptions within the library. It extends [RuntimeException] to allow unchecked exception handling.
 *
 * @param message A descriptive message explaining the error.
 * @param cause The underlying cause of the exception, if any. Defaults to `null`.
 */
open class QuafkaException(message: String, cause: Throwable? = null) : RuntimeException(message, cause)
