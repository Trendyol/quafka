package com.trendyol.quafka.extensions.errorHandling.recoverer

/**
 * A structured report containing the original exception and a processed detail string.
 * Used for logging and error handling decisions.
 */
data class ExceptionDetails(val exception: Throwable, val detail: String)

/**
 * A function type that takes a Throwable and produces an [ExceptionDetails].
 */
typealias ExceptionDetailsProvider = (Throwable) -> ExceptionDetails
