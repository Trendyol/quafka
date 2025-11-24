package com.trendyol.quafka.extensions.errorHandling.recoverer

import kotlin.reflect.KClass

/**
 * Type-safe wrapper for retry policy identifiers.
 *
 * This inline value class provides compile-time safety for policy identifiers,
 * preventing typos and mixing up identifiers with regular strings.
 *
 * **IMPORTANT**: The identifier must uniquely identify the exception scenario being retried.
 * When exception types change between attempts, the identifier changes, resetting the per-policy
 * attempt counter (while the overall counter continues).
 *
 * Example:
 * ```kotlin
 * val identifier = PolicyIdentifier("DatabaseConcurrencyException")
 * val policy = RetryPolicy.FullRetry(identifier, ...)
 * ```
 */
@JvmInline
value class PolicyIdentifier(val value: String) {
    init {
        require(value.isNotBlank()) { "Policy identifier cannot be blank" }
    }

    override fun toString(): String = value

    companion object {
        /**
         * Create a PolicyIdentifier from a string value.
         */
        fun of(value: String) = PolicyIdentifier(value)

        /**
         * Creates a special "none" identifier used when no policy identifier is set.
         *
         * This is typically used as a sentinel value to indicate that a message
         * has not been through a retry policy yet.
         *
         * @return A [PolicyIdentifier] with value "N/A".
         */
        fun none() = PolicyIdentifier("N/A")
    }
}

/**
 * Helper object for creating consistent policy identifiers based on exception scenarios.
 *
 * Provides conventions for identifier naming to ensure uniqueness and clarity.
 *
 * Example:
 * ```kotlin
 * // By exception class
 * PolicyIdentifiers.forException<DatabaseConcurrencyException>()
 *
 * // By exception instance
 * PolicyIdentifiers.forException(exception)
 *
 * // By exception class with error code
 * PolicyIdentifiers.forExceptionWithCode(SQLException::class, 1205)
 *
 * // Custom
 * PolicyIdentifiers.custom("PaymentGateway-Timeout")
 * ```
 */
object PolicyIdentifiers {
    /**
     * Create identifier from exception class type.
     *
     * Example: `DatabaseConcurrencyException`
     */
    inline fun <reified T : Throwable> forException(): PolicyIdentifier =
        PolicyIdentifier(T::class.simpleName ?: T::class.toString())

    /**
     * Create identifier from exception class.
     *
     * Example: `DatabaseConcurrencyException`
     */
    fun forException(exceptionClass: KClass<out Throwable>): PolicyIdentifier =
        PolicyIdentifier(exceptionClass.simpleName ?: exceptionClass.toString())

    /**
     * Create identifier from exception instance (uses class name).
     *
     * Example: `DatabaseConcurrencyException`
     */
    fun forException(exception: Throwable): PolicyIdentifier =
        forException(exception::class)

    /**
     * Create identifier from exception class and error code.
     *
     * Useful for exceptions like SQLException where error code distinguishes the scenario.
     *
     * Example: `SQLException-1205` (MySQL deadlock)
     */
    fun forExceptionWithCode(exceptionClass: KClass<out Throwable>, errorCode: Int): PolicyIdentifier =
        PolicyIdentifier("${exceptionClass.simpleName ?: exceptionClass}-$errorCode")

    /**
     * Create identifier from exception class and error code.
     */
    inline fun <reified T : Throwable> forExceptionWithCode(errorCode: Int): PolicyIdentifier =
        forExceptionWithCode(T::class, errorCode)

    /**
     * Create identifier from service/domain and error type.
     *
     * Useful for organizing retries by business domain.
     *
     * Example: `PaymentGateway-Timeout`, `ProductService-NetworkError`
     */
    fun forServiceError(serviceName: String, errorType: String): PolicyIdentifier =
        PolicyIdentifier("$serviceName-$errorType")

    /**
     * Create identifier from a string value.
     *
     * Use when none of the convention helpers fit your scenario.
     *
     * Alias for [PolicyIdentifier.of] for consistency.
     */
    fun of(identifier: String): PolicyIdentifier =
        PolicyIdentifier(identifier)
}
