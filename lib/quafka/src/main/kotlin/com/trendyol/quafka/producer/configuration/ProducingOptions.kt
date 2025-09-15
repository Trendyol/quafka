package com.trendyol.quafka.producer.configuration

import org.apache.kafka.common.errors.*

/**
 * Configuration options for handling errors and exceptions during message production.
 *
 * @property stopOnFirstError If `true`, stops producing messages upon encountering the first error.
 * @property throwException If `true`, throws an exception when an error occurs. Defaults to `false`.
 * @property unrecoverableExceptions A collection of exceptions that are considered unrecoverable.
 * Defaults to [AuthenticationException] and [ProducerFencedException].
 */
data class ProducingOptions(
    val stopOnFirstError: Boolean,
    val throwException: Boolean = false,
    val unrecoverableExceptions: Collection<Class<out Throwable>> = defaultUnrecoverableException
) {
    /**
     * Companion object holding default values for unrecoverable exceptions.
     */
    companion object {
        private val defaultUnrecoverableException: MutableSet<Class<out Throwable>> = mutableSetOf(
            AuthenticationException::class.java,
            ProducerFencedException::class.java
        )
    }

    /**
     * Checks whether the given exception is considered unrecoverable.
     *
     * @param exception The exception to check.
     * @return `true` if the exception is in the list of unrecoverable exceptions, `false` otherwise.
     */
    fun isUnrecoverableException(exception: Throwable): Boolean = unrecoverableExceptions.contains(exception.javaClass)
}
