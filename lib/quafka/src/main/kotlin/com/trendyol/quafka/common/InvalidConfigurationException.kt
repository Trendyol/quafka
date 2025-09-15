package com.trendyol.quafka.common

/**
 * Exception class indicating an invalid configuration in Quafka.
 *
 * This exception is thrown when there is an issue with the configuration, such as invalid parameters or unsupported settings.
 *
 * @param message A descriptive message explaining the configuration issue.
 * @param cause The underlying cause of the exception, if any. Defaults to `null`.
 */
class InvalidConfigurationException(
    message: String,
    cause: Throwable? = null
) : QuafkaException(message, cause)
