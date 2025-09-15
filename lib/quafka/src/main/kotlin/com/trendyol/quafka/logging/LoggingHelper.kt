package com.trendyol.quafka.logging

import com.trendyol.quafka.consumer.*
import org.slf4j.*
import org.slf4j.spi.LoggingEventBuilder

object LoggerHelper {
    private val logLevelDetectionLogger: Logger = LoggerFactory.getLogger(LoggerHelper::class.java)

    fun createLogger(
        clazz: Class<*>,
        suffix: String = "",
        keyValuePairs: Map<String, Any> = mapOf()
    ): Logger = createLogger(clazz.name, suffix, keyValuePairs)

    fun createLogger(
        name: String,
        suffix: String = "",
        keyValuePairs: Map<String, Any> = mapOf()
    ): Logger {
        if (logLevelDetectionLogger.isTraceEnabled || logLevelDetectionLogger.isDebugEnabled) {
            return QuafkaLogger(LoggerFactory.getLogger("$name::$suffix"), keyValuePairs)
        }
        return QuafkaLogger(LoggerFactory.getLogger(name), keyValuePairs)
    }
}

fun LoggingEventBuilder.enrichWithConsumerContext(consumerContext: ConsumerContext): LoggingEventBuilder {
    consumerContext.createConsumerContextKeyValuePairs().forEach { (k, v) -> this.addKeyValue(k, v) }
    return this
}

fun ConsumerContext.createConsumerContextKeyValuePairs(
    configure: MutableMap<String, Any>.() -> Unit = {
    }
): MutableMap<String, Any> = mutableMapOf<String, Any>(
    LogParameters.CLIENT_ID to this.consumerOptions.getClientId(),
    LogParameters.GROUP_ID to this.consumerOptions.getGroupId(),
    LogParameters.TOPIC to topicPartition.topic(),
    LogParameters.PARTITION to topicPartition.partition()
).apply(configure)

object LogParameters {
    const val CLIENT_ID = "clientId"
    const val GROUP_ID = "groupId"
    const val TOPIC = "topic"
    const val PARTITION = "partition"
}
