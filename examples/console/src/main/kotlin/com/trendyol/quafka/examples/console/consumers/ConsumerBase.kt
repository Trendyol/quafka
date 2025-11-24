package com.trendyol.quafka.examples.console.consumers

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.slf4j.*

/**
 * Base class for all consumer examples with common configuration.
 *
 * **Default configuration:**
 * - `enable.auto.commit=false` - Manual commit control
 * - `auto.offset.reset=earliest` - Read from beginning if no offset
 *
 * Extend this class and override `createProperties()` to add specific configs.
 */
abstract class ConsumerBase(properties: MutableMap<String, Any>) {
    companion object {
        val logger: Logger = LoggerFactory.getLogger(ConsumerBase::class.java)
    }

    protected val properties: MutableMap<String, Any> = createProperties(properties)

    protected open fun createProperties(baseProperties: MutableMap<String, Any>): MutableMap<String, Any> {
        baseProperties.putIfAbsent(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false)
        baseProperties.putIfAbsent(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
        return baseProperties
    }

    abstract suspend fun run()
}
