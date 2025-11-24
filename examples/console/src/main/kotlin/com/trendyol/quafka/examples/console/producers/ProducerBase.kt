package com.trendyol.quafka.examples.console.producers

import org.apache.kafka.clients.producer.ProducerConfig
import org.slf4j.Logger
import org.slf4j.LoggerFactory

/**
 * Base class for all producer examples with common configuration.
 *
 * **Default configuration:**
 * - `acks=all` - Wait for all replicas (strongest durability)
 * - `enable.idempotence=true` - Prevent duplicate messages
 *
 * Extend this class and override `createProperties()` to add specific configs.
 */
abstract class ProducerBase(properties: MutableMap<String, Any>) {
    protected val logger: Logger = LoggerFactory.getLogger(this::class.java)

    protected val properties: MutableMap<String, Any> = createProperties(properties)

    protected open fun createProperties(baseProperties: MutableMap<String, Any>): MutableMap<String, Any> {
        baseProperties.putIfAbsent(ProducerConfig.ACKS_CONFIG, "all")
        baseProperties.putIfAbsent(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true)
        return baseProperties
    }

    abstract suspend fun run()
}
