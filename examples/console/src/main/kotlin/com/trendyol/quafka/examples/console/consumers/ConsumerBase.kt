package com.trendyol.quafka.examples.console.consumers

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.slf4j.*

abstract class ConsumerBase(
    val servers: String
) {
    companion object {
        val logger: Logger = LoggerFactory.getLogger(ConsumerBase::class.java)
    }

    protected open fun createProperties(): MutableMap<String, Any> {
        val properties = HashMap<String, Any>()
        properties[ConsumerConfig.CLIENT_ID_CONFIG] = "client-id"
        properties[ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG] = servers
        properties[ConsumerConfig.GROUP_ID_CONFIG] = "group-id"
        return properties
    }

    protected abstract fun run(properties: MutableMap<String, Any>)

    fun run() {
        val properties = createProperties()
        run(properties)
    }
}
