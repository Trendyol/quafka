package com.trendyol.quafka.examples.console.consumers.single

import com.trendyol.quafka.consumer.configuration.*
import com.trendyol.quafka.examples.console.consumers.ConsumerBase
import org.apache.kafka.common.serialization.StringDeserializer

/**
 * Basic consumer example demonstrating simple message processing.
 *
 * **What it shows:**
 * - Creating a consumer with String deserializer
 * - Single message handler pattern
 * - Manual acknowledgment with `ack()`
 * - Synchronous message processing
 *
 * **Use when:** Learning Kafka basics or simple sequential processing.
 */
class BasicConsumer(properties: MutableMap<String, Any>) : ConsumerBase(properties) {
    override suspend fun run() {
        val deserializer = StringDeserializer()
        val consumer =
            QuafkaConsumerBuilder<String, String>(properties)
                .withDeserializer(deserializer)
                .subscribe("topic-example", "aa") {
                    withSingleMessageHandler { incomingMessage, consumerContext ->
                        // process incomingMessage
                        incomingMessage.ack()
                    }
                }.build()

        consumer.start()
    }
}
