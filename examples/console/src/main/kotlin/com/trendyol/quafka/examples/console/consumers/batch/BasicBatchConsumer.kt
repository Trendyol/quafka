package com.trendyol.quafka.examples.console.consumers.batch

import com.trendyol.quafka.consumer.ackAll
import com.trendyol.quafka.consumer.configuration.QuafkaConsumerBuilder
import com.trendyol.quafka.consumer.configuration.withDeserializer
import com.trendyol.quafka.examples.console.consumers.ConsumerBase
import org.apache.kafka.common.serialization.StringDeserializer

/**
 * Basic batch consumer processing multiple messages at once.
 *
 * **What it shows:**
 * - Batch message handler (process multiple messages together)
 * - Iterating through message batches
 * - Bulk acknowledgment with `ackAll()`
 * - Simple sequential batch processing
 *
 * **Use when:** Processing messages in bulk is more efficient than one-by-one.
 */
class BasicBatchConsumer(properties: MutableMap<String, Any>) : ConsumerBase(properties) {
    override suspend fun run() {
        val consumer =
            QuafkaConsumerBuilder<String, String>(properties)
                .withDeserializer(StringDeserializer())
                .subscribe("topic-example") {
                    withBatchMessageHandler { incomingMessages, consumerContext ->
                        for (incomingMessage in incomingMessages) {
                            logger.info("Received message: ${incomingMessage.toString(addValue = true, addHeaders = true)}")
                        }
                        incomingMessages.ackAll()
                    }
                }.build()

        consumer.start()
    }
}
