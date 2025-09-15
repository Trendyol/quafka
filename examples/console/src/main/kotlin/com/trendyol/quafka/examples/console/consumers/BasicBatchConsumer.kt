package com.trendyol.quafka.examples.console.consumers

import com.trendyol.quafka.consumer.ackAll
import com.trendyol.quafka.consumer.configuration.*
import org.apache.kafka.common.serialization.StringDeserializer

class BasicBatchConsumer(
    servers: String
) : ConsumerBase(servers) {
    override fun run(properties: MutableMap<String, Any>) {
        val deserializer = StringDeserializer()
        val consumer =
            QuafkaConsumerBuilder<String, String>(properties)
                .withDeserializer(deserializer, deserializer)
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
