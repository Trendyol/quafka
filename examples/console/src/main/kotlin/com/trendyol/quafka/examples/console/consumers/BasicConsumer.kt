package com.trendyol.quafka.examples.console.consumers

import com.trendyol.quafka.consumer.configuration.*
import org.apache.kafka.common.serialization.StringDeserializer

class BasicConsumer(
    servers: String
) : ConsumerBase(servers) {
    override fun run(properties: MutableMap<String, Any>) {
        val deserializer = StringDeserializer()
        val consumer =
            QuafkaConsumerBuilder<String, String>(properties)
                .withDeserializer(deserializer, deserializer)
                .subscribe("topic-example", "aa") {
                    withBatchMessageHandler { incomingMessages, consumerContext ->
                    }
                }.build()

        consumer.start()
    }
}
