package com.trendyol.quafka.examples.console.consumers

import com.trendyol.quafka.consumer.configuration.*
import org.apache.kafka.common.serialization.StringDeserializer
import kotlin.time.Duration.Companion.minutes

class BasicConsumerWithBackpressure(
    servers: String
) : ConsumerBase(servers) {
    override fun run(properties: MutableMap<String, Any>) {
        val deserializer = StringDeserializer()
        val consumer =
            QuafkaConsumerBuilder<String, String>(properties)
                .withDeserializer(deserializer, deserializer)
                .subscribe("topic-example") {
                    withSingleMessageHandler { incomingMessage, consumerContext ->
                        logger.info("Received message: ${incomingMessage.toString(addValue = true, addHeaders = true)}")
                        incomingMessage.ack()
                    }.withBackpressure(backpressureBufferSize = 10000, backpressureReleaseTimeout = 1.minutes)
                }.build()

        consumer.start()
    }
}
