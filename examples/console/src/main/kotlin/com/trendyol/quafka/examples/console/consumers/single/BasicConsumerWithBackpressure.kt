package com.trendyol.quafka.examples.console.consumers.single

import com.trendyol.quafka.consumer.configuration.*
import com.trendyol.quafka.examples.console.consumers.ConsumerBase
import org.apache.kafka.common.serialization.StringDeserializer
import kotlin.time.Duration.Companion.minutes

/**
 * Consumer with backpressure control for rate limiting.
 *
 * **What it shows:**
 * - Configuring backpressure buffer (max pending messages)
 * - Setting release timeout for buffer management
 * - Preventing consumer overload
 * - Message logging with headers and values
 *
 * **Configuration:**
 * - Buffer size: 10,000 messages
 * - Release timeout: 1 minute
 *
 * **Use when:** Consumer is slower than producer, or need to control memory usage.
 */
class BasicConsumerWithBackpressure(properties: MutableMap<String, Any>) : ConsumerBase(properties) {
    override suspend fun run() {
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
