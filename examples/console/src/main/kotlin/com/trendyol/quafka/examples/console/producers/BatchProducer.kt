package com.trendyol.quafka.examples.console.producers

import com.trendyol.quafka.producer.OutgoingMessage
import com.trendyol.quafka.producer.configuration.QuafkaProducerBuilder
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.StringSerializer
import kotlin.system.measureTimeMillis

/**
 * High-throughput batch producer with performance optimizations.
 *
 * **What it shows:**
 * - Batch sending with `sendAll()` for better throughput
 * - Producer optimization: larger batch size, linger time, compression
 * - Performance measurement and throughput calculation
 * - Configurable message count
 *
 * **Configuration:**
 * - Batch size: 32KB (larger batches = better throughput)
 * - Linger time: 20ms (wait to fill batches)
 * - Compression: LZ4 (fast compression)
 *
 * **Use when:** Sending high volume of messages or bulk operations.
 */
class BatchProducer(properties: MutableMap<String, Any>, val messageCount: Int) : ProducerBase(properties) {
    override fun createProperties(baseProperties: MutableMap<String, Any>): MutableMap<String, Any> {
        super.createProperties(baseProperties)
        baseProperties.putIfAbsent(ProducerConfig.BATCH_SIZE_CONFIG, 32768)
        baseProperties.putIfAbsent(ProducerConfig.LINGER_MS_CONFIG, 20)
        baseProperties.putIfAbsent(ProducerConfig.COMPRESSION_TYPE_CONFIG, "lz4")
        return baseProperties
    }

    override suspend fun run() {
        val producer = QuafkaProducerBuilder<String, String>(properties)
            .withSerializer(StringSerializer(), StringSerializer())
            .build()

        try {
            logger.info("=== Batch Producer Example ===")

            val messages = (1..messageCount).map { index ->
                OutgoingMessage.create(
                    topic = "demo-topic",
                    key = "batch-key-$index",
                    value = "Batch message $index"
                )
            }

            val timeMillis = measureTimeMillis {
                val results = producer.sendAll(messages)
                logger.info("Batch sent: {} messages", results.size)
                logger.info(
                    "First topic: {}, Last topic: {}",
                    results.first().recordMetadata.topic(),
                    results.last().recordMetadata.topic()
                )
            }

            logger.info(
                "Batch completed in {} ms ({} msg/sec)",
                timeMillis,
                if (timeMillis > 0) messageCount * 1000 / timeMillis else 0
            )
            logger.info("=== Batch Producer completed successfully ===")
        } finally {
            producer.close()
        }
    }
}
