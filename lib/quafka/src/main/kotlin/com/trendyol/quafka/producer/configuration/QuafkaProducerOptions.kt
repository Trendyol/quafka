package com.trendyol.quafka.producer.configuration

import com.trendyol.quafka.common.TimeProvider
import com.trendyol.quafka.producer.OutgoingMessageStringFormatter
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.Serializer

class QuafkaProducerOptions<TKey, TValue> internal constructor(
    val properties: Map<String, Any>,
    val timeProvider: TimeProvider,
    val outgoingMessageStringFormatter: OutgoingMessageStringFormatter,
    val keySerializer: Serializer<TKey>?,
    val valueSerializer: Serializer<TValue>?,
    val producingOptions: ProducingOptions
) {
    fun clientId(): Any? = this.properties[ProducerConfig.CLIENT_ID_CONFIG]
}
