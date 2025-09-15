package com.trendyol.quafka.examples.spring.configuration

import com.fasterxml.jackson.databind.ObjectMapper
import com.trendyol.quafka.examples.spring.configuration.KafkaConfig.clientId
import com.trendyol.quafka.extensions.producer.OutgoingMessageBuilder
import com.trendyol.quafka.extensions.serialization.json.*
import com.trendyol.quafka.extensions.serialization.json.typeResolvers.*
import com.trendyol.quafka.producer.QuafkaProducer
import com.trendyol.quafka.producer.configuration.*
import io.github.embeddedkafka.*
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.ByteArraySerializer
import org.springframework.context.annotation.*

@Configuration
class QuafkaProducerConfiguration {
    @Bean
    fun outgoingMessageBuilder(): OutgoingMessageBuilder<ByteArray?, ByteArray?> {
        val serializer = ByteArrayJsonMessageSerde(
            ObjectMapper(),
            AutoPackageNameBasedTypeResolver(HeaderAwareTypeNameExtractor())
        )
        val outgoingMessageBuilder = OutgoingMessageBuilder(serializer)
        return outgoingMessageBuilder
    }

    @Bean
    fun producer(
        embeddedKafka: EmbeddedK,
        embeddedKafkaConfig: EmbeddedKafkaConfig
    ): QuafkaProducer<ByteArray, ByteArray> {
        val props = HashMap<String, Any>()
        props[ProducerConfig.CLIENT_ID_CONFIG] = clientId
        props[ProducerConfig.BOOTSTRAP_SERVERS_CONFIG] = embeddedKafkaConfig.bootstrapServers()
        props[ProducerConfig.PARTITIONER_CLASS_CONFIG] = "org.apache.kafka.clients.producer.RoundRobinPartitioner"

        val byteArraySerializer = ByteArraySerializer()
        val producer =
            QuafkaProducerBuilder<ByteArray, ByteArray>(props)
                .withClientId(clientId)
                .withSerializer(byteArraySerializer, byteArraySerializer)
                .withErrorOptions(ProducingOptions(true))
                .build()
        return producer
    }
}
