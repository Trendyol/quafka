package com.trendyol.quafka.examples.spring.configuration

import io.github.embeddedkafka.*
import org.apache.kafka.clients.admin.*
import org.springframework.context.annotation.*

@Configuration
class KafkaAdminConfiguration {
    @Bean
    fun kafkaAdmin(embeddedKafka: EmbeddedK, embeddedKafkaConfig: EmbeddedKafkaConfig): AdminClient {
        val properties = mutableMapOf<String, Any>()
        properties[AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG] = embeddedKafkaConfig.bootstrapServers()
        val admin = KafkaAdminClient.create(properties)
        return admin
    }
}
