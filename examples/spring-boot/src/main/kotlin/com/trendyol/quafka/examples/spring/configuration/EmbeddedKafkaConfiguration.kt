package com.trendyol.quafka.examples.spring.configuration

import io.github.embeddedkafka.*
import jakarta.annotation.PreDestroy
import org.springframework.context.annotation.*

@Configuration
class EmbeddedKafkaConfiguration {
    @Bean
    fun embeddedKafkaConfig(): EmbeddedKafkaConfig = EmbeddedKafkaConfig.defaultConfig()

    @Bean
    fun embeddedKafka(embeddedKafkaConfig: EmbeddedKafkaConfig): EmbeddedK {
        val kafka = EmbeddedKafka.start(embeddedKafkaConfig)
        while (!EmbeddedKafka.isRunning()) {
            Thread.sleep(100)
        }
        return kafka
    }

    @PreDestroy
    fun onShutdown() {
        EmbeddedKafka.stop()
    }
}

fun EmbeddedKafkaConfig.bootstrapServers(): String = "localhost:${this.kafkaPort()}"
