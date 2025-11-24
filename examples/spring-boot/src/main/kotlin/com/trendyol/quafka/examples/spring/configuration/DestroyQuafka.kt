package com.trendyol.quafka.examples.spring.configuration

import com.trendyol.quafka.consumer.QuafkaConsumer
import com.trendyol.quafka.producer.QuafkaProducer
import jakarta.annotation.PreDestroy
import org.springframework.context.annotation.Configuration

@Configuration
class DestroyQuafka(private val consumer: QuafkaConsumer<*, *>, private val producer: QuafkaProducer<*, *>) {
    @PreDestroy
    fun closeConsumer() {
        consumer.stop()
    }

    @PreDestroy
    fun closeProducer() {
        producer.close()
    }
}
