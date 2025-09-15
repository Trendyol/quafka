package com.trendyol.quafka.examples.spring.configuration

import com.trendyol.quafka.consumer.QuafkaConsumer
import com.trendyol.quafka.consumer.configuration.QuafkaConsumerBuilder
import com.trendyol.quafka.examples.spring.consumers.Consumer
import com.trendyol.quafka.logging.LoggerHelper
import io.github.embeddedkafka.*
import kotlinx.coroutines.*
import org.apache.kafka.clients.admin.*
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.ByteArrayDeserializer
import org.slf4j.Logger
import org.springframework.context.annotation.*
import java.util.concurrent.*

@Configuration
class QuafkaConsumerConfiguration {
    private val logger: Logger = LoggerHelper.createLogger(this.javaClass)
    private val coroutineScope: CoroutineScope = CoroutineScope(Dispatchers.Unconfined)

    @Bean
    fun consumer(
        embeddedKafka: EmbeddedK,
        embeddedKafkaConfig: EmbeddedKafkaConfig,
        kafkaAdmin: AdminClient,
        consumers: List<Consumer<ByteArray, ByteArray>>
    ): QuafkaConsumer<ByteArray, ByteArray> {
        kafkaAdmin.createTopics(
            KafkaConfig.Topics.alltopics,
            CreateTopicsOptions()
        )

        val properties = mutableMapOf<String, Any>()
        properties[ConsumerConfig.CLIENT_ID_CONFIG] = KafkaConfig.clientId
        properties[ConsumerConfig.AUTO_OFFSET_RESET_CONFIG] = "latest"
        properties[ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG] = true
        properties[ConsumerConfig.MAX_POLL_RECORDS_CONFIG] = "10"
        properties[ConsumerConfig.ALLOW_AUTO_CREATE_TOPICS_CONFIG] = "false"
        properties[ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG] = embeddedKafkaConfig.bootstrapServers()
        properties[ConsumerConfig.GROUP_ID_CONFIG] = KafkaConfig.groupId
        properties[ConsumerConfig.AUTO_OFFSET_RESET_CONFIG] = "earliest"
        val byteArrayDeserializer = ByteArrayDeserializer()
        val consumer =
            QuafkaConsumerBuilder<ByteArray, ByteArray>(properties)
                .withDeserializer(byteArrayDeserializer, byteArrayDeserializer)
                .withDispatcher(createNamedDispatcher("OEDispatcher", 2))
                .let { builder ->
                    consumers.map { c -> c.configure(builder) }.last()
                }.build()

        coroutineScope.launch {
            consumer.eventPublisher.subscribe {
                logger.info("EVENT: $it")
            }
        }

        consumer.start()
        return consumer
    }
}

fun createNamedDispatcher(name: String, threadCount: Int): CoroutineDispatcher {
    val threadFactory = ThreadFactory { runnable ->
        Thread(runnable, "$name-thread-${Thread.currentThread().threadId()}")
    }
    val executor = Executors.newFixedThreadPool(threadCount, threadFactory)
    return executor.asCoroutineDispatcher()
}
