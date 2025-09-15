package com.trendyol.quafka.examples.console

import com.fasterxml.jackson.databind.ObjectMapper
import com.trendyol.quafka.common.header
import com.trendyol.quafka.consumer.*
import com.trendyol.quafka.consumer.configuration.QuafkaConsumerBuilder
import com.trendyol.quafka.extensions.producer.OutgoingMessageBuilder
import com.trendyol.quafka.extensions.serialization.json.ByteArrayJsonMessageSerde
import com.trendyol.quafka.extensions.serialization.json.typeResolvers.*
import com.trendyol.quafka.producer.QuafkaProducer
import com.trendyol.quafka.producer.configuration.*
import kotlinx.coroutines.*
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.*
import org.slf4j.LoggerFactory
import org.slf4j.event.Level
import java.time.Instant
import kotlin.time.Duration.Companion.seconds
import kotlin.time.measureTimedValue

fun main(args: Array<String>) =
    runBlocking {
        App(args).run()
    }

class App(
    private val args: Array<String>
) {
    private val clientId = Instant.now().epochSecond.toString()
    private val mode = System.getenv("Q_MODE") ?: "producer"
    private val servers = System.getenv("Q_SERVERS")

    companion object {
        private val logger = LoggerFactory.getLogger(App::class.java)
    }

    fun run() {
        if (servers.isNullOrEmpty()) {
            error("Servers not configured")
        }
        when (mode) {
            "producer" -> runBlocking { startProducing(topic = "topic1", messageSize = 1000) }
            "consumer" -> {
                val consumer =
                    createConsumer(
                        clientId = clientId,
                        group = "g_$clientId",
                        batch = false,
                        topics = listOf("topic1"),
                        handler = { incomingMessage, _ ->
                            logger.info(
                                "message processed. | {}",
                                incomingMessage.toString(logLevel = Level.INFO, addValue = true, addHeaders = true)
                            )
                        }
                    ) {
                        it.withStartupOptions(blockOnStart = false)
                    }

                consumer.start()
            }

            else -> error("undefined mode $mode")
        }
    }

    private suspend fun startProducing(topic: String, messageSize: Int = 10) {
        data class Request(
            val id: Int
        )

        val producer = createProducer(clientId)
        val serializer = ByteArrayJsonMessageSerde(ObjectMapper(), AutoPackageNameBasedTypeResolver(HeaderAwareTypeNameExtractor()))
        val outgoingMessageBuilder = OutgoingMessageBuilder(serializer)
        val messages = (1..messageSize).map {
            outgoingMessageBuilder
                .newMessageWithTypeInfo(topic, Request(it), it.toString())
                .withHeader(header("X-Header", clientId))
                .build()
        }

        val result = measureTimedValue {
            producer.sendAll(messages)
        }
        logger.info("messages were sent to kafka at ${result.duration}.")
    }

    private fun createProducer(clientId: String): QuafkaProducer<ByteArray?, ByteArray?> {
        val props = HashMap<String, Any>()
        props[ProducerConfig.CLIENT_ID_CONFIG] = clientId
        props[ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG] = servers

        val byteArraySerializer = ByteArraySerializer()
        val producer =
            QuafkaProducerBuilder<ByteArray?, ByteArray?>(props)
                .withClientId(clientId)
                .withSerializer(byteArraySerializer, byteArraySerializer)
                .withErrorOptions(ProducingOptions(true))
                .build()
        return producer
    }

    private fun createConsumer(
        clientId: String,
        group: String,
        batch: Boolean = false,
        topics: Collection<String>,
        handler: suspend (incomingMessage: IncomingMessage<ByteArray, ByteArray>, consumerContext: ConsumerContext) -> Unit,
        configure: (builder: QuafkaConsumerBuilder<*, *>) -> Unit = {
        }
    ): QuafkaConsumer<ByteArray, ByteArray> {
        val properties = HashMap<String, Any>()
        properties[ConsumerConfig.CLIENT_ID_CONFIG] = "my-client-$clientId"
        properties[ConsumerConfig.AUTO_OFFSET_RESET_CONFIG] = "latest"
        properties[ConsumerConfig.MAX_POLL_RECORDS_CONFIG] = "10"
        properties[ConsumerConfig.ALLOW_AUTO_CREATE_TOPICS_CONFIG] = "false"
        properties[ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG] = servers
        properties[ConsumerConfig.GROUP_ID_CONFIG] = "quafka.$group"
        properties[ConsumerConfig.AUTO_OFFSET_RESET_CONFIG] = "earliest"

        val byteArrayDeserializer = ByteArrayDeserializer()

        val consumer =
            QuafkaConsumerBuilder<ByteArray, ByteArray>(properties)
                .withDeserializer(byteArrayDeserializer, byteArrayDeserializer)
                .apply(configure)
                .subscribe(*topics.toTypedArray()) {
                    if (batch) {
                        withBatchMessageHandler(handler = { incomingMessages, consumerContext ->
                            coroutineScope {
                                incomingMessages
                                    .map {
                                        async {
                                            handler(it, consumerContext)
                                        }
                                    }.awaitAll()
                            }
                        }, batchSize = 5, batchTimeout = 5.seconds)
                            .autoAckAfterProcess(true)
                    } else {
                        withSingleMessageHandler { incomingMessage, consumerContext ->
                            handler(incomingMessage, consumerContext)
                        }.autoAckAfterProcess(true)
                    }
                }.build()

        return consumer
    }
}
