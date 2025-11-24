package com.trendyol.quafka

import com.trendyol.quafka.common.DefaultCharset
import com.trendyol.quafka.consumer.QuafkaConsumer
import com.trendyol.quafka.consumer.configuration.QuafkaConsumerBuilder
import com.trendyol.quafka.consumer.configuration.SubscriptionBuildStep
import com.trendyol.quafka.producer.OutgoingMessage
import com.trendyol.quafka.producer.QuafkaProducer
import com.trendyol.quafka.producer.configuration.QuafkaProducerBuilder
import io.github.embeddedkafka.EmbeddedK
import io.github.embeddedkafka.EmbeddedKafka
import io.github.embeddedkafka.EmbeddedKafkaConfig
import io.kotest.core.listeners.TestListener
import io.kotest.core.spec.Spec
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.admin.AdminClientConfig
import org.apache.kafka.clients.admin.NewPartitions
import org.apache.kafka.clients.admin.NewTopic
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.ByteArrayDeserializer
import org.apache.kafka.common.serialization.ByteArraySerializer
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.StringSerializer
import scala.collection.immutable.`Map$`
import java.nio.charset.Charset
import java.time.Instant
import java.util.Properties
import java.util.UUID
import java.util.concurrent.atomic.AtomicInteger
import kotlin.time.Duration.Companion.seconds

private val embeddedKafkaDefaultConfig = EmbeddedKafkaConfig
    .apply(
        0,
        0,
        `Map$`.`MODULE$`.empty(),
        `Map$`.`MODULE$`.empty(),
        `Map$`.`MODULE$`.empty()
    )
// val embeddedKafkaExtension: KafkaExtension = KafkaExtension(embeddedKafkaDefaultConfig)

class KafkaExtension(private val config: EmbeddedKafkaConfig = embeddedKafkaDefaultConfig) : TestListener {
    private lateinit var embeddedKafka: EmbeddedK

    override suspend fun afterSpec(spec: Spec) {
        if (!this::embeddedKafka.isInitialized) {
            start()
        }
    }

    override suspend fun beforeSpec(spec: Spec) {
        stop()
    }

    fun start() {
        println("Kafka starting, ${Instant.now()}")
        embeddedKafka = EmbeddedKafka.start(config)
        val runningServers = EmbeddedKafka.runningServers().list()
        while (!runningServers.contains(embeddedKafka)) {
            Thread.sleep(100)
        }
        println("Kafka started, ${Instant.now()}")
    }

    fun stop() {
        println("Kafka stopping, ${Instant.now()}")
        EmbeddedKafka.stop(embeddedKafka)
        println("Kafka stopped, ${Instant.now()}")
    }

    fun getBootstrapServers(): String {
        val port: Int = embeddedKafka!!.config().kafkaPort()
        val host: String = "127.0.0.1"
        val bootstrapServer = "$host:$port"
        return bootstrapServer
    }

    val topicSuffix = AtomicInteger(0)
    val topicPrefix = UUID.randomUUID().toString().substring(0, 8)

    fun createProducerProperties(): MutableMap<String, Any> {
        val props = mutableMapOf<String, Any>()
        props[ProducerConfig.BOOTSTRAP_SERVERS_CONFIG] = getBootstrapServers()
        return props
    }

    fun createConsumerProperties(): MutableMap<String, Any> {
        val props = mutableMapOf<String, Any>()
        props[ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG] = getBootstrapServers()
        props[ConsumerConfig.AUTO_OFFSET_RESET_CONFIG] = "earliest"
        props[ConsumerConfig.METADATA_MAX_AGE_CONFIG] = 3.seconds.inWholeMilliseconds.toString()
        //  props[ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG] = 30.seconds.inWholeMilliseconds.toString()
        // props[ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG] = "org.apache.kafka.clients.consumer.CooperativeStickyAssignor"
        return props
    }

    fun createAdminProperties(): MutableMap<String, Any> {
        val properties = mutableMapOf<String, Any>()
        properties[AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG] = getBootstrapServers()
        return properties
    }

    /**
     * Returns a kafka consumer configured with the details of the embedded broker.
     */
    fun createStringStringConsumer(configure: Properties.() -> Unit = {}): KafkaConsumer<String, String> {
        val props = createConsumerProperties()
        props[ConsumerConfig.GROUP_ID_CONFIG] = "test_consumer_group_" + System.currentTimeMillis()
        props[ConsumerConfig.AUTO_OFFSET_RESET_CONFIG] = "earliest"
        return KafkaConsumer(props, StringDeserializer(), StringDeserializer())
    }

    /**
     * Returns a kafka consumer subscribed to the given topic on the embedded broker.
     */
    fun createStringStringConsumer(
        topic: String,
        configure: Properties.() -> Unit = {}
    ): KafkaConsumer<String, String> {
        val consumer = createStringStringConsumer(configure)
        consumer.subscribe(listOf(topic))
        return consumer
    }

    /**
     * Returns a kafka consumer configured with the details of the embedded broker.
     */
    fun createBytesBytesConsumer(configure: Properties.() -> Unit = {}): KafkaConsumer<ByteArray, ByteArray> {
        val props = createConsumerProperties()
        props[ConsumerConfig.GROUP_ID_CONFIG] = "test_consumer_group_" + System.currentTimeMillis()
        props[ConsumerConfig.AUTO_OFFSET_RESET_CONFIG] = "earliest"
        return KafkaConsumer(props, ByteArrayDeserializer(), ByteArrayDeserializer())
    }

    /**
     * Returns a kafka consumer subscribed to the given topic on the embedded broker.
     */
    fun createBytesBytesConsumer(
        topic: String,
        configure: Properties.() -> Unit = {}
    ): KafkaConsumer<ByteArray, ByteArray> {
        val consumer = createBytesBytesConsumer(configure)
        consumer.subscribe(listOf(topic))
        return consumer
    }

    fun createBytesBytesProducer(configure: Properties.() -> Unit = {}): KafkaProducer<ByteArray, ByteArray> {
        val props = createProducerProperties()
        return KafkaProducer(props, ByteArraySerializer(), ByteArraySerializer())
    }

    fun createStringStringProducer(configure: Properties.() -> Unit = {}): KafkaProducer<String, String> {
        val props = createProducerProperties()
        return KafkaProducer(props, StringSerializer(), StringSerializer())
    }

    fun getRandomTopicName(): String = "topic_${topicPrefix}_${topicSuffix.incrementAndGet()}"

    fun createStringStringQuafkaConsumer(
        configure: (QuafkaConsumerBuilder<String, String>) -> SubscriptionBuildStep<String, String>
    ): QuafkaConsumer<String, String> {
        val deserializer = StringDeserializer()

        val consumer = QuafkaConsumerBuilder<String, String>(createConsumerProperties())
            .withGroupId("test-group")
            .withDeserializer(deserializer, deserializer)
            .let {
                configure(it)
            }

        return consumer.build()
    }

    fun createStringStringQuafkaProducer(
        configure: (QuafkaProducerBuilder<String, String>) -> QuafkaProducerBuilder<String, String> = {
            it
        }
    ): QuafkaProducer<String, String> {
        val serializer = StringSerializer()
        val consumer = QuafkaProducerBuilder<String, String>(createProducerProperties())
            .withSerializer(serializer, serializer)
            .let {
                configure(it)
            }

        return consumer.build()
    }

    fun newTopic(name: String, partition: Int) = NewTopic(name, partition, 1)

    fun createTopic(name: String, partition: Int) {
        createTopics(listOf(newTopic(name, partition)))
    }

    fun createTopics(topics: Collection<NewTopic>): Collection<NewTopic> {
        try {
            val admin = AdminClient.create(createAdminProperties())
            admin.createTopics(topics)
            admin.close()
        } catch (e: Exception) {
            // ignore
        }
        return topics
    }

    fun buildMessages(
        topics: Collection<NewTopic>,
        totalMessagePerPartition: Int = 10
    ): List<OutgoingMessage<String, String>> = topics
        .map { topic ->
            (0 until topic.numPartitions())
                .map { partition ->
                    (0 until totalMessagePerPartition).map { i ->
                        OutgoingMessage.create<String, String>(
                            topic = topic.name(),
                            partition = partition,
                            key = i.toString(),
                            value = "value",
                            headers = emptyList()
                        )
                    }
                }.flatten()
        }.flatten()

    fun increasePartitions(topics: Collection<NewTopic>): Collection<NewTopic> {
        try {
            val admin = AdminClient.create(createAdminProperties())
            admin.createPartitions(
                topics.associate { it.name() to NewPartitions.increaseTo(it.numPartitions()) }
            )
            admin.close()
        } catch (e: Exception) {
            print(e.message)
            // ignore
        }
        return topics
    }
}

fun ByteArray.toStringWithCharset(charset: Charset = DefaultCharset): String = this.toString(charset)

fun String.toByteArrayWithCharset(charset: Charset = DefaultCharset): ByteArray = this.toByteArray(charset)
