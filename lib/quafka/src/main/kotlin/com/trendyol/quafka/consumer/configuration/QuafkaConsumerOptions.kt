package com.trendyol.quafka.consumer.configuration

import com.trendyol.quafka.common.TimeProvider
import com.trendyol.quafka.consumer.IncomingMessageStringFormatter
import com.trendyol.quafka.events.EventBus
import io.github.resilience4j.retry.Retry
import kotlinx.coroutines.*
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.Deserializer
import kotlin.time.Duration

class QuafkaConsumerOptions<TKey, TValue> internal constructor(
    val properties: Map<String, Any>,
    val keyDeserializer: Deserializer<TKey>?,
    val valueDeserializer: Deserializer<TValue>?,
    val timeProvider: TimeProvider,
    val coroutineExceptionHandler: CoroutineExceptionHandler,
    val connectionRetryPolicy: Retry,
    val incomingMessageStringFormatter: IncomingMessageStringFormatter,
    val blockOnStart: Boolean,
    val pollDuration: Duration,
    val commitOptions: CommitOptions,
    val dispatcher: CoroutineDispatcher,
    val subscriptions: Collection<TopicSubscriptionOptions<TKey, TValue>>,
    val eventBus: EventBus,
    val gracefulShutdownTimeout: Duration
) {
    fun getSubscriptionOptionsByTopicName(
        topic: String
    ): TopicSubscriptionOptions<TKey, TValue>? = subscriptions.singleOrNull { it.topic == topic }

    fun hasSubscription() = subscriptions.isNotEmpty()

    fun subscribedTopics(): Collection<String> = subscriptions.map { it.topic }

    internal fun getClientId(): String = this.properties[ConsumerConfig.CLIENT_ID_CONFIG]?.toString() ?: ""

    internal fun getGroupId(): String = this.properties[ConsumerConfig.GROUP_ID_CONFIG]?.toString() ?: ""
}
