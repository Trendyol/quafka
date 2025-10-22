package com.trendyol.quafka.consumer

import com.trendyol.quafka.consumer.configuration.QuafkaConsumerOptions
import kotlinx.coroutines.isActive
import kotlin.coroutines.CoroutineContext

/**
 * Represents the context for a Kafka consumer, encapsulating essential details about the consumer's
 * lifecycle, partition, and execution environment.
 *
 * @property topicPartition The [TopicPartition] being processed by the consumer.
 * @property coroutineContext The [CoroutineContext] associated with the consumer's processing tasks.
 * @property consumerOptions The [QuafkaConsumerOptions]
 */
class ConsumerContext(
    val topicPartition: TopicPartition,
    val coroutineContext: CoroutineContext,
    val consumerOptions: QuafkaConsumerOptions<*, *>
) {
    /**
     * Indicates whether the consumer's coroutine context is currently active.
     *
     * @return `true` if the context is active, `false` otherwise.
     */
    val isActive: Boolean
        get() = coroutineContext.isActive
}
