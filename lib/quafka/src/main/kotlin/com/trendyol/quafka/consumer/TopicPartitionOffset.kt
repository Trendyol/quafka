package com.trendyol.quafka.consumer

import org.apache.kafka.clients.consumer.OffsetAndMetadata
import kotlin.jvm.optionals.getOrNull

/**
 * Represents a Kafka topic partition and offset.
 *
 * @param topic The topic to which the offset belongs.
 * @param partition The partition to which the offset belongs.
 * @param offset The offset for the topic partition.
 */
data class TopicPartitionOffset(
    val topic: String,
    val partition: Int,
    val offset: Long
) {
    constructor(topicPartition: TopicPartition, offset: Long) : this(topicPartition.topic, topicPartition.partition, offset)

    val topicPartition: TopicPartition
        get() = TopicPartition(topic, partition)

    override fun toString(): String = "topic: $topic | partition: $partition | offset: $offset"
}

internal fun Collection<TopicPartition>.toLogString(): String = "[${
    this.joinToString {
        "( ${it.toLogString()} )"
    }
}]"

internal fun Map<TopicPartition, OffsetAndMetadata>.toLogString(): String = "[${
    this.entries.joinToString {
        "( ${it.key.toLogString()} | offset: ${it.value.offset()} | metadata: ${it.value.metadata()} | epoch: ${
            it.value.leaderEpoch().getOrNull() ?: ""
        } )"
    }
}]"
