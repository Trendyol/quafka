package com.trendyol.quafka.consumer

import org.apache.kafka.common.TopicPartition

/**
 * Represents a Kafka topic partition and offset.
 *
 * @param topicPartition The topic and partition to which the offset belongs.
 * @param offset The offset for the topic partition.
 */
data class TopicPartitionOffset(
    val topicPartition: TopicPartition,
    val offset: Long
) {
    constructor(topic: String, partition: Int, offset: Long) : this(TopicPartition(topic, partition), offset)
}
