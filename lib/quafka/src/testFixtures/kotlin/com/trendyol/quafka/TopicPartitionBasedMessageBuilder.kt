package com.trendyol.quafka

import org.apache.kafka.common.TopicPartition
import java.util.concurrent.atomic.AtomicLong

class TopicPartitionBasedMessageBuilder<TKey, TValue>(
    val topic: String,
    val partition: Int
) {
    constructor(topicPartition: TopicPartition) : this(topicPartition.topic(), topicPartition.partition())

    val offsets = AtomicLong(-1)

    fun new(
        key: TKey,
        value: TValue,
        offset: Long = offsets.incrementAndGet()
    ): IncomingMessageBuilder<TKey, TValue> = IncomingMessageBuilder(
        topic = topic,
        partition = partition,
        offset = offset,
        key = key,
        value = value
    )
}
