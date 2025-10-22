package com.trendyol.quafka.consumer

import org.apache.kafka.common.TopicPartition

typealias TopicPartition = org.apache.kafka.common.TopicPartition

val TopicPartition.topic: String
    get() = this.topic()

val TopicPartition.partition: Int
    get() = this.partition()

fun TopicPartition.toLogString(): String = "topic: $topic | partition: $partition"
