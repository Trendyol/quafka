package com.trendyol.quafka.consumer

import org.apache.kafka.common.TopicPartition

/**
 * Type alias for Kafka's [org.apache.kafka.common.TopicPartition].
 *
 * Represents a specific partition within a Kafka topic.
 */
typealias TopicPartition = org.apache.kafka.common.TopicPartition

/**
 * Extension property to get the topic name from a [TopicPartition].
 *
 * @return The name of the topic.
 */
val TopicPartition.topic: String
    get() = this.topic()

/**
 * Extension property to get the partition number from a [TopicPartition].
 *
 * @return The partition number.
 */
val TopicPartition.partition: Int
    get() = this.partition()

/**
 * Converts the [TopicPartition] to a formatted string for logging purposes.
 *
 * @return A string in the format "topic: {topic} | partition: {partition}".
 *
 * @sample
 * ```kotlin
 * val tp = TopicPartition("orders", 3)
 * println(tp.toLogString())  // "topic: orders | partition: 3"
 * ```
 */
fun TopicPartition.toLogString(): String = "topic: $topic | partition: $partition"
