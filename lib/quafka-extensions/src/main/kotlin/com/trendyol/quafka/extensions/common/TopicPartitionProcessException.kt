package com.trendyol.quafka.extensions.common

import com.trendyol.quafka.common.QuafkaException
import org.apache.kafka.common.TopicPartition

class TopicPartitionProcessException(
    val topicPartition: TopicPartition,
    val offset: Long,
    message: String,
    cause: Throwable? = null
) : QuafkaException(message, cause)
