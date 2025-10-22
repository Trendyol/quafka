package com.trendyol.quafka.extensions.common

import com.trendyol.quafka.common.QuafkaException
import com.trendyol.quafka.consumer.TopicPartitionOffset

class TopicPartitionProcessException(
    val topicPartitionOffset: TopicPartitionOffset,
    message: String,
    cause: Throwable? = null
) : QuafkaException(message, cause)
