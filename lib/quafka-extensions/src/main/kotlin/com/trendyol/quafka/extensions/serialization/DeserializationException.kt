package com.trendyol.quafka.extensions.serialization

import com.trendyol.quafka.common.QuafkaException
import com.trendyol.quafka.consumer.TopicPartitionOffset

class DeserializationException(val topicPartitionOffset: TopicPartitionOffset, message: String, cause: Throwable? = null) :
    QuafkaException(message, cause)
