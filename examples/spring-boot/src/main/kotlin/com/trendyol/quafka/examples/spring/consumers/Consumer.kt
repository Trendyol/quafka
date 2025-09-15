package com.trendyol.quafka.examples.spring.consumers

import com.trendyol.quafka.consumer.configuration.*

interface Consumer<TKey, TValue> {
    fun configure(
        builder: QuafkaConsumerBuilder<TKey, TValue>
    ): SubscriptionBuildStep<TKey, TValue>
}
