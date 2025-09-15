package com.trendyol.quafka.extensions.consumer.single.pipelines

import com.trendyol.quafka.consumer.*

open class SingleMessageEnvelope<TKey, TValue>(
    val message: IncomingMessage<TKey, TValue>,
    val consumerContext: ConsumerContext,
    val attributes: Attributes = Attributes()
)
