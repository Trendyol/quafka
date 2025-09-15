package com.trendyol.quafka.extensions.consumer.single.pipelines

import com.trendyol.quafka.extensions.common.pipelines.Middleware

abstract class SingleMessageBaseMiddleware<TEnvelope, TKey, TValue> : Middleware<TEnvelope> where TEnvelope : SingleMessageEnvelope<*, *>
