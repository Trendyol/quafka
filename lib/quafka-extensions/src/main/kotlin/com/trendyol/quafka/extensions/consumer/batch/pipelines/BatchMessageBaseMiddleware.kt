package com.trendyol.quafka.extensions.consumer.batch.pipelines

import com.trendyol.quafka.extensions.common.pipelines.Middleware

abstract class BatchMessageBaseMiddleware<TEnvelope, TKey, TValue> : Middleware<TEnvelope> where TEnvelope : BatchMessageEnvelope<*, *>
