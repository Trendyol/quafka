package com.trendyol.quafka.extensions.common.pipelines

data class MapOptions<TEnvelope>(
    val condition: (TEnvelope) -> Boolean,
    val branch: Pipeline<TEnvelope>
)
