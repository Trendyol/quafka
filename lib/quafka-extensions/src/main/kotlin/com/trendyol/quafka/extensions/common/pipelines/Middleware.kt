package com.trendyol.quafka.extensions.common.pipelines

interface Middleware<TEnvelope> {
    suspend fun execute(
        envelope: TEnvelope,
        next: MiddlewareFn<TEnvelope>
    )
}

typealias MiddlewareFn<TEnvelope> = suspend (TEnvelope) -> Unit
