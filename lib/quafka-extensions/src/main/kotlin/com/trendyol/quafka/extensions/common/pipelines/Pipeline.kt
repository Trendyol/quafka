package com.trendyol.quafka.extensions.common.pipelines

class Pipeline<TEnvelope>(
    private val middlewareFn: MiddlewareFn<TEnvelope>
) {
    suspend fun execute(envelope: TEnvelope) {
        middlewareFn(envelope)
    }
}
