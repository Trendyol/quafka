package com.trendyol.quafka.extensions.common.pipelines

class MapPipelineMiddleware<TEnvelope>(
    private val next: MiddlewareFn<TEnvelope>,
    private val options: MapOptions<TEnvelope>
) {
    suspend fun execute(envelope: TEnvelope) {
        if (options.condition(envelope)) {
            options.branch.execute(envelope)
        } else {
            next(envelope)
        }
    }
}
