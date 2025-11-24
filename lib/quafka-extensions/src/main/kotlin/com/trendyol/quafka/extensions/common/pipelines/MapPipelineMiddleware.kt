package com.trendyol.quafka.extensions.common.pipelines

/**
 * Internal middleware for implementing conditional pipeline branching.
 *
 * This middleware evaluates a condition and either executes a branch pipeline or
 * continues to the next middleware in the main pipeline.
 *
 * Created automatically by [PipelineBuilder.map]. Most users should not instantiate
 * this class directly.
 *
 * @param TEnvelope The type of envelope being processed.
 * @property next The next middleware to execute if the condition is false.
 * @property options The configuration for the conditional branch.
 *
 * @see PipelineBuilder.map
 * @see MapOptions
 */
class MapPipelineMiddleware<TEnvelope>(private val next: MiddlewareFn<TEnvelope>, private val options: MapOptions<TEnvelope>) {
    /**
     * Executes the conditional branching logic.
     *
     * If the condition evaluates to `true`, the envelope is processed through the branch pipeline.
     * Otherwise, the envelope is passed to the next middleware in the main pipeline.
     *
     * @param envelope The envelope to process.
     */
    suspend fun execute(envelope: TEnvelope) {
        if (options.condition(envelope)) {
            options.branch.execute(envelope)
        } else {
            next(envelope)
        }
    }
}
