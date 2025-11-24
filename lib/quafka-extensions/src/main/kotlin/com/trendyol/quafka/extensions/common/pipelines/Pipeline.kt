package com.trendyol.quafka.extensions.common.pipelines

/**
 * Represents a processing pipeline composed of middleware functions that execute in sequence.
 *
 * A pipeline processes envelopes through a chain of middleware. Each middleware can perform
 * operations before and after passing control to the next middleware in the chain.
 *
 * Pipelines are immutable once built and are created using [PipelineBuilder].
 *
 * @param TEnvelope The type of envelope that flows through the pipeline.
 * @property middlewareFn The composed middleware function that represents the entire pipeline.
 *
 * @see PipelineBuilder
 * @see Middleware
 *
 * @sample
 * ```kotlin
 * val pipeline = PipelineBuilder<MyEnvelope>()
 *     .use { envelope, next ->
 *         println("Before")
 *         next()
 *         println("After")
 *     }
 *     .build()
 *
 * pipeline.execute(myEnvelope)
 * ```
 */
class Pipeline<TEnvelope>(private val middlewareFn: MiddlewareFn<TEnvelope>) {
    /**
     * Executes the pipeline by passing the envelope through all middleware in sequence.
     *
     * @param envelope The envelope to process through the pipeline.
     */
    suspend fun execute(envelope: TEnvelope) {
        middlewareFn(envelope)
    }
}
