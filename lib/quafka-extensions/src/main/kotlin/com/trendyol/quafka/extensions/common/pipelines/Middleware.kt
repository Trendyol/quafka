package com.trendyol.quafka.extensions.common.pipelines

/**
 * Interface for implementing reusable middleware components in a pipeline.
 *
 * Middleware components intercept and process envelopes as they flow through the pipeline.
 * Each middleware can perform operations before and/or after invoking the next middleware
 * in the chain.
 *
 * @param TEnvelope The type of envelope being processed.
 *
 * @see Pipeline
 * @see PipelineBuilder
 *
 * @sample
 * ```kotlin
 * class LoggingMiddleware<TEnvelope> : Middleware<TEnvelope> {
 *     override suspend fun execute(envelope: TEnvelope, next: MiddlewareFn<TEnvelope>) {
 *         println("Before: $envelope")
 *         next(envelope)
 *         println("After: $envelope")
 *     }
 * }
 * ```
 */
interface Middleware<TEnvelope> {
    /**
     * Executes the middleware logic for the given envelope.
     *
     * Implementations should call `next(envelope)` to pass control to the next middleware
     * in the chain. Operations before calling `next` are executed on the way "in", while
     * operations after are executed on the way "out" (unwinding the call stack).
     *
     * @param envelope The envelope to process.
     * @param next The function to call to continue processing with the next middleware.
     */
    suspend fun execute(
        envelope: TEnvelope,
        next: MiddlewareFn<TEnvelope>
    )
}

/**
 * Type alias for a middleware function that processes an envelope.
 *
 * This represents a single step in the middleware pipeline that takes an envelope
 * and performs some operation on it.
 *
 * @param TEnvelope The type of envelope being processed.
 */
typealias MiddlewareFn<TEnvelope> = suspend (TEnvelope) -> Unit
