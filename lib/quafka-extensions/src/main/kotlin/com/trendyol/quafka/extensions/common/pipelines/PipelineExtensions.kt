package com.trendyol.quafka.extensions.common.pipelines

/**
 * Adds a middleware to the pipeline that processes envelopes without transforming them.
 *
 * This is the standard way to add middleware logic to the pipeline. The envelope passed to
 * the middleware is automatically forwarded to the next middleware in the chain when `next()` is called.
 *
 * @param TEnvelope The type of envelope being processed through the pipeline.
 * @param middleware A suspend function that receives the envelope and a `next` callback.
 *                   Call `next()` to continue to the next middleware in the chain.
 *
 * @return This [PipelineBuilder] instance for method chaining.
 *
 * @sample
 * ```kotlin
 * pipelineBuilder.use { envelope, next ->
 *     logger.info("Before processing")
 *     next() // Continue with the same envelope
 *     logger.info("After processing")
 * }
 * ```
 */
fun <TEnvelope> PipelineBuilder<TEnvelope>.use(
    middleware: suspend (envelope: TEnvelope, next: suspend () -> Unit) -> Unit
): PipelineBuilder<TEnvelope> = this.use { next ->
    { envelope ->
        val nextMiddleware = suspend { next(envelope) }
        middleware(envelope, nextMiddleware)
    }
}

/**
 * Adds a middleware to the pipeline that can transform the envelope before passing it to the next middleware.
 *
 * Unlike [use], this variant allows you to modify or replace the envelope that gets passed to
 * subsequent middleware in the chain. This is useful for envelope transformation scenarios.
 *
 * @param TEnvelope The type of envelope being processed through the pipeline.
 * @param middleware A suspend function that receives the envelope and a `next` callback that accepts
 *                   an envelope parameter. You can pass a modified envelope to `next()`.
 *
 * @return This [PipelineBuilder] instance for method chaining.
 *
 * @sample
 * ```kotlin
 * pipelineBuilder.useTransform { envelope, next ->
 *     // Modify the envelope
 *     val modifiedEnvelope = envelope.copy(attributes = updatedAttributes)
 *     next(modifiedEnvelope) // Continue with modified envelope
 * }
 * ```
 */
@JvmName("useTEnvelope")
fun <TEnvelope> PipelineBuilder<TEnvelope>.useTransform(
    middleware: suspend (envelope: TEnvelope, next: suspend (TEnvelope) -> Unit) -> Unit
): PipelineBuilder<TEnvelope> = this.use { next ->
    { envelope ->
        val nextMiddleware: suspend (TEnvelope) -> Unit = { c: TEnvelope -> next(c) }
        middleware(envelope, nextMiddleware)
    }
}

/**
 * Conditionally adds a middleware to the pipeline based on a predicate.
 *
 * The middleware is only executed if the condition evaluates to `true` for the given envelope.
 * If the condition is `false`, the envelope is passed directly to the next middleware, bypassing
 * the conditional middleware entirely.
 *
 * @param TEnvelope The type of envelope being processed through the pipeline.
 * @param middleware The middleware to execute conditionally.
 * @param condition A predicate function that determines whether the middleware should be executed.
 *
 * @return This [PipelineBuilder] instance for method chaining.
 *
 * @sample
 * ```kotlin
 * pipelineBuilder.useMiddlewareWhen(
 *     middleware = LoggingMiddleware(),
 *     condition = { envelope -> envelope.attributes.contains("debug") }
 * )
 * ```
 */
fun <TEnvelope> PipelineBuilder<TEnvelope>.useMiddlewareWhen(
    middleware: Middleware<TEnvelope>,
    condition: (envelope: TEnvelope) -> Boolean
): PipelineBuilder<TEnvelope> = this.use { next ->
    { envelope ->
        if (condition(envelope)) {
            middleware.execute(envelope, next)
        } else {
            next(envelope)
        }
    }
}

/**
 * Adds a [Middleware] instance to the pipeline.
 *
 * This is the standard way to add middleware that implements the [Middleware] interface.
 * The middleware's [Middleware.execute] method will be called for each envelope that flows
 * through the pipeline.
 *
 * @param TEnvelope The type of envelope being processed through the pipeline.
 * @param middleware The middleware instance to add to the pipeline.
 *
 * @return This [PipelineBuilder] instance for method chaining.
 *
 * @sample
 * ```kotlin
 * pipelineBuilder
 *     .useMiddleware(LoggingMiddleware())
 *     .useMiddleware(ValidationMiddleware())
 *     .useMiddleware(ProcessingMiddleware())
 * ```
 */
fun <TEnvelope> PipelineBuilder<TEnvelope>.useMiddleware(middleware: Middleware<TEnvelope>): PipelineBuilder<TEnvelope> = this
    .use { next ->
        { envelope ->
            middleware.execute(envelope, next)
        }
    }
