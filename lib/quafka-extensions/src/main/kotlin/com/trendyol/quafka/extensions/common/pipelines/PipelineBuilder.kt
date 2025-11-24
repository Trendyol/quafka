package com.trendyol.quafka.extensions.common.pipelines

/**
 * Builder for constructing a [Pipeline] with a chain of middleware.
 *
 * The builder uses a fluent API to add middleware in the order they should be executed.
 * Middleware added first will be executed first (outermost), and middleware added last
 * will be executed last (innermost).
 *
 * @param TEnvelope The type of envelope that will flow through the pipeline.
 *
 * @see Pipeline
 * @see Middleware
 *
 * @sample
 * ```kotlin
 * val pipeline = PipelineBuilder<MyEnvelope>()
 *     .use { envelope, next ->
 *         // First middleware
 *         next()
 *     }
 *     .use { envelope, next ->
 *         // Second middleware
 *         next()
 *     }
 *     .build()
 * ```
 */
class PipelineBuilder<TEnvelope> {
    private val middlewares: MutableList<(MiddlewareFn<TEnvelope>) -> MiddlewareFn<TEnvelope>> =
        mutableListOf()

    /**
     * Adds a middleware to the pipeline using a function wrapper.
     *
     * This is the low-level API for adding middleware. Most users should use the extension
     * functions in [PipelineExtensions] instead, such as [use] with lambda or [useMiddleware].
     *
     * @param middleware A function that takes the next middleware and returns a new middleware function.
     * @return This builder instance for method chaining.
     */
    fun use(middleware: (MiddlewareFn<TEnvelope>) -> MiddlewareFn<TEnvelope>): PipelineBuilder<TEnvelope> {
        middlewares.add(middleware)
        return this
    }

    /**
     * Creates a conditional branch in the pipeline.
     *
     * If the condition evaluates to `true`, the envelope is processed through the branch pipeline.
     * If the condition evaluates to `false`, the envelope continues to the next middleware in the main pipeline.
     *
     * @param condition A predicate function to determine if the branch should be taken.
     * @param configuration A lambda to configure the branch pipeline.
     * @return This builder instance for method chaining.
     *
     * @sample
     * ```kotlin
     * pipelineBuilder.map(
     *     condition = { envelope -> envelope.attributes.contains("special") }
     * ) {
     *     use { envelope, next ->
     *         // This runs only if condition is true
     *         next()
     *     }
     * }
     * ```
     */
    fun map(
        condition: (envelope: TEnvelope) -> Boolean,
        configuration: PipelineBuilder<TEnvelope>.() -> Unit
    ): PipelineBuilder<TEnvelope> {
        val branchBuilder = this.new()
        configuration(branchBuilder)
        val branch = branchBuilder.build()

        val options = MapOptions(condition, branch)
        return this.use { next -> MapPipelineMiddleware(next, options)::execute }
    }

    /**
     * Builds the [Pipeline] from the configured middleware chain.
     *
     * This method composes all added middleware into a single pipeline function, where
     * middleware are executed in the order they were added.
     *
     * @return A new [Pipeline] instance ready to process envelopes.
     */
    fun build(): Pipeline<TEnvelope> {
        var middleware: MiddlewareFn<TEnvelope> = { _ -> }

        for (i in middlewares.count() - 1 downTo 0) {
            middleware = middlewares[i](middleware)
        }

        return Pipeline(middleware)
    }

    /**
     * Creates a new empty [PipelineBuilder] with the same envelope type.
     *
     * This is useful for creating branch pipelines in [map].
     *
     * @return A new [PipelineBuilder] instance.
     */
    fun new(): PipelineBuilder<TEnvelope> = PipelineBuilder()
}

/**
 * Convenience function to build a pipeline using a DSL-style lambda.
 *
 * @param TEnvelope The type of envelope that will flow through the pipeline.
 * @param init A lambda to configure the [PipelineBuilder].
 * @return A new [Pipeline] instance ready to process envelopes.
 *
 * @sample
 * ```kotlin
 * val pipeline = pipelineBuilder<MyEnvelope> {
 *     use { envelope, next ->
 *         println("Processing: $envelope")
 *         next()
 *     }
 * }
 * ```
 */
suspend fun <TEnvelope> pipelineBuilder(init: suspend PipelineBuilder<TEnvelope>.() -> Unit): Pipeline<TEnvelope> {
    val builder = PipelineBuilder<TEnvelope>()
    init.invoke(builder)
    return builder.build()
}
