package com.trendyol.quafka.extensions.common.pipelines

class PipelineBuilder<TEnvelope> {
    private val middlewares: MutableList<(MiddlewareFn<TEnvelope>) -> MiddlewareFn<TEnvelope>> =
        mutableListOf()

    fun use(middleware: (MiddlewareFn<TEnvelope>) -> MiddlewareFn<TEnvelope>): PipelineBuilder<TEnvelope> {
        middlewares.add(middleware)
        return this
    }

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

    fun build(): Pipeline<TEnvelope> {
        var middleware: MiddlewareFn<TEnvelope> = { _ -> }

        for (i in middlewares.count() - 1 downTo 0) {
            middleware = middlewares[i](middleware)
        }

        return Pipeline(middleware)
    }

    fun new(): PipelineBuilder<TEnvelope> = PipelineBuilder()
}

suspend fun <TEnvelope> pipelineBuilder(init: suspend PipelineBuilder<TEnvelope>.() -> Unit): Pipeline<TEnvelope> {
    val builder = PipelineBuilder<TEnvelope>()
    init.invoke(builder)
    return builder.build()
}
