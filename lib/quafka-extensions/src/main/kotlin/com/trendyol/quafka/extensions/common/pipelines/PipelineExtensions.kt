package com.trendyol.quafka.extensions.common.pipelines

fun <TEnvelope> PipelineBuilder<TEnvelope>.use(
    middleware: suspend (envelope: TEnvelope, next: suspend () -> Unit) -> Unit
): PipelineBuilder<TEnvelope> = this.use { next ->
    { envelope ->
        val nextMiddleware = suspend { next(envelope) }
        middleware(envelope, nextMiddleware)
    }
}

@JvmName("useTEnvelope")
fun <TEnvelope> PipelineBuilder<TEnvelope>.use(
    middleware: suspend (envelope: TEnvelope, next: suspend (TEnvelope) -> Unit) -> Unit
): PipelineBuilder<TEnvelope> = this.use { next ->
    { envelope ->
        val nextMiddleware: suspend (TEnvelope) -> Unit = { c: TEnvelope -> next(c) }
        middleware(envelope, nextMiddleware)
    }
}

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

fun <TEnvelope> PipelineBuilder<TEnvelope>.useMiddleware(middleware: Middleware<TEnvelope>): PipelineBuilder<TEnvelope> = this
    .use { next ->
        { envelope ->
            middleware.execute(envelope, next)
        }
    }
