package com.trendyol.quafka.events

import kotlinx.coroutines.*
import kotlinx.coroutines.flow.*
import java.io.Closeable
import kotlin.reflect.KClass

interface QuafkaEvent

@Suppress("UNCHECKED_CAST")
open class EventBus(dispatcher: CoroutineDispatcher = Dispatchers.IO) : Closeable {
    private val job = Job()
    private val scope = CoroutineScope(dispatcher + job)

    private val channel = MutableSharedFlow<QuafkaEvent>(extraBufferCapacity = 1)

    val events: SharedFlow<QuafkaEvent> = channel.asSharedFlow()

    open fun publishAsync(event: QuafkaEvent) {
        scope.launch {
            channel.emit(event)
        }
    }

    open fun publish(event: QuafkaEvent) {
        channel.tryEmit(event)
    }

    open suspend fun publishSuspendable(event: QuafkaEvent) {
        channel.emit(event)
    }

    open suspend fun subscribe(callback: suspend (event: QuafkaEvent) -> Unit) {
        events.collect { event ->
            currentCoroutineContext().ensureActive()
            callback(event)
        }
    }

    open fun <T : Any> subscribe(
        type: KClass<T>
    ): Flow<T> = channel
        .filter { type.isInstance(it) } as Flow<T>

    override fun close() {
        job.cancel()
    }
}

suspend inline fun <reified T : QuafkaEvent> EventBus.subscribe(crossinline callback: suspend (T) -> Unit) {
    events
        .filterIsInstance<T>()
        .collect { event ->
            currentCoroutineContext().ensureActive()
            callback(event)
        }
}
