package com.trendyol.quafka.consumer.internals

import com.trendyol.quafka.common.chunked
import com.trendyol.quafka.consumer.*
import com.trendyol.quafka.consumer.Events.toDetail
import com.trendyol.quafka.consumer.configuration.*
import com.trendyol.quafka.consumer.messageHandlers.*
import com.trendyol.quafka.logging.*
import kotlinx.coroutines.*
import kotlinx.coroutines.channels.*
import org.apache.kafka.common.TopicPartition
import org.slf4j.Logger
import org.slf4j.event.Level
import java.util.concurrent.atomic.AtomicBoolean

/**
 * Worker responsible for processing messages from a specific [TopicPartition].
 *
 * The [TopicPartitionWorker] receives messages via a channel and processes them using either batch
 * or single message handling options specified in [subscriptionOptions]. It manages its own coroutine
 * scope and cancellation, logs processing events, and publishes worker lifecycle events.
 *
 * @param TKey The type of the message key.
 * @param TValue The type of the message value.
 * @property topicPartition The Kafka topic partition assigned to this worker.
 * @property quafkaConsumerOptions Consumer configuration options.
 * @property subscriptionOptions Subscription options including message handling settings.
 * @param scope The coroutine scope used to launch asynchronous operations.
 */
internal class TopicPartitionWorker<TKey, TValue>(
    private val topicPartition: TopicPartition,
    private val quafkaConsumerOptions: QuafkaConsumerOptions<TKey, TValue>,
    val subscriptionOptions: TopicSubscriptionOptions<TKey, TValue>,
    private val scope: CoroutineScope,
    assignedOffset: Long
) {
    private var consumerContext = ConsumerContext(
        topicPartition = topicPartition,
        coroutineContext = scope.coroutineContext,
        consumerOptions = quafkaConsumerOptions
    )
    private val logger: Logger =
        LoggerHelper.createLogger(
            clazz = TopicPartitionWorker::class.java,
            suffix = "worker_${quafkaConsumerOptions.getClientId()}",
            keyValuePairs = consumerContext.createConsumerContextKeyValuePairs()
        )
    private val workerStopped: AtomicBoolean = AtomicBoolean(false)
    private val inFlightMessages: Channel<IncomingMessage<TKey, TValue>> = Channel(
        Channel.UNLIMITED,
        BufferOverflow.SUSPEND
    )

    init {
        startProcessLoop()
        setupCancellation()
        logger.info(
            "Worker started. | topic: {} | partition: {} | assigned offset: {}",
            topicPartition.topic(),
            topicPartition.partition(),
            assignedOffset
        )
    }

    /**
     * Enqueues an [incomingMessage] into the worker's channel for processing.
     *
     * If the worker is stopping or its scope is no longer active, the message is skipped.
     *
     * @param incomingMessage The message to enqueue.
     * @return `true` if the message was successfully enqueued; `false` otherwise.
     */
    suspend fun enqueue(incomingMessage: IncomingMessage<TKey, TValue>): Boolean {
        if (workerStopped.get() || !scope.isActive) {
            logMessage(
                level = Level.DEBUG,
                message = "Worker stopped. Skipping message!! | {}",
                incomingMessage = incomingMessage
            )
            return false
        }

        return handleChannelResult(inFlightMessages.trySend(incomingMessage), incomingMessage)
    }

    /**
     * Handles the result of attempting to send a message to the channel.
     *
     * Depending on the [channelResult], the method logs success, failure, or unexpected states,
     * and stops the worker if necessary.
     *
     * @param channelResult The result of trying to send a message to the channel.
     * @param incomingMessage The message that was attempted to be sent.
     * @return `true` if the message was sent successfully; `false` otherwise.
     */
    private suspend fun handleChannelResult(
        channelResult: ChannelResult<Unit>,
        incomingMessage: IncomingMessage<TKey, TValue>
    ): Boolean = when {
        channelResult.isClosed -> {
            logMessage(
                level = Level.DEBUG,
                message = "Channel closed when sending records. | {}",
                incomingMessage = incomingMessage
            )
            false
        }

        channelResult.isFailure -> {
            val exception = channelResult.exceptionOrNull()
            if (exception != null) {
                logMessage(
                    level = Level.WARN,
                    message = "Channel send failed when trying to send records. Worker will stop. | {}",
                    incomingMessage = incomingMessage,
                    exception = exception
                )
                scope.coroutineContext.job.cancel(exception.message ?: "Channel send failed", exception)
                scope.coroutineContext.job.join()
            }
            false
        }

        channelResult.isSuccess -> {
            logMessage(
                level = Level.TRACE,
                message = "Message sent to buffer channel. | {}",
                incomingMessage = incomingMessage
            )
            true
        }

        else -> {
            val exception = IllegalStateException("Unexpected error")
            logMessage(
                level = Level.WARN,
                message = "Unexpected state. Worker will stop. | {}",
                incomingMessage = incomingMessage,
                exception = exception
            )
            scope.coroutineContext.job.cancel(exception.message!!, exception)
            scope.coroutineContext.job.join()
            throw exception
        }
    }

    /**
     * Logs a formatted message for a single [incomingMessage] at the given log [level].
     *
     * @param level The log level.
     * @param message The log message format.
     * @param incomingMessage The message to include in the log.
     * @param exception Optional exception to log.
     */
    private fun logMessage(
        level: Level,
        message: String,
        incomingMessage: IncomingMessage<TKey, TValue>,
        exception: Throwable? = null
    ) {
        logger
            .atLevel(level)
            .log(
                "$message | {}",
                incomingMessage.toString(level),
                exception
            )
    }

    /**
     * Starts the processing loop that consumes messages from the [inFlightMessages] channel.
     *
     * The loop chooses between batch or single message processing based on the handling options in
     * [subscriptionOptions]. Messages are processed until the worker's scope is cancelled.
     */
    private fun startProcessLoop() = scope.launch {
        when (val strategy = subscriptionOptions.messageHandlingStrategy) {
            is BatchMessageHandlingStrategy<TKey, TValue> ->
                inFlightMessages
                    .chunked(scope, strategy.batchSize, strategy.batchTimeout)
                    .consumeEach { messages ->
                        scope.ensureActive()
                        strategy.invoke(messages, consumerContext)
                    }

            is SingleMessageHandlingStrategy<TKey, TValue> ->
                inFlightMessages
                    .consumeEach { message ->
                        scope.ensureActive()
                        strategy.invoke(message, consumerContext)
                    }
        }
    }

    /**
     * Sets up a cancellation listener on the worker's coroutine job.
     *
     * When the worker is cancelled or completes, this method closes the [inFlightMessages] channel,
     * logs the stopping event, handles any uncaught exceptions, and publishes a worker stopped event.
     */
    private fun setupCancellation() {
        scope.coroutineContext.job.invokeOnCompletion { exception ->
            logger.info(
                "Worker stopping... | topic: {} | partition: {} | reason: {}",
                topicPartition.topic(),
                topicPartition.partition(),
                exception.getReason(),
                exception?.cause
            )
            inFlightMessages.close()
            logger.info(
                "Worker stopped. | topic: {} | partition: {} | reason: {}",
                topicPartition.topic(),
                topicPartition.partition(),
                exception.getReason(),
                exception?.cause
            )
            quafkaConsumerOptions.eventBus.publish(Events.WorkerStopped(topicPartition, quafkaConsumerOptions.toDetail()))
        }
    }

    /**
     * Extension function for [Throwable] to extract a concise reason string.
     *
     * @return A string describing the reason for the throwable.
     */
    private fun Throwable?.getReason(): String = when (this) {
        null -> ""
        is CancellationException -> "Cancellation"
        else -> "${this.message} - ${this.cause?.message}"
    }

    fun stop() {
        if (workerStopped.compareAndSet(false, true)) {
            this.inFlightMessages.close()
        }
    }
}
