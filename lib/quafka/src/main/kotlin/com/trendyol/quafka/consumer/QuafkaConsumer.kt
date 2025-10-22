package com.trendyol.quafka.consumer

import com.trendyol.quafka.common.Waiter
import com.trendyol.quafka.consumer.configuration.QuafkaConsumerOptions
import com.trendyol.quafka.consumer.internals.*
import com.trendyol.quafka.events.subscribe
import com.trendyol.quafka.logging.*
import io.github.resilience4j.kotlin.retry.executeFunction
import kotlinx.coroutines.*
import java.io.Closeable
import java.util.concurrent.atomic.AtomicReference

/**
 * Represents a Kafka consumer with enhanced functionality, including coroutine support, custom event publishing,
 * and connection retry policies.
 *
 * The `QuafkaConsumer` class allows consuming messages from Kafka with configurable options for error handling,
 * dispatching, and lifecycle management.
 *
 * @param TKey The type of the Kafka message key.
 * @param TValue The type of the Kafka message value.
 * @property quafkaConsumerOptions Configuration options for the consumer, including retry policies, dispatcher, and more.
 * @property pollingConsumerFactory Factory for creating and managing the internal Kafka consumer instance.
 */
class QuafkaConsumer<TKey, TValue> internal constructor(
    val quafkaConsumerOptions: QuafkaConsumerOptions<TKey, TValue>,
    private val pollingConsumerFactory: PollingConsumerFactory<TKey, TValue>
) : Closeable {
    private var consumer: PollingConsumer<TKey, TValue>? = null
    private var rootJob: Job? = null
    private var scope: CoroutineScope? = null
    private val _status = AtomicReference(ConsumerStatus.Ready)
    private val logger = LoggerHelper.createLogger(
        clazz = QuafkaConsumer::class.java,
        suffix = "consumer_${quafkaConsumerOptions.getClientId()}",
        keyValuePairs = mutableMapOf<String, Any>(
            LogParameters.CLIENT_ID to quafkaConsumerOptions.getClientId(),
            LogParameters.GROUP_ID to quafkaConsumerOptions.getGroupId()
        )
    )

    /**
     * Publisher for custom events related to the consumer's lifecycle and operations.
     */
    val eventPublisher = quafkaConsumerOptions.eventBus

    /**
     * Represents the current status of the consumer.
     * Possible values are [ConsumerStatus.Ready], [ConsumerStatus.Running], and [ConsumerStatus.Stopping].
     */
    val status: ConsumerStatus
        get() = _status.get()

    /**
     * Enum representing the possible statuses of the consumer.
     */
    enum class ConsumerStatus {
        /** The consumer is ready to start but not currently running. */
        Ready,

        /** The consumer is actively running and processing messages. */
        Running,

        /** The consumer is in the process of stopping. */
        Stopping
    }

    /**
     * Starts the consumer, transitioning its status from [ConsumerStatus.Ready] to [ConsumerStatus.Running].
     *
     * If the consumer is already running, the method returns without performing any actions.
     * The consumer will use the configured retry policy to handle connection failures.
     */
    fun start() {
        if (!quafkaConsumerOptions.hasSubscription()) {
            logger.warn("The subscription topic list is empty, so the consumer will not start.")
            return
        }

        if (!_status.compareAndSet(ConsumerStatus.Ready, ConsumerStatus.Running)) {
            return
        }

        val waiter: Waiter? = if (quafkaConsumerOptions.blockOnStart) Waiter() else null

        val job = SupervisorJob()
        val newScope = CoroutineScope(
            quafkaConsumerOptions.dispatcher +
                quafkaConsumerOptions.coroutineExceptionHandler + job
        )
        this.rootJob = job
        this.scope = newScope
        newScope.launch {
            eventPublisher.subscribe<Events.WorkerFailed> {
                logger.error(
                    "Worker failed, consumer will be stopped. | {}",
                    it.topicPartition.toLogString(),
                    it.exception
                )
                launch {
                    stop()
                }
            }
        }
        this.consumer = quafkaConsumerOptions.connectionRetryPolicy.executeFunction {
            pollingConsumerFactory
                .create(
                    quafkaConsumerOptions,
                    newScope,
                    waiter,
                    logger
                ).apply { start() }
        }

        waiter?.wait()
    }

    /**
     * Stops the consumer, transitioning its status from [ConsumerStatus.Running] to [ConsumerStatus.Stopping].
     *
     * Cancels all running coroutines associated with the consumer and waits for them to complete.
     * Once stopped, the consumer's status is reset to [ConsumerStatus.Ready].
     */
    fun stop() {
        if (!_status.compareAndSet(ConsumerStatus.Running, ConsumerStatus.Stopping)) {
            return
        }
        rootJob?.cancel()
        runBlocking {
            scope?.coroutineContext[Job]?.children?.forEach { it.join() }
        }
        rootJob = null
        scope = null

        _status.set(ConsumerStatus.Ready)
    }

    override fun close() {
        stop()
    }
}
