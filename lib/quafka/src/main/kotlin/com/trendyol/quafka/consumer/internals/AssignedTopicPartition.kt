package com.trendyol.quafka.consumer.internals

import com.trendyol.quafka.common.InvalidConfigurationException
import com.trendyol.quafka.consumer.*
import com.trendyol.quafka.consumer.Events.toDetail
import com.trendyol.quafka.consumer.configuration.*
import com.trendyol.quafka.logging.*
import kotlinx.coroutines.*
import kotlinx.coroutines.selects.*
import org.apache.kafka.common.TopicPartition
import org.slf4j.Logger
import org.slf4j.event.Level
import java.io.Closeable
import java.util.concurrent.atomic.AtomicBoolean
import kotlin.math.max
import kotlin.time.Duration

/**
 * Represents an assigned Kafka topic partition managed by the consumer.
 *
 * This class is responsible for handling message processing, backpressure management,
 * pausing/resuming consumption, and offset tracking for a specific [TopicPartition].
 *
 * @param TKey The type of the message key.
 * @param TValue The type of the message value.
 * @property topicPartition The Kafka topic partition this instance manages.
 * @property logger Logger instance for logging events.
 * @property scope Coroutine scope used for launching background tasks.
 * @property quafkaConsumerOptions Consumer options configuration.
 * @property subscriptionOptions Subscription options configuration.
 * @param initialOffset The initial offset from which to start consumption.
 */
internal class AssignedTopicPartition<TKey, TValue> private constructor(
    val topicPartition: TopicPartition,
    private val logger: Logger,
    private val job: Job,
    private val scope: CoroutineScope,
    private val quafkaConsumerOptions: QuafkaConsumerOptions<TKey, TValue>,
    private val subscriptionOptions: TopicSubscriptionOptions<TKey, TValue>,
    private val topicPartitionOffsets: TopicPartitionOffsets,
    private val offsetManager: OffsetManager<TKey, TValue>,
    initialOffset: Long
) : Closeable {
    companion object {
        const val UNASSIGNED_OFFSET: Long = -1
        private val completedDeferred = CompletableDeferred(Unit)

        fun <TKey, TValue> create(
            topicPartition: TopicPartition,
            parentScope: CoroutineScope,
            quafkaConsumerOptions: QuafkaConsumerOptions<TKey, TValue>,
            offsetManager: OffsetManager<TKey, TValue>,
            initialOffset: Long
        ): AssignedTopicPartition<TKey, TValue> {
            val job = Job(parentScope.coroutineContext[Job])
            val scope = CoroutineScope(parentScope.coroutineContext + job + CoroutineName("tp_$topicPartition"))
            val topicPartitionOffset = offsetManager.register(topicPartition, scope)

            val logger = LoggerHelper.createLogger(
                clazz = AssignedTopicPartition::class.java,
                suffix = "assigned_$topicPartition",
                keyValuePairs = mutableMapOf<String, Any>(
                    LogParameters.CLIENT_ID to quafkaConsumerOptions.getClientId(),
                    LogParameters.GROUP_ID to quafkaConsumerOptions.getGroupId(),
                    LogParameters.TOPIC to topicPartition.topic(),
                    LogParameters.PARTITION to topicPartition.partition()
                )
            )
            return AssignedTopicPartition(
                topicPartition,
                logger,
                job,
                scope,
                quafkaConsumerOptions,
                quafkaConsumerOptions.getSubscriptionOptions(topicPartition),
                topicPartitionOffset,
                offsetManager,
                initialOffset
            )
        }

        /**
         * Retrieves the subscription options for the given [topicPartition].
         *
         * This method obtains subscription options based on the topic name extracted from the partition.
         *
         * @param topicPartition The topic partition for which subscription options are needed.
         * @return The subscription options for the corresponding topic.
         * @throws InvalidConfigurationException If subscription options are not found for the topic.
         */
        private fun <TKey, TValue> QuafkaConsumerOptions<TKey, TValue>.getSubscriptionOptions(topicPartition: TopicPartition) =
            this
                .getSubscriptionOptionsByTopicName(
                    topic = topicPartition.topic()
                ) ?: throw InvalidConfigurationException(
                "Subscription options not found for topic: ${topicPartition.topic()}"
            )
    }

    private val workerFactory = lazy {
        TopicPartitionWorker(
            topicPartition = topicPartition,
            scope = scope,
            subscriptionOptions = subscriptionOptions,
            quafkaConsumerOptions = quafkaConsumerOptions
        )
    }
    private val worker by workerFactory
    private var autoResumeJob: Job? = null
    private var paused = AtomicBoolean(false)
    private var backpressureActivated = AtomicBoolean(false)
    private val workerStopped = AtomicBoolean(false)
    var assignedOffset = initialOffset
        private set
    val isPaused: Boolean
        get() = paused.get()
    val isBackpressureActive: Boolean
        get() = backpressureActivated.get()

    var latestPausedOffset = UNASSIGNED_OFFSET
        private set

    val hasPendingResumeJob get() = autoResumeJob != null

    /**
     * Updates the assigned offset for this partition.
     *
     * @param offset New offset to assign.
     */
    fun updateAssignedOffset(offset: Long) {
        assignedOffset = offset
    }

    /**
     * Resumes consumption from the latest paused offset.
     */
    fun resume() = resume(latestPausedOffset)

    /**
     * Resumes consumption from the specified offset.
     *
     * This method clears any scheduled auto-resume job and marks the partition as active.
     *
     * @param offset The offset from which to resume consumption.
     */
    fun resume(offset: Long, force: Boolean = false) {
        if (workerStopped.get()) {
            return
        }
        if (paused.compareAndSet(true, false) || force) {
            autoResumeJob?.cancel()
            autoResumeJob = null
            if (logger.isDebugEnabled) {
                logger.debug("Consumer resumed from offset: {}", offset)
            }
        }
    }

    /**
     * Attempts to enqueue an incoming message for processing.
     *
     * This method first checks if backpressure should be activated, tracks the message offset,
     * overrides its acknowledgment mechanism, and then enqueues it to the worker.
     *
     * @param incomingMessage The incoming message to offer.
     * @return An [EnqueueResult] indicating the outcome of the enqueue operation.
     */
    suspend fun offerMessage(incomingMessage: IncomingMessage<TKey, TValue>): EnqueueResult {
        if (workerStopped.get()) {
            return EnqueueResult.WorkerClosed
        }
        if (shouldActivateBackpressure()) {
            activateBackpressure(incomingMessage)
            return EnqueueResult.Backpressure
        }
        topicPartitionOffsets.track(incomingMessage)
        incomingMessage.overrideAcknowledgment(IncomingMessageAcknowledgment())
        if (worker.enqueue(incomingMessage)) {
            return EnqueueResult.Ok
        } else {
            topicPartitionOffsets.untrack(incomingMessage)
            return EnqueueResult.WorkerClosed
        }
    }

    /**
     * Cancels coroutine scope and childs.
     */
    override fun close() {
        stopWorker()
        job.cancel()
        autoResumeJob?.cancel()
        autoResumeJob = null
        offsetManager.unregister(topicPartition)
    }

    /**
     * Stops the processing worker
     */
    fun stopWorker() {
        if (workerStopped.compareAndSet(false, true)) {
            if (workerFactory.isInitialized()) {
                worker.stop()
            }
        }
    }

    /**
     * Pauses message consumption for this partition.
     *
     * If already paused and the [incomingMessage]'s offset is greater than the current [latestPausedOffset],
     * the pause state is updated. Otherwise, it marks the partition as paused and schedules an auto-resume
     * if [autoResumeTimeout] is provided.
     *
     * @param incomingMessage The message triggering the pause.
     * @param autoResumeTimeout Duration after which consumption should automatically resume.
     */
    fun pause(incomingMessage: IncomingMessage<*, *>, autoResumeTimeout: Duration = Duration.ZERO) {
        if (paused.get() && incomingMessage.offset > latestPausedOffset) {
            updatePauseState(incomingMessage, autoResumeTimeout, "Paused offset changed")
            return
        }
        if (paused.compareAndSet(false, true)) {
            latestPausedOffset = incomingMessage.offset
            scheduleAutoResume(incomingMessage, autoResumeTimeout)
            if (logger.isDebugEnabled) {
                logger.debug(
                    "Consumer paused. | {}",
                    incomingMessage.toString(Level.DEBUG)
                )
            }
        }
    }

    /**
     * Schedules an auto-resume job to resume consumption after a specified timeout.
     *
     * If the [autoResumeTimeout] is greater than zero, a coroutine is launched that waits for the timeout
     * before resuming consumption from the maximum of the current message offset and [latestPausedOffset].
     *
     * @param incomingMessage The message associated with the auto-resume trigger.
     * @param autoResumeTimeout Duration after which to resume consumption automatically.
     */
    @OptIn(ExperimentalCoroutinesApi::class)
    private fun scheduleAutoResume(incomingMessage: IncomingMessage<*, *>, autoResumeTimeout: Duration) {
        if (workerStopped.get() || autoResumeTimeout <= Duration.ZERO) {
            return
        }

        autoResumeJob?.cancel()
        autoResumeJob = scope.launch {
            whileSelect {
                onTimeout(autoResumeTimeout) {
                    resume(max(incomingMessage.offset, latestPausedOffset))
                    false
                }
            }
        }
        if (logger.isDebugEnabled) {
            logger.debug(
                "Consumer resume task scheduled. | {}",
                incomingMessage.toString(Level.DEBUG)
            )
        }
    }

    /**
     * Determines if backpressure should be activated based on the current offset count.
     *
     * @return True if adding one more message would exceed the backpressure buffer size; false otherwise.
     */
    private fun shouldActivateBackpressure(): Boolean =
        topicPartitionOffsets.totalOffset() + 1 > subscriptionOptions.backpressureBufferSize

    /**
     * Updates the pause state by setting the latest paused offset and rescheduling auto-resume.
     *
     * @param incomingMessage The message triggering the pause state update.
     * @param autoResumeTimeout Duration after which consumption should automatically resume.
     * @param logMessage Log message to describe the reason for the update.
     */
    private fun updatePauseState(
        incomingMessage: IncomingMessage<*, *>,
        autoResumeTimeout: Duration,
        logMessage: String
    ) {
        latestPausedOffset = incomingMessage.offset
        scheduleAutoResume(incomingMessage, autoResumeTimeout)
        if (logger.isDebugEnabled) {
            logger.debug(
                "$logMessage | offset: {} | {}",
                incomingMessage.offset,
                incomingMessage.toString(Level.DEBUG)
            )
        }
    }

    /**
     * Activates backpressure for this partition if it is not already active.
     *
     * This method pauses consumption and publishes an event indicating that backpressure has been activated.
     *
     * @param incomingMessage The message that triggered backpressure activation.
     */
    private fun activateBackpressure(incomingMessage: IncomingMessage<TKey, TValue>) {
        if (backpressureActivated.compareAndSet(false, true)) {
            pause(
                incomingMessage,
                subscriptionOptions.backpressureReleaseTimeout
            )
            if (logger.isDebugEnabled) {
                logger.debug(
                    "Backpressure enabled. | {}",
                    incomingMessage.toString(Level.DEBUG)
                )
            }
            quafkaConsumerOptions.eventBus.publish(
                Events.BackpressureActivated(topicPartition, quafkaConsumerOptions.toDetail())
            )
        }
    }

    /**
     * Releases backpressure if possible.
     *
     * If backpressure is active and the total offset count for this partition is zero,
     * consumption is resumed and a backpressure release event is published.
     */
    fun releaseBackpressureIfPossible() {
        if (topicPartitionOffsets.totalOffset() == 0) {
            if (backpressureActivated.compareAndSet(true, false)) {
                resume()
                if (logger.isDebugEnabled) {
                    logger.debug("Backpressure released.")
                }
                quafkaConsumerOptions.eventBus.publish(
                    Events.BackpressureReleased(topicPartition, quafkaConsumerOptions.toDetail())
                )
            }
        }
    }

    /**
     * Acknowledgment implementation for incoming messages.
     *
     * This inner class ensures that each message is acknowledged and committed only once.
     * It delegates acknowledgment and commit operations to the parent [offsetManager].
     */
    private inner class IncomingMessageAcknowledgment : Acknowledgment {
        private var acked: AtomicBoolean = AtomicBoolean(false)

        /**
         * Acknowledges the given message.
         *
         * If the message has not been acknowledged yet, it delegates the acknowledgment to the parent's [offsetManager].
         *
         * @param incomingMessage The message to acknowledge.
         */
        override fun acknowledge(incomingMessage: IncomingMessage<*, *>) {
            if (acked.compareAndSet(false, true)) {
                topicPartitionOffsets.acknowledge(incomingMessage)
                incomingMessage.markAsAcknowledged()
            }
        }

        /**
         * Commits the offset for the given message.
         *
         * If the message has not been committed yet, it delegates the commit operation to the parent's [offsetManager].
         *
         * @param incomingMessage The message for which to commit the offset.
         * @return A [Deferred] representing the commit operation.
         */
        override fun commit(incomingMessage: IncomingMessage<*, *>): Deferred<Unit> {
            if (acked.compareAndSet(false, true)) {
                val deferred = topicPartitionOffsets.commit(incomingMessage)
                deferred.invokeOnCompletion {
                    incomingMessage.markAsAcknowledged()
                }
                return deferred
            }
            return completedDeferred
        }
    }
}
