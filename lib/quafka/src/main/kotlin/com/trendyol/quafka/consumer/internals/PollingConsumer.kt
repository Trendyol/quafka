package com.trendyol.quafka.consumer.internals

import com.trendyol.quafka.common.*
import com.trendyol.quafka.common.Waiter
import com.trendyol.quafka.consumer.*
import com.trendyol.quafka.consumer.Events.toDetail
import com.trendyol.quafka.consumer.configuration.QuafkaConsumerOptions
import kotlinx.coroutines.*
import org.apache.kafka.clients.consumer.*
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors.WakeupException
import org.slf4j.Logger
import org.slf4j.event.Level
import java.util.concurrent.atomic.AtomicBoolean
import kotlin.time.toJavaDuration

/**
 * A Kafka polling consumer that continuously polls records, dispatches messages to assigned partitions,
 * manages offset commits, and handles partition rebalancing.
 *
 * This consumer integrates with the internal [PartitionAssignmentManager] and [OffsetManager] to coordinate
 * partition assignments and offset management. It also implements the [ConsumerRebalanceListener] to receive
 * notifications about partition revocations and assignments.
 *
 * @param TKey The type of the key in Kafka records.
 * @param TValue The type of the value in Kafka records.
 * @property quafkaConsumerOptions Consumer configuration options.
 * @property consumer The underlying [KafkaConsumer] instance.
 * @property partitionAssignmentManager Manages partition assignments for this consumer.
 * @property offsetManager Manages tracking and committing offsets.
 * @property scope The coroutine scope used to launch asynchronous tasks.
 * @property logger Logger used for logging events and errors.
 * @property waiter An optional [Waiter] to signal when the consumer has stopped.
 */
internal class PollingConsumer<TKey, TValue>(
    private val quafkaConsumerOptions: QuafkaConsumerOptions<TKey, TValue>,
    private val consumer: KafkaConsumer<TKey, TValue>,
    private val partitionAssignmentManager: PartitionAssignmentManager<TKey, TValue>,
    private val offsetManager: OffsetManager<TKey, TValue>,
    private val scope: CoroutineScope,
    private val logger: Logger,
    private val waiter: Waiter?
) : ConsumerRebalanceListener {
    private val isClosing = AtomicBoolean(false)
    private val eventPublisher = quafkaConsumerOptions.eventBus

    /**
     * Starts the polling consumer.
     *
     * This method subscribes the consumer to the configured topics, publishes a subscription event,
     * and launches a coroutine to continuously poll for records. When polling ends, the consumer
     * is gracefully shut down and the wait group is notified.
     *
     * @throws Throwable If an error occurs during startup.
     */
    internal fun start() {
        val topics = quafkaConsumerOptions.subscribedTopics()
        logger.info("Quafka consumer starting... | subscribing topics: ${topics.joinToString()} ")
        try {
            consumer.subscribe(topics, this)
            eventPublisher.publish(Events.Subscribed(topics, quafkaConsumerOptions.toDetail()))
            scope.launch {
                try {
                    startPolling()
                } finally {
                    shutdownConsumer()
                    waiter?.done()
                }
            }
        } catch (exception: Throwable) {
            exception.rethrowIfFatalOrCancelled()
            logger.warn("An error occurred when starting consumer", exception)
            throw exception
        }
    }

    /**
     * Shuts down the consumer gracefully.
     *
     * This method stops partition workers, flushes offsets synchronously, wakes up and unsubscribes
     * the consumer, closes the consumer within the graceful shutdown timeout, and publishes a consumer
     * stopped event.
     */
    private fun shutdownConsumer() {
        logger.info("Stopping Quafka consumer...")
        isClosing.set(true)
        try {
            partitionAssignmentManager.stopWorkers()
            offsetManager.flushOffsetsSync() // Final synchronous offset flush
            consumer.wakeup()
            consumer.unsubscribe()
            consumer.close(quafkaConsumerOptions.gracefulShutdownTimeout.toJavaDuration())
            logger.info("Quafka consumer stopped.")
            eventPublisher.publish(Events.ConsumerStopped(quafkaConsumerOptions.toDetail()))
        } catch (e: Throwable) {
            logger.warn("An error occurred when closing consumer", e)
            e.rethrowIfFatalOrCancelled()
        }
    }

    /**
     * Continuously polls Kafka for records and dispatches incoming messages.
     *
     * This suspend function runs in a loop as long as the coroutine scope is active and the consumer
     * is not closing. It processes waiting tasks (such as flushing offsets and pausing/resuming partitions),
     * polls records from Kafka, triggers partition rebalancing, and dispatches messages for processing.
     */
    private suspend fun startPolling() {
        val pollTimeout = quafkaConsumerOptions.pollDuration.toJavaDuration()
        while (scope.isActive && !isClosing.get()) {
            try {
                processWaitingTasks()
                val now = quafkaConsumerOptions.timeProvider.now()
                val records = consumer.poll(pollTimeout)
                if (records.isEmpty) {
                    continue
                }
                records.partitions().map { topicPartition ->
                    val partitionRecords = records.records(topicPartition)
                    val messages = ArrayList<IncomingMessage<TKey, TValue>>(partitionRecords.size)
                    partitionRecords.forEach { record ->
                        val message = IncomingMessage.create(
                            consumerRecord = record,
                            incomingMessageStringFormatter = quafkaConsumerOptions.incomingMessageStringFormatter,
                            groupId = quafkaConsumerOptions.getGroupId(),
                            clientId = quafkaConsumerOptions.getClientId(),
                            consumedAt = now
                        )
                        messages.add(message)
                    }
                    eventPublisher.publish(Events.MessagesReceived(messages, quafkaConsumerOptions.toDetail()))
                    dispatch(topicPartition, messages)
                }
            } catch (_: WakeupException) {
                if (!scope.isActive || isClosing.get()) {
                    break
                }
            } catch (_: CancellationException) {
                // CancellationExceptions are ignored.
            } catch (ex: Throwable) {
                logger.warn("Error polling Kafka records!", ex)
                eventPublisher.publish(Events.PollingExceptionOccurred(ex, quafkaConsumerOptions.toDetail()))
                ex.rethrowIfFatalOrCancelled()
            }
        }
    }

    /**
     * Dispatches a list of messages for a given topic partition.
     *
     * The messages are processed in offset order. For each message, the method checks if the partition is
     * assigned and not paused. If the message is acceptable for processing, it is offered to the assigned
     * partition's worker.
     *
     * @param topicPartition The topic partition from which the messages were polled.
     * @param messages The list of incoming messages to dispatch.
     */
    private suspend fun dispatch(topicPartition: TopicPartition, messages: List<IncomingMessage<TKey, TValue>>) {
        for (message in messages) {
            if (!scope.isActive) {
                break
            }

            val assignedPartition = partitionAssignmentManager.get(topicPartition)
            if (assignedPartition == null) {
                logger.warn(
                    "Message from unassigned partition, ignoring. | {}",
                    message.toString(Level.WARN, addValue = true, addHeaders = true)
                )
                break
            }

            if (assignedPartition.isPaused) {
                if (logger.isTraceEnabled) {
                    logger.trace(
                        "Skipping message from paused partition. | {}",
                        message.toString(Level.TRACE)
                    )
                }
                break
            }

            when (assignedPartition.offerMessage(message)) {
                EnqueueResult.Ok -> {
                    continue
                }

                EnqueueResult.WorkerClosed -> {
                    if (logger.isTraceEnabled) {
                        logger.trace(
                            "Worker closed, message will be dropped. | {}",
                            message.toString(Level.TRACE)
                        )
                    }
                    break
                }

                EnqueueResult.Backpressure -> {
                    if (logger.isTraceEnabled) {
                        logger.trace(
                            "Backpressure activated, message will be dropped. | {}",
                            message.toString(Level.TRACE)
                        )
                    }
                    break
                }
            }
        }
    }

    /**
     * Processes waiting tasks such as flushing offsets and managing partition pause/resume states.
     *
     * This method is called in each polling cycle to ensure that any pending offset commits
     * are flushed and that partition pause/resume events are handled accordingly.
     */
    private fun processWaitingTasks() {
        offsetManager.tryToFlushOffsets()
        partitionAssignmentManager.pauseResumePartitions()
    }

    /**
     * Callback invoked when partitions are revoked during a rebalance.
     *
     * This method publishes a partitions revoked event and flushes the offsets for the revoked partitions
     * synchronously.
     *
     * @param revokedPartitions The collection of topic partitions that have been revoked.
     */
    override fun onPartitionsRevoked(revokedPartitions: MutableCollection<TopicPartition>) {
        if (logger.isDebugEnabled) {
            logger.debug(
                "Partitions revoked: {}",
                revokedPartitions.toLogString()
            )
        }
        partitionAssignmentManager.revokePartitions(revokedPartitions)
        eventPublisher.publish(Events.PartitionsRevoked(revokedPartitions, quafkaConsumerOptions.toDetail()))
    }

    /**
     * Callback invoked when partitions are assigned during a rebalance.
     *
     * This method creates a list of [TopicPartitionOffset] from the assigned partitions using the current
     * consumer positions, and publishes a partitions assigned event.
     *
     * @param assignedPartitions The collection of topic partitions that have been assigned.
     */
    override fun onPartitionsAssigned(assignedPartitions: MutableCollection<TopicPartition>) {
        val offsets = assignedPartitions.map { tp ->
            TopicPartitionOffset(tp, consumer.position(tp))
        }
        if (logger.isDebugEnabled) {
            logger.debug("Partitions assigned. | {}", offsets)
        }
        partitionAssignmentManager.assignPartitions(offsets)
        eventPublisher.publish(Events.PartitionsAssigned(offsets, quafkaConsumerOptions.toDetail()))
    }
}
