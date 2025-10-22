package com.trendyol.quafka.consumer.internals

import com.trendyol.quafka.consumer.*
import com.trendyol.quafka.consumer.Events.toDetail
import com.trendyol.quafka.consumer.configuration.QuafkaConsumerOptions
import com.trendyol.quafka.consumer.internals.AssignedTopicPartition.Companion.UNASSIGNED_OFFSET
import kotlinx.coroutines.*
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.slf4j.Logger
import java.util.concurrent.*

/**
 * Manages the assignment, revocation, and updating of partitions for a Kafka consumer.
 *
 * This class is responsible for handling partition rebalancing. It compares the current assignment
 * from the [KafkaConsumer] with the internal state, revokes partitions that are no longer assigned,
 * creates new partition assignments, and updates partitions that remain the same. It also manages
 * pausing and resuming of partitions and coordinates offset flushing with the [OffsetManager].
 *
 * @param TKey The type of the key in Kafka records.
 * @param TValue The type of the value in Kafka records.
 * @property kafkaConsumer The Kafka consumer instance to be managed.
 * @property logger The logger for logging informational and error messages.
 * @property parentScope The coroutine scope for launching asynchronous tasks.
 * @property quafkaConsumerOptions Consumer configuration options including subscription and commit options.
 * @property offsetManager The manager responsible for tracking and committing offsets.
 */
internal class PartitionAssignmentManager<TKey, TValue>(
    private val kafkaConsumer: KafkaConsumer<TKey, TValue>,
    private val logger: Logger,
    private val parentScope: CoroutineScope,
    private val quafkaConsumerOptions: QuafkaConsumerOptions<TKey, TValue>,
    private val offsetManager: OffsetManager<TKey, TValue>
) {
    private val eventPublisher = quafkaConsumerOptions.eventBus
    private val assignedTopicPartitions: ConcurrentMap<TopicPartition, AssignedTopicPartition<TKey, TValue>> =
        ConcurrentHashMap()

    /**
     * Retrieves the assigned topic partition for the given [topicPartition].
     *
     * @param topicPartition The topic partition to look up.
     * @return The [AssignedTopicPartition] associated with the given [topicPartition], or null if not assigned.
     */
    fun get(topicPartition: TopicPartition): AssignedTopicPartition<TKey, TValue>? =
        assignedTopicPartitions[topicPartition]

    /**
     * Retrieves all currently assigned topic partitions.
     *
     * @return A collection of all [AssignedTopicPartition] instances.
     */
    fun getAll(): Collection<AssignedTopicPartition<TKey, TValue>> = assignedTopicPartitions.values

    /**
     * Rebalances partition assignments based on the current consumer assignment.
     *
     * This method retrieves the current set of assigned partitions from the [KafkaConsumer].
     * If there is a discrepancy between the internal assignment and the consumer's assignment,
     * it calculates the current offsets and reassigns partitions accordingly.
     */
    fun rebalance() {
        // Retrieve current assignments from KafkaConsumer.
        val assignments = kafkaConsumer.assignment()
        if (assignedTopicPartitions.keys != assignments) {
            // Map each TopicPartition to its current offset.
            val topicPartitionOffsets = assignments.map { tp ->
                TopicPartitionOffset(tp, kafkaConsumer.position(tp))
            }
            assignPartitions(topicPartitionOffsets)
        }
    }

    /**
     * Revokes the specified partitions from the consumer.
     *
     * For each revoked partition, this method stops its worker, removes it from the internal state,
     * flushes its offsets synchronously, and removes offset tracking for the partition.
     *
     * @param revokedTopicPartitions The collection of topic partitions to revoke.
     */
    fun revokePartitions(revokedTopicPartitions: Collection<TopicPartition>) {
        if (revokedTopicPartitions.isEmpty()) {
            return
        }

        val partitionsToFlush = revokedTopicPartitions
            .mapNotNull { revokedPartition ->
                assignedTopicPartitions[revokedPartition]?.let { assignedPartition ->
                    assignedPartition.stopWorker()
                    assignedTopicPartitions.remove(revokedPartition)
                    assignedPartition
                }
            }
        offsetManager.flushOffsetsSync(revokedTopicPartitions)
        partitionsToFlush.forEach { it.close() }
        logger.info("Partitions revoked from consumer. | {}", revokedTopicPartitions.toLogString())
    }

    /**
     * Assigns new partitions to the consumer and updates existing ones.
     *
     * This method handles partition assignment during rebalances by:
     * - Determining new partitions to assign.
     * - Identifying partitions that remain unchanged.
     * - Revoking partitions that are no longer assigned.
     *
     * For new and existing partitions, it updates the assigned offsets and optionally resumes
     * consumption if [resume] is true.
     *
     * @param assignedTopicPartitions A collection of [TopicPartitionOffset] indicating partitions and their offsets.
     * @param resume If true, partitions will resume consumption immediately after assignment.
     */
    fun assignPartitions(
        assignedTopicPartitions: Collection<TopicPartitionOffset>,
        resume: Boolean = true
    ) {
        val assignedTopicPartitionsMap = assignedTopicPartitions.associateBy { it.topicPartition }
        val sameTopicPartitions = assignedTopicPartitionsMap.filterKeys { it in this.assignedTopicPartitions }
        val revokedTopicPartitions = this.assignedTopicPartitions
            .filterKeys { it !in assignedTopicPartitionsMap }
            .map { it.value.topicPartition }
        val newAssignedTopicPartitions = assignedTopicPartitionsMap
            .filterKeys { it !in this.assignedTopicPartitions }

        // Revoke partitions that are no longer assigned.
        revokePartitions(revokedTopicPartitions)

        // Assign new partitions.
        for ((topicPartition, detail) in newAssignedTopicPartitions) {
            val assignedOffset = detail.offset

            val assignedTopicPartition =
                AssignedTopicPartition.create(
                    topicPartition = topicPartition,
                    parentScope = parentScope,
                    quafkaConsumerOptions = quafkaConsumerOptions,
                    initialOffset = assignedOffset,
                    offsetManager = offsetManager
                )
            this.assignedTopicPartitions[topicPartition] = assignedTopicPartition
            if (resume) {
                assignedTopicPartition.resume(assignedOffset, force = true)
            }
        }

        // Update partitions that remain the same.
        for ((topicPartition, detail) in sameTopicPartitions) {
            val assignedTopicPartition = this.assignedTopicPartitions.computeIfPresent(topicPartition) { _, v ->
                v.updateAssignedOffset(detail.offset)
                v
            }
            if (resume && assignedTopicPartition != null) {
                assignedTopicPartition.resume()
            }
        }

        logger.info(
            "Partitions assignment changed. | assigned partitions: {}" +
                " | revoked partitions: {} | partitions that remain the same: {}",
            newAssignedTopicPartitions.values,
            revokedTopicPartitions.toLogString(),
            sameTopicPartitions.values
        )
        eventPublisher.publish(
            Events.RebalanceCompleted(
                this.assignedTopicPartitions.values.map {
                    TopicPartitionOffset(
                        it.topicPartition,
                        it.assignedOffset
                    )
                },
                this.quafkaConsumerOptions.toDetail()
            )
        )
    }

    /**
     * Pauses or resumes partitions based on their current state.
     *
     * This method first releases any active backpressure on partitions, then:
     * - Pauses partitions that are marked as paused but are not yet paused on the consumer,
     *   publishing a pause event for each.
     * - Resumes partitions that are not marked as paused but are currently paused on the consumer,
     *   seeking to the latest paused offset if available, and publishing a resume event.
     */
    fun pauseResumePartitions() {
        // Release backpressure if possible.
        this.assignedTopicPartitions.values.forEach { it.releaseBackpressureIfPossible() }

        val assignedList = assignedTopicPartitions.values
        val currentlyPaused = kafkaConsumer.paused()

        // Identify partitions that need to be paused.
        val partitionsToPause = assignedList.filter { it.isPaused }
        val newlyPausedPartitions = partitionsToPause.filterNot { it.topicPartition in currentlyPaused }

        // Pause newly identified partitions and publish events.
        if (newlyPausedPartitions.isNotEmpty()) {
            kafkaConsumer.pause(newlyPausedPartitions.map { it.topicPartition })
            newlyPausedPartitions.forEach { tp ->
                eventPublisher.publish(
                    Events.TopicPartitionPaused(
                        tp.topicPartition,
                        this.quafkaConsumerOptions.toDetail()
                    )
                )
            }
        }

        // Identify partitions that need to be resumed.
        val partitionsToResume = assignedList.filter { !it.isPaused && it.topicPartition in currentlyPaused }

        // Seek to the latest paused offset if available and resume these partitions.
        if (partitionsToResume.isNotEmpty()) {
            partitionsToResume.forEach { tp ->
                if (tp.latestPausedOffset > UNASSIGNED_OFFSET) {
                    kafkaConsumer.seek(tp.topicPartition, tp.latestPausedOffset)
                }
            }
            kafkaConsumer.resume(partitionsToResume.map { it.topicPartition })
            partitionsToResume.forEach { tp ->
                eventPublisher.publish(
                    Events.TopicPartitionResumed(
                        tp.topicPartition,
                        offset = tp.latestPausedOffset,
                        this.quafkaConsumerOptions.toDetail()
                    )
                )
            }
        }
    }

    /**
     * Stops all workers associated with the assigned partitions.
     *
     * This method iterates over each assigned partition and stops its processing worker.
     */
    fun stopWorkers() {
        this.assignedTopicPartitions.forEach { (_, assigned) ->
            assigned.stopWorker()
        }
    }
}
