package com.trendyol.quafka.consumer.internals

import com.trendyol.quafka.common.rethrowIfFatal
import com.trendyol.quafka.common.rethrowIfFatalOrCancelled
import com.trendyol.quafka.consumer.Events
import com.trendyol.quafka.consumer.Events.toDetail
import com.trendyol.quafka.consumer.TopicPartition
import com.trendyol.quafka.consumer.configuration.QuafkaConsumerOptions
import com.trendyol.quafka.consumer.toLogString
import kotlinx.coroutines.CoroutineScope
import org.apache.kafka.clients.consumer.*
import org.apache.kafka.common.errors.WakeupException
import org.slf4j.Logger
import java.time.Duration
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicLong
import kotlin.collections.isNotEmpty
import kotlin.coroutines.*
import kotlin.time.toKotlinDuration

/**
 * Manages offset tracking, committing, and flushing for a Kafka consumer.
 *
 * This class tracks offsets for each [TopicPartition] and handles both synchronous and
 * asynchronous commit operations. It also provides functionality to acknowledge, track, and
 * untrack offsets for individual messages.
 *
 * @param TKey The type of the key in the Kafka messages.
 * @param TValue The type of the value in the Kafka messages.
 * @property kafkaConsumer The Kafka consumer used for committing offsets.
 * @property quafkaConsumerOptions Consumer-specific configuration options.
 * @property logger Logger for logging events and errors.
 */
internal class OffsetManager<TKey, TValue>(
    private val kafkaConsumer: KafkaConsumer<TKey, TValue>,
    private val quafkaConsumerOptions: QuafkaConsumerOptions<TKey, TValue>,
    private val logger: Logger
) {
    private val eventPublisher = quafkaConsumerOptions.eventBus
    private val commitSequence = AtomicLong(0)

    @Volatile
    private var latestFlushCommitAt = quafkaConsumerOptions.timeProvider.now()
    private val topicPartitionOffsets: ConcurrentHashMap<TopicPartition, TopicPartitionOffsets> = ConcurrentHashMap()

    fun register(topicPartition: TopicPartition, scope: CoroutineScope): TopicPartitionOffsets = topicPartitionOffsets.computeIfAbsent(
        topicPartition
    ) { key ->
        TopicPartitionOffsets(quafkaConsumerOptions.commitOptions, logger, key, scope)
    }

    fun unregister(topicPartition: TopicPartition) {
        topicPartitionOffsets.remove(topicPartition)
    }

    /**
     * Attempts to flush offsets synchronously if either the commit duration has elapsed or
     * there are pending commits.
     */
    fun tryToFlushOffsets() {
        val now = this.quafkaConsumerOptions.timeProvider.now()
        val diff = Duration.between(latestFlushCommitAt, now).toKotlinDuration()
        val pendingCommits = topicPartitionOffsets.values.any { it.hasPendingCommit() }
        if (diff > quafkaConsumerOptions.commitOptions.duration || pendingCommits) {
            flushOffsetsSync()
            this.latestFlushCommitAt = now
        }
    }

    /**
     * Flushes the tracked offsets synchronously by committing them via the Kafka consumer.
     *
     * Optionally, only offsets for the specified [topicPartitions] can be flushed.
     *
     * @param topicPartitions Optional collection of topic partitions to flush. If empty, flushes all.
     */
    fun flushOffsetsSync(topicPartitions: Collection<TopicPartition> = emptyList()) {
        val allWaitingOffsets = getReadyOffsets(topicPartitions)
        if (allWaitingOffsets.isEmpty()) {
            return
        }

        val latestOffsets = getLatest(allWaitingOffsets)
        if (logger.isTraceEnabled) {
            logger.debug("offsets will be committed: {}", latestOffsets.toLogString())
        }
        try {
            commitOffsetSync(latestOffsets)
            for ((assigned, offsets) in allWaitingOffsets) {
                assigned.completeOffsets(offsets)
            }
        } catch (ex: Throwable) {
            ex.rethrowIfFatal()
            logger.warn("Error flushing offsets sync. | latest offsets: {}", latestOffsets.toLogString(), ex)
            for ((assigned, offsets) in allWaitingOffsets) {
                assigned.completeOffsets(offsets, ex)
            }
            throw ex
        }
    }

    /**
     * Retrieves the [OffsetState] for a specific offset in the given [topicPartition].
     *
     * @param topicPartition The topic partition to search in.
     * @param offset The offset to retrieve.
     * @return The [OffsetState] if found, or null otherwise.
     */
    fun getOffset(topicPartition: TopicPartition, offset: Long): OffsetState? =
        topicPartitionOffsets[topicPartition]?.getOffset(offset)

    /**
     * Flushes tracked offsets asynchronously by committing them via the Kafka consumer.
     *
     * Optionally, only offsets for the specified [topicPartitions] can be flushed. If [fireAndForget]
     * is true, the caller will not wait for the commit callback.
     *
     * @param topicPartitions Optional collection of topic partitions to flush. If empty, flushes all.
     * @param fireAndForget If true, the commit operation will not suspend for the result.
     */
    suspend fun flushOffsetsAsync(
        topicPartitions: Collection<TopicPartition> = emptyList(),
        fireAndForget: Boolean = false
    ) {
        val allWaitingOffsets = getReadyOffsets(topicPartitions)
        if (allWaitingOffsets.isEmpty()) {
            return
        }

        val latestOffsets = getLatest(allWaitingOffsets)
        try {
            suspendCoroutine { cont ->
                var resumed = false
                try {
                    kafkaConsumer.commitAsync(latestOffsets, KeepOrderAsyncCommit(latestOffsets, cont, fireAndForget))
                } catch (ex: Throwable) {
                    cont.resumeWithException(ex)
                    resumed = true
                } finally {
                    if (fireAndForget && !resumed) {
                        cont.resume(Unit)
                    }
                }
            }

            for ((assigned, offsets) in allWaitingOffsets) {
                assigned.completeOffsets(offsets)
            }
        } catch (ex: Throwable) {
            ex.rethrowIfFatalOrCancelled()
            logger.warn("Error flushing offsets async. | latest offsets: {}", latestOffsets.toLogString(), ex)
            for ((assigned, offsets) in allWaitingOffsets) {
                assigned.completeOffsets(offsets, ex)
            }
            throw ex
        }
    }

    /**
     * Commits the provided [offsets] synchronously using the Kafka consumer.
     *
     * This method retries the commit if a [WakeupException] is encountered.
     *
     * @param offsets A map of topic partitions to their corresponding offset and metadata.
     */
    private fun commitOffsetSync(offsets: Map<TopicPartition, OffsetAndMetadata>) {
        try {
            kafkaConsumer.commitSync(offsets)
        } catch (e: WakeupException) {
            kafkaConsumer.commitSync(offsets)
        }
        if (logger.isDebugEnabled) {
            logger.debug("Offset committed (sync). | offsets: {}", offsets.toLogString())
        }
        eventPublisher.publish(Events.OffsetsCommitted(offsets, true, quafkaConsumerOptions.toDetail()))
    }

    /**
     * Retrieves offsets that are ready to be committed for the specified [topicPartitions].
     *
     * If [topicPartitions] is empty, offsets for all partitions are retrieved.
     *
     * @param topicPartitions Optional collection of topic partitions to filter.
     * @return A map where keys are [TopicPartitionOffsets] and values are lists of pending [OffsetState] instances.
     */
    fun getReadyOffsets(topicPartitions: Collection<TopicPartition>): Map<TopicPartitionOffsets, List<OffsetState>> {
        val result = mutableMapOf<TopicPartitionOffsets, List<OffsetState>>()
        for ((tp, partitionOffsets) in topicPartitionOffsets) {
            if (topicPartitions.isNotEmpty() && !topicPartitions.contains(tp)) {
                continue
            }

            val pendingOffsets = partitionOffsets.getPendingOffsets()
            if (pendingOffsets.isNotEmpty()) {
                result[partitionOffsets] = pendingOffsets
            }
        }
        return result
    }

    /**
     * Determines the latest offsets from all waiting offsets that are eligible for commit.
     *
     * For each [TopicPartitionOffsets] in [allWaitingOffsets], the highest offset that can be committed
     * is determined. The resulting map contains topic partitions paired with [OffsetAndMetadata]
     * (with the offset incremented by one).
     *
     * @param allWaitingOffsets A map of [TopicPartitionOffsets] to lists of pending [OffsetState] instances.
     * @return A map of [TopicPartition] to [OffsetAndMetadata] representing the latest commitable offsets.
     */
    private fun getLatest(
        allWaitingOffsets: Map<TopicPartitionOffsets, List<OffsetState>>
    ): Map<TopicPartition, OffsetAndMetadata> {
        val result = mutableMapOf<TopicPartition, OffsetAndMetadata>()

        for ((assigned, offsets) in allWaitingOffsets) {
            var maxOffset: Long? = null

            // Single pass to find max committable offset
            for (offset in offsets) {
                if (assigned.canBeCommitted(offset.offset)) {
                    if (maxOffset == null || offset.offset > maxOffset) {
                        maxOffset = offset.offset
                    }
                }
            }

            if (maxOffset != null) {
                result[assigned.topicPartition] = OffsetAndMetadata(maxOffset + 1)
            }
        }
        return result
    }

    /**
     * A callback implementation for asynchronous commit operations that ensures the commit order
     * is maintained.
     *
     * @property committingOffsets The offsets that are being committed.
     * @property continuation The coroutine continuation to resume once the commit is completed.
     * @property fireAndForget If true, the callback will resume immediately without waiting.
     */
    private inner class KeepOrderAsyncCommit(
        private val committingOffsets: Map<TopicPartition, OffsetAndMetadata>,
        private val continuation: Continuation<Unit>,
        private val fireAndForget: Boolean
    ) : OffsetCommitCallback {
        private val position = commitSequence.incrementAndGet()

        /**
         * Called when the asynchronous commit operation completes.
         *
         * If an exception occurs and the commit sequence position matches, the commit is retried.
         * Otherwise, the continuation is resumed with the result or exception.
         *
         * @param offsets The offsets that were committed.
         * @param exception An exception if the commit failed; null if successful.
         */
        override fun onComplete(
            offsets: MutableMap<TopicPartition, OffsetAndMetadata>,
            exception: Exception?
        ) {
            if (exception != null) {
                if (position == commitSequence.get()) {
                    // Retry the commit if the current position is still valid.
                    kafkaConsumer.commitAsync(committingOffsets, this)
                } else {
                    continuation.resumeWithException(exception)
                }
            } else {
                if (logger.isDebugEnabled) {
                    logger.debug("Offset committed (async). | offsets: {}", offsets.toLogString())
                }
                eventPublisher.publish(Events.OffsetsCommitted(offsets, false, quafkaConsumerOptions.toDetail()))
                if (!fireAndForget) {
                    continuation.resume(Unit)
                }
            }
        }
    }
}
