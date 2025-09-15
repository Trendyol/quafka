package com.trendyol.quafka.consumer.internals

import com.trendyol.quafka.consumer.IncomingMessage
import com.trendyol.quafka.consumer.configuration.CommitOptions
import kotlinx.coroutines.*
import org.apache.kafka.common.TopicPartition
import org.slf4j.Logger
import org.slf4j.event.Level
import java.util.concurrent.ConcurrentSkipListMap
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicLong

/**
 * Manages offset states for a specific [TopicPartition].
 *
 * This class tracks the state of offsets (e.g. acknowledged or committed) for messages processed
 * on a Kafka topic partition. It supports tracking, acknowledging, committing, and finalizing offsets,
 * while enforcing commit ordering and handling commit gaps if specified by [CommitOptions].
 *
 * @property commitOptions Configuration options related to offset commits.
 * @property logger Logger used for logging messages and errors.
 * @property topicPartition The Kafka topic partition for which offsets are tracked.
 */
internal class TopicPartitionOffsets(
    private val commitOptions: CommitOptions,
    private val logger: Logger,
    val topicPartition: TopicPartition,
    private val coroutineScope: CoroutineScope
) {
    private val offsets = ConcurrentSkipListMap<Long, OffsetState>()
    private val latestCommittedOffset: AtomicLong = AtomicLong(AssignedTopicPartition.UNASSIGNED_OFFSET)
    private val pendingCommitCount = AtomicInteger(0)

    companion object {
        private val completedDeferred = CompletableDeferred(Unit)
    }

    /**
     * Returns the total number of tracked offsets.
     *
     * @return The number of offset states currently being tracked.
     */
    fun totalOffset() = offsets.size

    /**
     * Checks if there is any pending commit in the tracked offsets.
     *
     * @return `true` if at least one tracked offset is in commit mode; `false` otherwise.
     */
    fun hasPendingCommit(): Boolean = pendingCommitCount.get() > 0

    /**
     * Retrieves a list of offset states that are ready to be committed.
     *
     * When [commitOptions.deferCommitsUntilNoGaps] is enabled, this method returns a contiguous list
     * of offset states starting from the lowest offset, stopping when a gap is encountered or when a
     * non-ready state is found. Otherwise, all ready offset states are returned sorted by offset.
     *
     * @return A list of [OffsetState] instances that are ready for commit.
     */
    fun getPendingOffsets(): List<OffsetState> {
        if (this.offsets.isEmpty()) {
            return emptyList()
        }

        if (this.commitOptions.deferCommitsUntilNoGaps) {
            val first = offsets.firstEntry() ?: return emptyList()
            if (!first.value.isReady) return emptyList()
            val res = ArrayList<OffsetState>()
            var e = first
            while (true) {
                res.add(e.value)
                val next = offsets.higherEntry(e.key) ?: break
                if (!next.value.isReady) break
                e = next
            }
            return res
        } else {
            val res = ArrayList<OffsetState>()
            for (v in offsets.values) {
                if (v.isReady) res.add(v)
            }
            return res
        }
    }

    /**
     * Retrieves the [OffsetState] associated with the given offset.
     *
     * @param offset The offset value to look up.
     * @return The [OffsetState] for the given offset, or `null` if not found.
     */
    fun getOffset(offset: Long): OffsetState? = offsets[offset]

    /**
     * Checks if an offset can be committed.
     *
     * An offset is considered committable if it exists, is ready for commit, and is greater than the latest
     * committed offset.
     *
     * @param offset The offset value to check.
     * @return `true` if the offset can be committed; `false` otherwise.
     */
    fun canBeCommitted(offset: Long): Boolean {
        val offsetRecord = offsets[offset]
        return offsetRecord != null && offsetRecord.isReady && this.latestCommittedOffset.get() < offset
    }

    /**
     * Returns the latest committed offset.
     *
     * @return The most recent committed offset value.
     */
    fun getLatestCommitedOffset() = latestCommittedOffset.get()

    /**
     * Determines whether the given [incomingMessage] is valid for commit.
     *
     * A message is valid if either out-of-order commits are allowed by [commitOptions.allowOutOfOrderCommit]
     * or if its offset is greater than the latest committed offset.
     *
     * @param incomingMessage The message to validate.
     * @return `true` if the message is valid for commit; `false` otherwise.
     */
    private fun isValidForCommit(incomingMessage: IncomingMessage<*, *>): Boolean =
        commitOptions.allowOutOfOrderCommit || latestCommittedOffset.get() < incomingMessage.offset

    /**
     * Acknowledges an incoming message.
     *
     * This method marks the offset associated with [incomingMessage] as acknowledged by updating its [OffsetState].
     * If the message is not valid for commit (already acknowledged or committed), no action is taken.
     *
     * @param incomingMessage The message to acknowledge.
     * @return `true` if the acknowledgment was successful; `false` otherwise.
     */
    fun acknowledge(incomingMessage: IncomingMessage<*, *>): Boolean {
        if (!isValidForCommit(incomingMessage)) {
            if (logger.isTraceEnabled) {
                logger.trace(
                    "Message already acknowledged. | {}",
                    incomingMessage.toString(Level.DEBUG)
                )
            }
            return false
        }

        val offset = offsets.compute(
            incomingMessage.offset
        ) { _, o ->
            o?.apply { ack() }
        }
        if (offset == null) {
            logger.warn("Offset record not found when acknowledging | {}", incomingMessage.toString(Level.WARN))
            return false
        }

        if (logger.isDebugEnabled) {
            logger.debug(
                "Message marked as acknowledged. | {}",
                incomingMessage.toString(Level.DEBUG)
            )
        }
        return true
    }

    /**
     * Commits the offset associated with the given [incomingMessage].
     *
     * If the message is already committed or not valid for commit, a completed [Deferred] is returned immediately.
     * Otherwise, the method updates the [OffsetState] to commit mode and returns a [Deferred] that will be completed
     * when the commit operation finishes.
     *
     * @param incomingMessage The message whose offset should be committed.
     * @return A [Deferred] representing the commit operation.
     */
    fun commit(incomingMessage: IncomingMessage<*, *>): Deferred<Unit> {
        if (!isValidForCommit(incomingMessage)) {
            if (logger.isTraceEnabled) {
                logger.trace(
                    "Message already committed. | {}",
                    incomingMessage.toString(Level.TRACE)
                )
            }
            return completedDeferred
        }

        var switchedToCommit = false
        val offset = offsets.compute(
            incomingMessage.offset
        ) { _, o ->
            o?.apply {
                if (!this.isCommitMode) {
                    switchedToCommit = true
                }
                commit(CompletableDeferred<Unit>(coroutineScope.coroutineContext.job))
            }
        }
        if (offset == null) {
            logger.warn("Offset record not found when committing | {}", incomingMessage.toString(Level.WARN))
            return completedDeferred // or cancellation
        }
        if (switchedToCommit) {
            pendingCommitCount.incrementAndGet()
        }

        if (logger.isDebugEnabled) {
            logger.debug(
                "Message marked as committed. | {}",
                incomingMessage.toString(Level.DEBUG)
            )
        }

        return offset.continuation!!
    }

    /**
     * Completes the commit operation for a collection of [OffsetState] instances.
     *
     * For each offset state in [offsetStates], this method marks the operation as complete,
     * logs the result, updates the latest committed offset if the commit was successful, and removes
     * the offset from tracking.
     *
     * @param offsetStates The collection of offset states to complete.
     * @param exception An optional exception that occurred during commit; `null` if the commit was successful.
     */
    fun completeOffsets(offsetStates: Collection<OffsetState>, exception: Throwable? = null) {
        offsetStates
            .forEach { offsetRecord ->
                val offset = offsetRecord.offset
                offsetRecord.complete(exception)

                if (offsetRecord.isCompleted) {
                    if (offsetRecord.isSuccessfullyCompleted()) {
                        if (logger.isTraceEnabled) {
                            logger.trace("Commit offset completed. | topic partition:{} | offset:{}", topicPartition, offset)
                        }
                        tryToUpdateLatestCommittedOffset(offset)
                    } else {
                        logger.warn("Commit offset failed. | topic partition:{} | offset:{}", topicPartition, offset, exception)
                    }
                    if (offsetRecord.isCommitMode) {
                        pendingCommitCount.decrementAndGet()
                    }
                    this.offsets.remove(offset)
                } else {
                    logger.warn(
                        "Commit offset failed, will try to commit again. | topic partition:{} | offset:{}",
                        topicPartition,
                        offset,
                        exception
                    )
                }
            }
    }

    /**
     * Updates the latest committed offset.
     *
     * This method updates the [latestCommittedOffset] if the provided [offset] is greater than the current value.
     *
     * @param offset The offset to update as the latest committed.
     */
    private fun tryToUpdateLatestCommittedOffset(offset: Long) {
        this.latestCommittedOffset.updateAndGet { currentOffset ->
            when {
                currentOffset > offset -> currentOffset
                else -> offset
            }
        }
    }

    /**
     * Removes the tracking of an incoming message's offset.
     *
     * @param incomingMessage The message whose offset tracking should be removed.
     */
    fun untrack(incomingMessage: IncomingMessage<*, *>) {
        val removed = offsets.remove(incomingMessage.offset)
        if (removed?.isCommitMode == true && !removed.isCompleted) {
            pendingCommitCount.decrementAndGet()
        }
    }

    /**
     * Tracks a new incoming message by adding its offset state.
     *
     * A new [OffsetState] is created for the message, assigned a sequential number, and stored in the tracking map.
     *
     * @param incomingMessage The message to track.
     */
    fun track(incomingMessage: IncomingMessage<*, *>) {
        offsets
            .compute(incomingMessage.offset) { _, _ ->
                OffsetState(incomingMessage.offset, maxRetryAttempts = commitOptions.maxRetryAttemptOnFail)
            }?.also { state ->
                if (logger.isTraceEnabled) {
                    logger.trace("Offset added to tracking list. | {} | {}", incomingMessage.toString(Level.TRACE), state)
                }
            }
    }
}
