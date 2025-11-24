package com.trendyol.quafka.consumer.internals

import kotlinx.coroutines.CompletableDeferred

/**
 * Represents the state of an offset including its commit or acknowledgment mode,
 * retry attempts, and completion status.
 *
 * This class is used to track the progress of offset operations such as committing or
 * acknowledging an offset. It supports retrying commit operations until the maximum number
 * of retry attempts is reached.
 *
 * @property offset The offset value associated with this state.
 * @property maxRetryAttempts The maximum number of retry attempts allowed for committing the offset.
 */
internal class OffsetState(val offset: Long, val maxRetryAttempts: Int = 0) {
    /**
     * Indicates whether this offset is set for acknowledgment or commit.
     *
     * This property remains null until the mode is explicitly set using [ack] or [commit].
     */
    var commitMode: CommitMode? = null
        private set

    /**
     * A continuation that will be completed when the commit operation finishes.
     *
     * This is only set when the [commit] method is used.
     */
    var continuation: CompletableDeferred<Unit>? = null
        private set

    var isCompleted: Boolean = false
        private set

    /**
     * Stores an exception if the offset operation fails.
     */
    var exception: Throwable? = null
        private set

    /**
     * The number of retry attempts that have been made for this offset.
     */
    var retryAttempt: Int = 0
        private set

    /**
     * Checks if the offset operation has been successfully completed without errors.
     *
     * @return `true` if the operation is completed and no exception occurred; `false` otherwise.
     */
    fun isSuccessfullyCompleted() = isCompleted && exception == null

    /**
     * Returns `true` if this offset state is in commit mode.
     */
    val isCommitMode get() = commitMode == CommitMode.Commit

    /**
     * Returns `true` if this offset state is in acknowledgment mode.
     */
    val isAckMode get() = commitMode == CommitMode.Ack

    /**
     * Returns `true` if this offset state is ready for processing, meaning it is either in commit or ack mode.
     */
    val isReady get() = isCommitMode || isAckMode

    /**
     * Completes the offset operation, marking it as either successful or failed.
     *
     * If an [exception] is provided, the operation is considered a failure.
     * If the number of retry attempts has reached [maxRetryAttempts], the operation is marked as completed
     * and the associated [continuation] is completed exceptionally. Otherwise, the [retryAttempt] counter is
     * incremented and the exception is stored.
     *
     * If no exception is provided, the operation is considered successful, the [continuation] is completed,
     * and the operation is marked as completed.
     *
     * @param exception An optional [Throwable] indicating a failure; if `null`, the operation is successful.
     */
    fun complete(exception: Throwable? = null) {
        if (exception != null) {
            this.exception = exception
            if (retryAttempt >= maxRetryAttempts) {
                this.isCompleted = true
                continuation?.completeExceptionally(exception)
                return
            } else {
                // Increment retry attempt and keep the operation incomplete
                this.exception = exception
                this.retryAttempt += 1
                return
            }
        } else {
            // Success case: complete the continuation and mark as completed.
            continuation?.complete(Unit)
            this.isCompleted = true
        }
    }

    /**
     * Marks the offset state for acknowledgment.
     *
     * If the commit mode has already been set, this method does nothing.
     *
     * @return This [OffsetState] instance to allow method chaining.
     */
    fun ack() = apply {
        if (this.commitMode != null) {
            return this
        }
        this.commitMode = CommitMode.Ack
    }

    /**
     * Marks the offset state for commit and sets the [continuation] to be completed upon commit completion.
     *
     * If the commit mode has already been set, this method overrides existing commit mode
     *
     * @param continuation A [CompletableDeferred] that will be resumed when the commit completes.
     * @return This [OffsetState] instance to allow method chaining.
     */
    fun commit(continuation: CompletableDeferred<Unit>) = apply {
        this.commitMode = CommitMode.Commit
        this.continuation = continuation
    }

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other !is OffsetState) return false

        if (offset != other.offset) return false

        return true
    }

    override fun hashCode(): Int = offset.hashCode()

    override fun toString(): String = "OffsetState(offset=$offset, " +
        " maxRetryAttempts=$maxRetryAttempts, commitMode=$commitMode, " +
        "isCompleted=$isCompleted, exception=$exception, retryAttempt=$retryAttempt, " +
        "isCommitMode=$isCommitMode, isAckMode=$isAckMode, isReady=$isReady)"

    /**
     * Enum representing the mode of offset operation.
     */
    enum class CommitMode {
        /**
         * Indicates that the offset is being acknowledged.
         */
        Ack,

        /**
         * Indicates that the offset is being committed.
         */
        Commit
    }
}
