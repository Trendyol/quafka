package com.trendyol.quafka.consumer.internals

/**
 * Represents the result of attempting to enqueue a message for processing.
 *
 * This enum is used to indicate the outcome when a message is offered to a processing worker.
 *
 * - [WorkerClosed]: The worker responsible for processing messages is closed, so the message could not be enqueued.
 * - [Backpressure]: Backpressure is active, indicating that the system is overloaded and the message was not enqueued.
 * - [Ok]: The message was successfully enqueued for processing.
 */
internal enum class EnqueueResult {
    /**
     * Indicates that the worker has been closed, and no further messages can be enqueued.
     */
    WorkerClosed,

    /**
     * Indicates that backpressure is currently active, preventing the message from being enqueued.
     */
    Backpressure,

    /**
     * Indicates that the message was successfully enqueued for processing.
     */
    Ok
}
