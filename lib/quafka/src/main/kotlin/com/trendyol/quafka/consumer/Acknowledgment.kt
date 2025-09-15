package com.trendyol.quafka.consumer

import kotlinx.coroutines.*

/**
 * Interface for acknowledging the processing of Kafka messages.
 *
 * Implementations of this interface define strategies for acknowledging and committing
 * message offsets.
 */
interface Acknowledgment {
    /**
     * Acknowledges the successful processing of a message.
     *
     * @param incomingMessage The [IncomingMessage] being acknowledged.
     */
    fun acknowledge(incomingMessage: IncomingMessage<*, *>)

    /**
     * Commits the offset of a processed message to Kafka.
     *
     * @param incomingMessage The [IncomingMessage] whose offset should be committed.
     * @return A [Deferred] representing the completion of the commit operation.
     */
    fun commit(incomingMessage: IncomingMessage<*, *>): Deferred<Unit>
}

/**
 * Acknowledges all messages in the collection by processing them in order of their offsets.
 *
 * @receiver A collection of [IncomingMessage]s to be acknowledged.
 */
fun Collection<IncomingMessage<*, *>>.ackAll() {
    this
        .sortedBy { incomingMessage -> incomingMessage.offset }
        .forEach { incomingMessage ->
            incomingMessage.ack()
        }
}

/**
 * Commits the offsets of all messages in the collection to Kafka by processing them in order of their offsets.
 *
 * This function waits for all commit operations to complete before returning.
 *
 * @receiver A collection of [IncomingMessage]s whose offsets should be committed.
 * @throws Exception If any of the commit operations fail.
 */
suspend fun Collection<IncomingMessage<*, *>>.commitAll() {
    this
        .sortedBy { incomingMessage -> incomingMessage.offset }
        .map { incomingMessage ->
            incomingMessage.commit()
        }.awaitAll()
}
