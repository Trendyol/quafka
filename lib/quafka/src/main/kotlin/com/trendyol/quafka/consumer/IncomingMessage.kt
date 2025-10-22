package com.trendyol.quafka.consumer

import com.trendyol.quafka.common.QuafkaHeader
import kotlinx.coroutines.*
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.header.Header
import org.apache.kafka.common.record.TimestampType
import org.slf4j.event.Level
import java.time.Instant
import kotlin.jvm.optionals.getOrNull

/**
 * Represents an incoming Kafka message and provides utility methods for message processing.
 *
 * This class encapsulates metadata, the message payload, and acknowledgment capabilities for a Kafka message.
 * It also provides methods for formatting, committing, and acknowledging the message.
 *
 * @param TKey The type of the message key.
 * @param TValue The type of the message value.
 * @property topic The topic from which the message was consumed.
 * @property partition The partition within the topic.
 * @property topicPartition The [TopicPartition] representing the topic and partition.
 * @property offset The offset of the message within the partition.
 * @property timestamp The timestamp of the message.
 * @property timestampType The type of the timestamp (e.g., CreateTime, LogAppendTime).
 * @property serializedKeySize The size of the serialized key, in bytes.
 * @property serializedValueSize The size of the serialized value, in bytes.
 * @property headers The collection of headers associated with the message.
 * @property key The key of the message.
 * @property value The value (payload) of the message.
 * @property leaderEpoch The leader epoch of the partition, if available.
 * @property groupId The group ID of the Kafka consumer processing this message.
 * @property clientId The client ID of the Kafka consumer processing this message.
 */
class IncomingMessage<TKey, TValue> private constructor(
    val consumerRecord: ConsumerRecord<TKey, TValue>,
    private val incomingMessageStringFormatter: IncomingMessageStringFormatter,
    val groupId: String,
    val clientId: String,
    private var acknowledgment: Acknowledgment,
    val consumedAt: Instant
) {
    val topic: String = consumerRecord.topic()
    val partition = consumerRecord.partition()
    val offset = consumerRecord.offset()
    val timestamp = consumerRecord.timestamp()
    val timestampType: TimestampType = consumerRecord.timestampType()
    val serializedKeySize = consumerRecord.serializedKeySize()
    val serializedValueSize = consumerRecord.serializedValueSize()
    val headers: Collection<Header> = consumerRecord.headers().map { QuafkaHeader(it.key(), it.value()) }
    val key: TKey = consumerRecord.key()
    val value: TValue = consumerRecord.value()
    val leaderEpoch = consumerRecord.leaderEpoch().getOrNull()
    var acknowledged: Boolean = false
        private set
    val topicPartitionOffset: TopicPartitionOffset
        get() {
            return TopicPartitionOffset(this.topic, this.partition, this.offset)
        }
    val topicPartition: TopicPartition
        get() {
            return TopicPartition(this.topic, this.partition)
        }

    /**
     * Overrides the current acknowledgment with a new acknowledgment implementation.
     *
     * @param acknowledgment The new [Acknowledgment] implementation.
     */
    internal fun overrideAcknowledgment(acknowledgment: Acknowledgment) {
        this.acknowledgment = acknowledgment
    }

    internal fun markAsAcknowledged() {
        acknowledged = true
    }

    /**
     * Acknowledges the processing of this message.
     */
    fun ack() {
        this.acknowledgment.acknowledge(this)
    }

    /**
     * Commits the message offset to Kafka.
     *
     * @return A [Deferred] representing the completion of the commit operation.
     */
    fun commit(): Deferred<Unit> = this.acknowledgment.commit(this)

    /**
     * Converts the message's timestamp to an [Instant].
     *
     * @return An [Instant] representation of the timestamp.
     */
    fun getTimestampAsInstant(): Instant =
        if (isMillisecond(timestamp)) {
            Instant.ofEpochMilli(timestamp)
        } else {
            Instant.ofEpochSecond(timestamp)
        }

    private fun isMillisecond(value: Long) = (value >= 1E11) || (value <= -3E10)

    /**
     * Checks if the message is a tombstone message (i.e., has a `null` value).
     *
     * @return `true` if the message value is `null`, `false` otherwise.
     */
    fun isTombStonedMessage(): Boolean = value == null

    /**
     * Checks if the message key is `null`.
     *
     * @return `true` if the key is `null`, `false` otherwise.
     */
    fun isKeyNull(): Boolean = key == null

    /**
     * Formats the message as a string using the configured formatter.
     *
     * @param logLevel The logging level (e.g., INFO, DEBUG).
     * @param addValue If `true`, includes the message value in the formatted output.
     * @param addHeaders If `true`, includes the message headers in the formatted output.
     * @return A formatted string representation of the message.
     */
    fun toString(
        logLevel: Level = Level.INFO,
        addValue: Boolean = false,
        addHeaders: Boolean = false
    ): String = incomingMessageStringFormatter.format(
        incomingMessage = this,
        logLevel = logLevel,
        addValue = addValue,
        addHeaders = addHeaders
    )

    /**
     * Default string representation of the message.
     *
     * @return A formatted string with default logging options.
     */
    override fun toString(): String = toString(logLevel = Level.INFO, addValue = false, addHeaders = false)

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other !is IncomingMessage<*, *>) return false

        if (topic != other.topic) return false
        if (partition != other.partition) return false
        if (offset != other.offset) return false

        return true
    }

    override fun hashCode(): Int {
        var result = topic.hashCode()
        result = 31 * result + partition
        result = 31 * result + offset.hashCode()
        return result
    }

    companion object {
        private object DefaultAcknowledgment : Acknowledgment {
            private val completed = CompletableDeferred(Unit)

            override fun acknowledge(incomingMessage: IncomingMessage<*, *>) = Unit

            override fun commit(incomingMessage: IncomingMessage<*, *>) = completed
        }

        /**
         * Factory method for creating an [IncomingMessage] instance.
         *
         * @param TKey The type of the message key.
         * @param TValue The type of the message value.
         * @param consumerRecord The Kafka [ConsumerRecord] to wrap.
         * @param incomingMessageStringFormatter The formatter for generating string representations.
         * @param groupId The group ID of the Kafka consumer.
         * @param clientId The client ID of the Kafka consumer.
         * @param acknowledgment The acknowledgment strategy for the message. Defaults to a no-op acknowledgment.
         * @return A new instance of [IncomingMessage].
         */
        fun <TKey, TValue> create(
            consumerRecord: ConsumerRecord<TKey, TValue>,
            incomingMessageStringFormatter: IncomingMessageStringFormatter,
            groupId: String,
            clientId: String,
            acknowledgment: Acknowledgment = DefaultAcknowledgment,
            consumedAt: Instant = Instant.now()
        ): IncomingMessage<TKey, TValue> = IncomingMessage(
            consumerRecord,
            incomingMessageStringFormatter,
            groupId,
            clientId,
            acknowledgment,
            consumedAt
        )
    }
}
