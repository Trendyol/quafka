package com.trendyol.quafka

import com.trendyol.quafka.Extensions.toRecordHeaders
import com.trendyol.quafka.common.*
import com.trendyol.quafka.consumer.*
import io.mockk.*
import kotlinx.coroutines.*
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.header.Header
import org.apache.kafka.common.record.TimestampType
import java.util.Optional
import kotlin.coroutines.*

data class IncomingMessageBuilder<TKey, TValue>(
    val topic: String,
    val partition: Int,
    val offset: Long,
    val key: TKey,
    val value: TValue,
    val headers: MutableList<Header> = mutableListOf(),
    val timestamp: Long = 0,
    val groupId: String = "",
    val clientId: String = "",
    val ackCallback: (incomingMessage: IncomingMessage<*, *>) -> Unit = {},
    val commitCallback: (incomingMessage: IncomingMessage<*, *>) -> Unit = {}
) {
    fun build(): IncomingMessage<TKey, TValue> {
        val consumerRecord =
            ConsumerRecord(
                topic,
                partition,
                offset,
                timestamp,
                TimestampType.LOG_APPEND_TIME,
                0,
                0,
                key,
                value,
                headers.toRecordHeaders(),
                Optional.empty()
            )
        return IncomingMessage.create(
            consumerRecord,
            IncomingMessageStringFormatter(),
            groupId,
            clientId,
            NoopAcknowledgment(consumerRecord, ackCallback, commitCallback)
        )
    }

    fun withHeader(header: Header): IncomingMessageBuilder<TKey, TValue> {
        val headers = this.headers.toMutableList()
        headers.add(header)
        return this.copy(headers = headers)
    }

    class NoopAcknowledgment<TKey, TValue>(
        val consumerRecord: ConsumerRecord<TKey, TValue>,
        private val ackCallback: (incomingMessage: IncomingMessage<*, *>) -> Unit,
        private val commitCallback: (incomingMessage: IncomingMessage<*, *>) -> Unit
    ) : Acknowledgment {
        fun topicPartition(): TopicPartition = TopicPartition(consumerRecord.topic(), consumerRecord.partition())

        fun offset(): Long = consumerRecord.offset()

        override fun acknowledge(incomingMessage: IncomingMessage<*, *>) {
            ackCallback(incomingMessage)
        }

        override fun commit(incomingMessage: IncomingMessage<*, *>): Deferred<Unit> {
            commitCallback(incomingMessage)
            return CompletableDeferred(Unit)
        }
    }
}

fun IncomingMessage<*, *>.buildConsumerContext(
    timeProvider: TimeProvider = FakeTimeProvider.default()
): ConsumerContext = ConsumerContext(
    topicPartition = this.topicPartition,
    consumerOptions = mockk(relaxed = true) {
        val opt = this
        every { opt.getGroupId() } returns this@buildConsumerContext.groupId
        every { opt.getClientId() } returns this@buildConsumerContext.clientId
        every { opt.timeProvider } returns timeProvider
    },
    coroutineContext = EmptyCoroutineContext
)
