package com.trendyol.quafka.consumer

import com.trendyol.quafka.consumer.configuration.QuafkaConsumerOptions
import com.trendyol.quafka.events.QuafkaEvent
import org.apache.kafka.clients.consumer.OffsetAndMetadata

object Events {
    data class Subscribed(
        val topics: Collection<String>,
        val detail: ConsumerDetail
    ) : QuafkaEvent

    data class MessagesReceived(
        val messages: Collection<IncomingMessage<*, *>>,
        val detail: ConsumerDetail
    ) : QuafkaEvent

    data class PollingExceptionOccurred(
        val exception: Throwable,
        val detail: ConsumerDetail
    ) : QuafkaEvent

    data class ConsumerDetail(
        val groupId: String,
        val clientId: String,
        val properties: Map<String, Any>
    )

    data class PartitionsRevoked(
        val revokedPartitions: MutableCollection<TopicPartition>,
        val detail: ConsumerDetail
    ) : QuafkaEvent

    data class PartitionsAssigned(
        val assignedPartitions: List<TopicPartitionOffset>,
        val detail: ConsumerDetail
    ) : QuafkaEvent

    data class ConsumerStopped(
        val detail: ConsumerDetail
    ) : QuafkaEvent

    data class OffsetsCommitted(
        val offsets: Map<TopicPartition, OffsetAndMetadata>,
        val sync: Boolean,
        val detail: ConsumerDetail
    ) : QuafkaEvent

    data class WorkerStopped(
        val topicPartition: TopicPartition,
        val detail: ConsumerDetail
    ) : QuafkaEvent

    data class BackpressureActivated(
        val topicPartition: TopicPartition,
        val detail: ConsumerDetail
    ) : QuafkaEvent

    data class BackpressureReleased(
        val topicPartition: TopicPartition,
        val detail: ConsumerDetail
    ) : QuafkaEvent

    data class RebalanceCompleted(
        val assignedTopicPartitions: Collection<TopicPartitionOffset>,
        val detail: ConsumerDetail
    ) : QuafkaEvent

    data class TopicPartitionPaused(
        val topicPartition: TopicPartition,
        val detail: ConsumerDetail
    ) : QuafkaEvent

    data class TopicPartitionResumed(
        val topicPartition: TopicPartition,
        val offset: Long,
        val detail: ConsumerDetail
    ) : QuafkaEvent

    data class WorkerFailed(
        val topicPartition: TopicPartition,
        val exception: Throwable,
        val detail: ConsumerDetail
    ) : QuafkaEvent

    fun QuafkaConsumerOptions<*, *>.toDetail() = ConsumerDetail(
        groupId = this.getGroupId(),
        clientId = this.getClientId(),
        this.properties
    )
}
