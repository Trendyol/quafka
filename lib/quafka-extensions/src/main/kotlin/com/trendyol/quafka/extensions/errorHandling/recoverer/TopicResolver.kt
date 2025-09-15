package com.trendyol.quafka.extensions.errorHandling.recoverer

import com.trendyol.quafka.consumer.IncomingMessage

open class TopicResolver(
    private val topicConfigurations: Collection<TopicConfiguration>
) {
    open suspend fun resolve(incomingMessage: IncomingMessage<*, *>): Topic {
        val topic = incomingMessage.originalTopicOrTopic()
        return topicConfigurations
            .firstOrNull { tc ->
                tc.isSuitableTopic(topic)
            }?.let {
                Topic.ResolvedTopic(it)
            } ?: Topic.UnknownTopic
    }

    sealed class Topic {
        data class ResolvedTopic(
            val configuration: TopicConfiguration
        ) : Topic()

        data object UnknownTopic : Topic()
    }
}
