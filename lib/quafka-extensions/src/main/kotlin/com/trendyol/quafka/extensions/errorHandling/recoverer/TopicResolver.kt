package com.trendyol.quafka.extensions.errorHandling.recoverer

import com.trendyol.quafka.consumer.IncomingMessage

/**
 * Resolves topic configurations for incoming messages.
 *
 * Given an incoming message, this resolver looks up the corresponding [TopicConfiguration]
 * by checking the message's topic (or original topic if it's a retry) against registered configurations.
 *
 * @property topicConfigurations The collection of topic configurations to search through.
 */
open class TopicResolver(private val topicConfigurations: Collection<TopicConfiguration<*>>) {
    /**
     * Resolves the topic configuration for the given incoming message.
     *
     * Checks the message's original topic (for retries) or current topic (for first-time processing)
     * against all registered configurations.
     *
     * @param incomingMessage The message to resolve configuration for.
     * @return [Topic.ResolvedTopic] if a matching configuration is found, [Topic.UnknownTopic] otherwise.
     */
    open suspend fun resolve(incomingMessage: IncomingMessage<*, *>): Topic {
        val topic = incomingMessage.originalTopicOrTopic()
        return topicConfigurations
            .firstOrNull { tc ->
                tc.isSuitableTopic(topic)
            }?.let {
                Topic.ResolvedTopic(it)
            } ?: Topic.UnknownTopic
    }

    /**
     * Represents the result of topic resolution.
     */
    sealed class Topic {
        /**
         * A topic with a known configuration.
         *
         * @property configuration The resolved topic configuration.
         */
        data class ResolvedTopic(val configuration: TopicConfiguration<*>) : Topic()

        /**
         * A topic with no matching configuration.
         *
         * Messages from unknown topics are typically sent to the dangling topic.
         */
        data object UnknownTopic : Topic()
    }
}
