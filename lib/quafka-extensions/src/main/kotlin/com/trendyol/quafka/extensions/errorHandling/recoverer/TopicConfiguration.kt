package com.trendyol.quafka.extensions.errorHandling.recoverer

import kotlin.time.Duration

/**
 * Encapsulates the complete configuration for a message topic, including its primary source,
 * retry strategy, and dead-letter topic.
 *
 * @property topic The primary topic from which messages are consumed.
 * @property retry How to handle failures (see subclasses of [TopicRetryStrategy])
 * @property deadLetterTopic The topic where messages are sent after all retry attempts are exhausted.
 */
data class TopicConfiguration(
    val topic: String,
    val retry: TopicRetryStrategy,
    val deadLetterTopic: String
) {
    init {
        require(topic.isNotBlank()) { "topic cannot be blank" }
        require(deadLetterTopic.isNotBlank()) { "deadLetterTopic cannot be blank" }
    }

    fun isSuitableTopic(topic: String): Boolean {
        if (this.topic == topic) {
            return true
        }
        return when (retry) {
            is TopicRetryStrategy.ExponentialBackoffToSingleTopicRetry -> retry.delayTopics.any { dt -> dt.topic == topic }
            is TopicRetryStrategy.ExponentialBackoffMultiTopicRetry -> retry.delayTopics.any { dt -> dt.topic == topic }
            is TopicRetryStrategy.SingleTopicRetry -> retry.retryTopic == topic
            TopicRetryStrategy.NoneStrategy -> false
        }
    }

    /**
     * Defines strategies for retrying failed messages.
     */
    sealed class TopicRetryStrategy {
        /**
         * Disable retries entirely – failed records are sent directly to the
         * [TopicConfiguration.deadLetterTopic].
         *
         * Useful for idempotent workflows where a single failure is considered fatal.
         */
        data object NoneStrategy : TopicRetryStrategy()

        /**
         * A simple strategy that forwards all failed messages to a single topic for reprocessing.
         *
         * @property retryTopic The single topic where all failed messages are sent for retrying.
         * @property maxTotalRetryAttempts The total number of times a message should be processed
         */
        data class SingleTopicRetry(
            val retryTopic: String,
            val maxTotalRetryAttempts: Int
        ) : TopicRetryStrategy() {
            init {
                require(retryTopic.isNotBlank()) { "retryTopic cannot be blank" }
            }
        }

        /**
         * Defines a single "level" or "bucket" in an exponential backoff strategy,
         * associating a topic with a maximum delay threshold.
         *
         * @property topic The name of the topic for this specific delay level.
         * @property maxDelay The maximum delay this level can handle. A calculated delay will be routed to the first level whose threshold is greater than or equal to the delay.
         */
        data class DelayTopicConfiguration(
            val topic: String,
            val maxDelay: Duration
        ) {
            init {
                require(topic.isNotBlank()) { "topic cannot be blank" }
            }
        }

        /**
         * An exponential backoff strategy that forwards a failed message to a temporary delay topic.
         * After the delay, the message is then forwarded to a **single, final retry topic** for consumption.
         * This simplifies the consumer, which only needs to subscribe to one topic for all retries.
         *
         * ### Example Flow:
         * 1. Message fails on `input-topic`.
         * 2. First retry is calculated to be 8s. It's sent to delay topic `delay-10s`.
         * 3. After the delay, the message is forwarded to `input-retry-topic`.
         * 4. Application consumes from `input-retry-topic` and it fails again.
         * 5. Second retry is calculated to be 15s. It's sent to delay topic `delay-30s`.
         * 6. After the delay, the message is forwarded back to `input-retry-topic`.
         *
         * @property retryTopic The single, final topic the application subscribes to for consuming all retried messages.
         * @property maxTotalRetryAttempts The total number of times a message should be processed
         * @property delayTopics A list of topic configurations that act as buckets for different delay durations.
         */
        data class ExponentialBackoffToSingleTopicRetry(
            val retryTopic: String,
            val maxTotalRetryAttempts: Int,
            val delayTopics: List<DelayTopicConfiguration>
        ) : TopicRetryStrategy() {
            init {
                require(retryTopic.isNotBlank()) { "retryTopic cannot be blank" }
            }

            private val sortedDelayedTopics = delayTopics.sortedBy { it.maxDelay }

            /**
             * Finds the appropriate delay level for a given retry delay.
             *
             * @param delay The calculated delay for the current retry attempt.
             * @return The first [maxDelay] whose `delayThreshold` is greater than or equal to the `delay`, or `null` if none match.
             */
            fun findBucket(delay: Duration): DelayTopicConfiguration? = sortedDelayedTopics
                .firstOrNull { it.maxDelay >= delay }
        }

        /**
         * An exponential backoff strategy that forwards a failed message to a specific delay topic
         * based on the retry attempt. The application must **subscribe to each of the delay topics**
         * to process retried messages.
         *
         * ### Example Flow:
         * 1. Message fails on `input-topic`.
         * 2. First retry is calculated to be 8s. It's sent to delay topic `input-retry-10s`.
         * 3. Application consumes from `input-retry-10s` and it fails again.
         * 4. Second retry is calculated to be 15s. It's sent to delay topic `input-retry-30s`.
         * 5. Application consumes from `input-retry-30s`.
         *
         * @property delayTopics A list of topic configurations that act as buckets for different delay durations.
         * @property maxTotalRetryAttempts The total number of times a message should be processed
         */
        data class ExponentialBackoffMultiTopicRetry(
            val delayTopics: List<DelayTopicConfiguration>,
            val maxTotalRetryAttempts: Int
        ) : TopicRetryStrategy() {
            private val sortedDelayedTopics = delayTopics.sortedBy { it.maxDelay }

            /**
             * Finds the appropriate delay level for a given retry delay.
             *
             * @param delay The calculated delay for the current retry attempt.
             * @return The first [maxDelay] whose `delayThreshold` is greater than or equal to the `delay`, or `null` if none match.
             */
            fun findBucket(delay: Duration): DelayTopicConfiguration? = sortedDelayedTopics
                .firstOrNull { it.maxDelay >= delay }
        }
    }
}
