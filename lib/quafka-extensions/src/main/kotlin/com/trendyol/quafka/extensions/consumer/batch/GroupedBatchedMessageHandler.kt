package com.trendyol.quafka.extensions.consumer.batch

import com.trendyol.quafka.consumer.*
import com.trendyol.quafka.consumer.configuration.*
import com.trendyol.quafka.consumer.messageHandlers.*
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.flow.*

typealias GroupingDelegate<TKey, TValue> = (incomingMessage: IncomingMessage<TKey, TValue>) -> String

/**
 * Handles messages grouped by a specified key, where messages within each group are processed sequentially,
 * and groups are processed concurrently based on the specified concurrency level.
 *
 * @param TKey the type of the message key
 * @param TValue the type of the message value
 */
class GroupedBatchedMessageHandler<TKey, TValue>(
    private val options: GroupedBatchedMessageHandlerOptions<TKey, TValue>
) : BatchMessageHandler<TKey, TValue> {
    /**
     * Processes a batch of incoming messages by grouping them using the provided grouping function.
     * Messages within each group are processed sequentially, while groups are processed concurrently.
     *
     * @param incomingMessages a collection of messages to process
     * @param consumerContext the context used for message consumption
     */
    @OptIn(ExperimentalCoroutinesApi::class)
    override suspend fun invoke(
        incomingMessages: Collection<IncomingMessage<TKey, TValue>>,
        consumerContext: ConsumerContext
    ) {
        incomingMessages
            .groupBy { message ->
                options.groupingFn(message)
            }.iterator()
            .asFlow()
            .flatMapMerge(concurrency = options.concurrency) { group ->
                flow {
                    group.value
                        .sortedBy { it.offset }
                        .forEach { message ->
                            emit(options.singleMessageHandler(message, consumerContext))
                        }
                }
            }.collect()
    }

    companion object {
        /**
         * Configures a grouped message handler where messages within each group are processed sequentially,
         * and groups are processed concurrently based on the specified concurrency level.
         *
         * @param options configuration options for the grouped message handler
         */
        fun <TKey, TValue> SubscriptionHandlerChoiceStep<TKey, TValue>.useGroupedMessageHandler(
            options: GroupedBatchedMessageHandlerOptions<TKey, TValue>
        ): SubscriptionOptionsStep<TKey, TValue> {
            val handler = GroupedBatchedMessageHandler(options)
            return this.withBatchMessageHandler(handler = handler)
        }

        /**
         * Configures a topic-partition-based grouped message handler, where messages in each topic partition
         * are processed sequentially, and topic partitions are processed concurrently based on the concurrency level.
         *
         * @param handler the handler for processing individual messages
         * @param concurrency the maximum number of topic partitions to process concurrently
         */
        fun <TKey, TValue> SubscriptionHandlerChoiceStep<TKey, TValue>.useTopicPartitionBasedGroupedMessageHandler(
            handler: SingleMessageHandler<TKey, TValue>,
            concurrency: Int = 1
        ): SubscriptionOptionsStep<TKey, TValue> = useGroupedMessageHandler(
            GroupedBatchedMessageHandlerOptions(
                handler,
                concurrency
            ) { messageContext -> messageContext.topicPartition.toString() }
        )

        /**
         * Configures a grouped message handler with a custom grouping function.
         * Messages within each group are processed sequentially, while groups are processed concurrently.
         *
         * @param handler the handler for processing individual messages
         * @param groupingFn a function to determine the group key for each message
         * @param concurrency the maximum number of groups to process concurrently
         */
        fun <TKey, TValue> SubscriptionHandlerChoiceStep<TKey, TValue>.useGroupedMessageHandler(
            handler: SingleMessageHandler<TKey, TValue>,
            concurrency: Int = 1,
            groupingFn: GroupingDelegate<TKey, TValue>
        ): SubscriptionOptionsStep<TKey, TValue> = useGroupedMessageHandler(
            GroupedBatchedMessageHandlerOptions(
                handler,
                concurrency,
                groupingFn
            )
        )
    }

    /**
     * Configuration options for the grouped batched message handler.
     *
     * @param TKey the type of the message key
     * @param TValue the type of the message value
     * @param singleMessageHandler the handler for processing individual messages
     * @param concurrency the maximum number of groups to process concurrently
     * @param groupingFn a function to determine the group key for each message
     */
    data class GroupedBatchedMessageHandlerOptions<TKey, TValue>(
        var singleMessageHandler: SingleMessageHandler<TKey, TValue>,
        var concurrency: Int = 1,
        var groupingFn: GroupingDelegate<TKey, TValue> = { it.offset.toString() }
    )
}
