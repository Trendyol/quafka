package com.trendyol.quafka.extensions.consumer.batch.pipelines

import com.trendyol.quafka.consumer.*
import com.trendyol.quafka.consumer.configuration.*
import com.trendyol.quafka.consumer.messageHandlers.*
import com.trendyol.quafka.extensions.common.pipelines.*

/**
 * A batch message handler that processes messages through a middleware pipeline.
 *
 * This handler wraps incoming message batches in a [BatchMessageEnvelope] and passes
 * them through a configured pipeline of middleware. This allows for composable,
 * reusable processing logic organized as middleware.
 *
 * **Features:**
 * - Middleware-based batch processing
 * - Composable processing steps
 * - Shared attributes across middleware
 * - Support for complex workflows
 *
 * @param TKey The type of the message key.
 * @param TValue The type of the message value.
 * @property pipeline The middleware pipeline to execute for each batch.
 *
 * @see BatchMessageEnvelope
 * @see Pipeline
 * @see usePipelineBatchMessageHandler
 *
 * @sample
 * ```kotlin
 * consumer.subscribe("orders") {
 *     usePipelineBatchMessageHandler {
 *         use { envelope, next ->
 *             logger.info("Processing batch of ${envelope.messages.size} messages")
 *             next()
 *         }
 *
 *         use { envelope, next ->
 *             envelope.messages.forEach { message ->
 *                 processOrder(message.value)
 *             }
 *             envelope.ackAll()
 *             next()
 *         }
 *     }
 * }
 * ```
 */
class PipelineBatchMessageHandler<TKey, TValue>(val pipeline: Pipeline<BatchMessageEnvelope<TKey, TValue>>) :
    BatchMessageHandler<TKey, TValue> {
    /**
     * Invokes the handler to process a batch of messages.
     *
     * Creates a [BatchMessageEnvelope] with the incoming messages and consumer context,
     * then executes it through the configured pipeline.
     *
     * @param incomingMessages The batch of messages to process.
     * @param consumerContext The consumer context providing acknowledgment capabilities.
     */
    override suspend fun invoke(
        incomingMessages: Collection<IncomingMessage<TKey, TValue>>,
        consumerContext: ConsumerContext
    ) {
        pipeline.execute(
            BatchMessageEnvelope(
                incomingMessages,
                consumerContext
            )
        )
    }

    companion object {
        /**
         * Extension function for configuring a pipeline batch message handler in a subscription.
         *
         * This is the recommended way to use [PipelineBatchMessageHandler] when building
         * a consumer subscription.
         *
         * @param configure Lambda with receiver to configure the pipeline using [PipelineBuilder].
         * @return The next step in the subscription configuration.
         *
         * @sample
         * ```kotlin
         * consumer.subscribe("events") {
         *     usePipelineBatchMessageHandler {
         *         use(LoggingMiddleware())
         *         use(MetricsMiddleware())
         *         use { envelope, next ->
         *             processBatch(envelope.messages)
         *             envelope.ackAll()
         *             next()
         *         }
         *     }
         * }
         * ```
         */
        fun <TKey, TValue> SubscriptionHandlerChoiceStep<TKey, TValue>.usePipelineBatchMessageHandler(
            configure: PipelineBuilder<BatchMessageEnvelope<TKey, TValue>>.() -> Unit
        ): SubscriptionOptionsStep<TKey, TValue> {
            val handler = createPipelineBatchMessageHandler(configure)
            return this.withBatchMessageHandler(handler = handler)
        }

        /**
         * Creates a [PipelineBatchMessageHandler] with the specified pipeline configuration.
         *
         * Use this function when you need to create the handler independently of a subscription
         * configuration, for example when the handler is reused across multiple subscriptions.
         *
         * @param configure Lambda with receiver to configure the pipeline using [PipelineBuilder].
         * @return A configured [PipelineBatchMessageHandler] instance.
         *
         * @sample
         * ```kotlin
         * val handler = createPipelineBatchMessageHandler<String, ByteArray> {
         *     use { envelope, next ->
         *         logger.info("Batch size: ${envelope.messages.size}")
         *         next()
         *     }
         *     use { envelope, next ->
         *         processBatch(envelope.messages)
         *         envelope.ackAll()
         *         next()
         *     }
         * }
         *
         * consumer.subscribe("topic1") {
         *     withBatchMessageHandler(handler)
         * }
         *
         * consumer.subscribe("topic2") {
         *     withBatchMessageHandler(handler)
         * }
         * ```
         */
        fun <TKey, TValue> createPipelineBatchMessageHandler(
            configure: PipelineBuilder<BatchMessageEnvelope<TKey, TValue>>.() -> Unit
        ): PipelineBatchMessageHandler<TKey, TValue> {
            val pipeline = PipelineBuilder<BatchMessageEnvelope<TKey, TValue>>()
            pipeline.apply(configure)
            return PipelineBatchMessageHandler(pipeline.build())
        }
    }
}
