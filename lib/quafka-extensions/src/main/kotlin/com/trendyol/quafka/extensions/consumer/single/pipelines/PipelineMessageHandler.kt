package com.trendyol.quafka.extensions.consumer.single.pipelines

import com.trendyol.quafka.consumer.*
import com.trendyol.quafka.consumer.configuration.*
import com.trendyol.quafka.consumer.messageHandlers.*
import com.trendyol.quafka.extensions.common.pipelines.*

/**
 * A single message handler that processes messages through a middleware pipeline.
 *
 * This handler wraps each incoming message in a [SingleMessageEnvelope] and passes
 * it through a configured pipeline of middleware. This allows for composable,
 * reusable processing logic organized as middleware.
 *
 * **Features:**
 * - Middleware-based message processing
 * - Composable processing steps
 * - Shared attributes across middleware
 * - Support for complex workflows
 *
 * @param TKey The type of the message key.
 * @param TValue The type of the message value.
 * @property pipeline The middleware pipeline to execute for each message.
 *
 * @see SingleMessageEnvelope
 * @see Pipeline
 * @see usePipelineMessageHandler
 *
 * @sample
 * ```kotlin
 * consumer.subscribe("orders") {
 *     usePipelineMessageHandler {
 *         use { envelope, next ->
 *             logger.info("Processing message: ${envelope.message.key}")
 *             next()
 *         }
 *
 *         use { envelope, next ->
 *             val order = parseOrder(envelope.message.value)
 *             processOrder(order)
 *             envelope.message.ack()
 *             next()
 *         }
 *     }
 * }
 * ```
 */
class PipelineMessageHandler<TKey, TValue>(val pipeline: Pipeline<SingleMessageEnvelope<TKey, TValue>>) :
    SingleMessageHandler<TKey, TValue> {
    /**
     * Invokes the handler to process a single message.
     *
     * Creates a [SingleMessageEnvelope] with the incoming message and consumer context,
     * then executes it through the configured pipeline.
     *
     * @param incomingMessage The message to process.
     * @param consumerContext The consumer context providing acknowledgment capabilities.
     */
    override suspend fun invoke(
        incomingMessage: IncomingMessage<TKey, TValue>,
        consumerContext: ConsumerContext
    ) {
        pipeline.execute(
            SingleMessageEnvelope(
                incomingMessage,
                consumerContext
            )
        )
    }

    companion object {
        /**
         * Extension function for configuring a pipeline message handler in a subscription.
         *
         * This is the recommended way to use [PipelineMessageHandler] when building
         * a consumer subscription.
         *
         * @param configure Lambda with receiver to configure the pipeline using [PipelineBuilder].
         * @return The next step in the subscription configuration.
         *
         * @sample
         * ```kotlin
         * consumer.subscribe("events") {
         *     usePipelineMessageHandler {
         *         use(LoggingMiddleware())
         *         use(DeserializationMiddleware(serde))
         *         use { envelope, next ->
         *             val event = envelope.getDeserializedValue<Event>()
         *             processEvent(event)
         *             envelope.message.ack()
         *             next()
         *         }
         *     }
         * }
         * ```
         */
        fun <TKey, TValue> SubscriptionHandlerChoiceStep<TKey, TValue>.usePipelineMessageHandler(
            configure: PipelineBuilder<SingleMessageEnvelope<TKey, TValue>>.() -> Unit
        ): SubscriptionOptionsStep<TKey, TValue> {
            val handler = createPipelineMessageHandler(configure)
            return this.withSingleMessageHandler(handler = handler)
        }

        /**
         * Creates a [PipelineMessageHandler] with the specified pipeline configuration.
         *
         * Use this function when you need to create the handler independently of a subscription
         * configuration, for example when the handler is reused across multiple subscriptions.
         *
         * @param configure Lambda with receiver to configure the pipeline using [PipelineBuilder].
         * @return A configured [PipelineMessageHandler] instance.
         *
         * @sample
         * ```kotlin
         * val handler = createPipelineMessageHandler<String, ByteArray> {
         *     use { envelope, next ->
         *         logger.info("Message key: ${envelope.message.key}")
         *         next()
         *     }
         *     use { envelope, next ->
         *         processMessage(envelope.message)
         *         envelope.message.ack()
         *         next()
         *     }
         * }
         *
         * consumer.subscribe("topic1") {
         *     withSingleMessageHandler(handler)
         * }
         *
         * consumer.subscribe("topic2") {
         *     withSingleMessageHandler(handler)
         * }
         * ```
         */
        fun <TKey, TValue> createPipelineMessageHandler(
            configure: PipelineBuilder<SingleMessageEnvelope<TKey, TValue>>.() -> Unit
        ): PipelineMessageHandler<TKey, TValue> {
            val pipeline = PipelineBuilder<SingleMessageEnvelope<TKey, TValue>>()
            pipeline.apply(configure)
            return PipelineMessageHandler(pipeline.build())
        }
    }
}
