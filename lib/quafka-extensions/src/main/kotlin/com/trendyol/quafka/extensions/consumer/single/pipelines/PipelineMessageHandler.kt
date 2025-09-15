package com.trendyol.quafka.extensions.consumer.single.pipelines

import com.trendyol.quafka.consumer.*
import com.trendyol.quafka.consumer.configuration.*
import com.trendyol.quafka.consumer.messageHandlers.*
import com.trendyol.quafka.extensions.common.pipelines.*

class PipelineMessageHandler<TKey, TValue>(
    val pipeline: Pipeline<SingleMessageEnvelope<TKey, TValue>>
) : SingleMessageHandler<TKey, TValue> {
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
        fun <TKey, TValue> SubscriptionHandlerChoiceStep<TKey, TValue>.usePipelineMessageHandler(
            configure: PipelineBuilder<SingleMessageEnvelope<TKey, TValue>>.() -> Unit
        ): SubscriptionOptionsStep<TKey, TValue> {
            val handler = createPipelineMessageHandler(configure)
            return this.withSingleMessageHandler(handler = handler)
        }

        fun <TKey, TValue> createPipelineMessageHandler(
            configure: PipelineBuilder<SingleMessageEnvelope<TKey, TValue>>.() -> Unit
        ): PipelineMessageHandler<TKey, TValue> {
            val pipeline = PipelineBuilder<SingleMessageEnvelope<TKey, TValue>>()
            pipeline.apply(configure)
            return PipelineMessageHandler(pipeline.build())
        }
    }
}
