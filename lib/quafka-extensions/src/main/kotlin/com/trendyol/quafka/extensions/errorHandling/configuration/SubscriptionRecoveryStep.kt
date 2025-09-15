package com.trendyol.quafka.extensions.errorHandling.configuration

import com.trendyol.quafka.consumer.configuration.*
import com.trendyol.quafka.consumer.messageHandlers.*
import com.trendyol.quafka.extensions.delaying.MessageDelayer
import com.trendyol.quafka.extensions.errorHandling.recoverer.*
import com.trendyol.quafka.producer.QuafkaProducer

internal class SubscriptionRecoveryStepImpl<TKey, TValue>(
    topicResolver: TopicResolver,
    danglingTopic: String,
    producer: QuafkaProducer<TKey, TValue>,
    messageDelayer: MessageDelayer,
    private val subscriptionHandlerChoiceStep: SubscriptionHandlerChoiceStep<TKey, TValue>
) : SubscriptionRecoveryConfigStep<TKey, TValue>,
    SubscriptionWithSingleMessageHandlingStep<TKey, TValue> {
    private var innerBuilder = RecoverableMessageExecutorBuilder(
        topicResolver = topicResolver,
        danglingTopic = danglingTopic,
        producer = producer,
        messageDelayer = messageDelayer
    )

    override fun withSingleMessageHandler(
        configure: SingleMessageHandlingStrategy<TKey, TValue>.() -> Unit,
        handler: SingleMessageHandler<TKey, TValue>
    ): SubscriptionOptionsStep<TKey, TValue> {
        val executor = innerBuilder.build()
        return subscriptionHandlerChoiceStep.withSingleMessageHandler(configure) { incomingMessage, consumerContext ->
            executor.execute(incomingMessage, consumerContext) {
                handler(incomingMessage, consumerContext)
            }
        }
    }

    override fun withRecoverable(
        configure: RecoverableMessageExecutorBuilder<TKey, TValue>.() -> Unit
    ): SubscriptionWithSingleMessageHandlingStep<TKey, TValue> {
        configure(innerBuilder)
        return this
    }
}
