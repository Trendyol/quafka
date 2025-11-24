package com.trendyol.quafka.extensions.errorHandling.configuration

import com.trendyol.quafka.consumer.configuration.*
import com.trendyol.quafka.consumer.messageHandlers.*
import com.trendyol.quafka.extensions.delaying.MessageDelayer
import com.trendyol.quafka.extensions.errorHandling.recoverer.*
import com.trendyol.quafka.producer.QuafkaProducer

/**
 * Internal implementation of the subscription recovery configuration steps.
 *
 * This class coordinates the fluent configuration API for error recovery,
 * bridging between the builder pattern and the final handler registration.
 *
 * @param TKey The type of the message key.
 * @param TValue The type of the message value.
 * @property topicResolver Resolves topic configurations for incoming messages.
 * @property danglingTopic Fallback topic for unconfigured topics.
 * @property producer Kafka producer for retry/error messages.
 * @property messageDelayer Handles delayed message processing.
 * @property subscriptionHandlerChoiceStep The underlying subscription step to delegate to.
 */
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

    /**
     * Registers a single message handler wrapped with error recovery.
     *
     * @param configure Lambda to configure single message handling strategy.
     * @param handler The message handler to execute (will be wrapped with recovery logic).
     * @return A step for configuring subscription options.
     */
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

    /**
     * Configures the recoverable message executor.
     *
     * @param configure Lambda to configure the [RecoverableMessageExecutorBuilder].
     * @return This step for further configuration.
     */
    override fun withRecoverable(
        configure: RecoverableMessageExecutorBuilder<TKey, TValue>.() -> Unit
    ): SubscriptionWithSingleMessageHandlingStep<TKey, TValue> {
        configure(innerBuilder)
        return this
    }
}
