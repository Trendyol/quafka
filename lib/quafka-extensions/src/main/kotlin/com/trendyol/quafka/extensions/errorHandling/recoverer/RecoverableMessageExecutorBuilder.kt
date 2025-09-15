package com.trendyol.quafka.extensions.errorHandling.recoverer

import com.trendyol.quafka.common.*
import com.trendyol.quafka.consumer.*
import com.trendyol.quafka.extensions.delaying.MessageDelayer
import com.trendyol.quafka.producer.QuafkaProducer
import kotlin.time.Duration

class RecoverableMessageExecutorBuilder<TKey, TValue>(
    private val topicResolver: TopicResolver,
    private val danglingTopic: String,
    private val producer: QuafkaProducer<TKey, TValue>,
    private var messageDelayer: MessageDelayer
) {
    private var outgoingMessageModifierDelegate: OutgoingMessageModifierDelegate<TKey, TValue> = { _, _, _ -> this }
    private var retryPolicyProvider: RetryPolicyProvider = { _, _, _ -> DefaultPolicy }
    private var exceptionDetailsProvider: ExceptionDetailsProvider =
        { throwable: Throwable -> ExceptionDetails(throwable, throwable.message ?: throwable.javaClass.simpleName) }

    fun withExceptionDetailsProvider(exceptionDetailsProvider: ExceptionDetailsProvider) = apply {
        this.exceptionDetailsProvider = exceptionDetailsProvider
    }

    fun withMessageDelayer(messageDelayer: MessageDelayer) = apply {
        this.messageDelayer = messageDelayer
    }

    fun withoutDelay() = apply {
        this.messageDelayer = NoMessageDelay
    }

    fun withPolicyProvider(policyProvider: RetryPolicyProvider) = apply {
        this.retryPolicyProvider = policyProvider
    }

    fun withPolicies(vararg conditionalPolicies: ConditionalRetryPolicy, defaultPolicy: RetryPolicy = DefaultPolicy) = apply {
        this.retryPolicyProvider = ConditionalRetryPolicyRegistry(defaultPolicy)
            .apply {
                conditionalPolicies.forEach { this.addPolicy(it) }
            }
    }

    fun withOutgoingMessageModifier(delegate: OutgoingMessageModifierDelegate<TKey, TValue>) = apply {
        this.outgoingMessageModifierDelegate = delegate
    }

    object NoMessageDelay : MessageDelayer() {
        override suspend fun delay(duration: Duration) {
            return
        }

        override suspend fun delayIfNeeded(
            message: IncomingMessage<*, *>,
            consumerContext: ConsumerContext,
            defaultDelayOptions: DelayOptions
        ) {
            return
        }
    }

    fun build(): RecoverableMessageExecutor<TKey, TValue> {
        val failedMessageRouter = FailedMessageRouter(
            exceptionDetailsProvider = exceptionDetailsProvider,
            topicResolver = topicResolver,
            danglingTopic = danglingTopic,
            quafkaProducer = producer,
            policyProvider = retryPolicyProvider,
            outgoingMessageModifier = outgoingMessageModifierDelegate
        )

        val recoverableMessageExecutor = RecoverableMessageExecutor(
            policyProvider = retryPolicyProvider,
            exceptionDetailsProvider = exceptionDetailsProvider,
            failedMessageRouter = failedMessageRouter,
            messageDelayer = messageDelayer
        )
        return recoverableMessageExecutor
    }
}
