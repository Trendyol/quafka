package com.trendyol.quafka.extensions.errorHandling.configuration

import com.trendyol.quafka.consumer.configuration.SubscriptionWithSingleMessageHandlingStep
import com.trendyol.quafka.extensions.errorHandling.recoverer.RecoverableMessageExecutorBuilder

interface SubscriptionRecoveryConfigStep<TKey, TValue> {
    fun withRecoverable(
        configure: RecoverableMessageExecutorBuilder<TKey, TValue>.() -> Unit
    ): SubscriptionWithSingleMessageHandlingStep<TKey, TValue>
}
