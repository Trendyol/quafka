package com.trendyol.quafka.extensions.errorHandling.configuration

import com.trendyol.quafka.consumer.configuration.SubscriptionWithSingleMessageHandlingStep
import com.trendyol.quafka.extensions.errorHandling.recoverer.RecoverableMessageExecutorBuilder

/**
 * Configuration step for setting up error recovery in a subscription.
 *
 * This interface is part of the fluent configuration DSL for [subscribeWithErrorHandling].
 *
 * @param TKey The type of the message key.
 * @param TValue The type of the message value.
 */
interface SubscriptionRecoveryConfigStep<TKey, TValue> {
    /**
     * Configures the recoverable message executor with custom settings.
     *
     * Use this to customize retry policies, exception handlers, message delayers, etc.
     * After calling this, you must configure a message handler via [SubscriptionWithSingleMessageHandlingStep.withSingleMessageHandler].
     *
     * @param configure Lambda to configure the [RecoverableMessageExecutorBuilder].
     * @return A step for configuring the message handler.
     */
    fun withRecoverable(
        configure: RecoverableMessageExecutorBuilder<TKey, TValue>.() -> Unit
    ): SubscriptionWithSingleMessageHandlingStep<TKey, TValue>
}
