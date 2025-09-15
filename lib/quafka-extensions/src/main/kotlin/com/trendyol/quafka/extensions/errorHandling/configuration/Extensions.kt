package com.trendyol.quafka.extensions.errorHandling.configuration

import com.trendyol.quafka.consumer.configuration.*
import com.trendyol.quafka.consumer.messageHandlers.SingleMessageHandler
import com.trendyol.quafka.extensions.delaying.DelayHeaders.clearDelay
import com.trendyol.quafka.extensions.delaying.MessageDelayer
import com.trendyol.quafka.extensions.errorHandling.recoverer.*
import com.trendyol.quafka.producer.*

/**
 * DSL-friendly shortcut that wires a **consumer subscription + recovery /
 * retry layer** in one go.
 *
 * ### What it does
 * 1. Registers hidden *“retry forwarder”* subscriptions for every
 *    [TopicConfiguration.retry] topic so that previously failed messages can
 *    be re-processed.
 * 2. Subscribes to the **main** topics returned by
 *    [Collection.getAllSubscriptionTopics] (usually the business topics
 *    declared in your config).
 * 3. Builds a [SubscriptionRecoveryStep] for each topic-partition pair so you can
 *    configure retry and finally pick **exactly one** message-handling
 *    strategy (`single { … }` or `batch { … }`).
 *
 * The call is meant to replace the verbose boiler-plate you would otherwise
 * write when combining:
 *
 * * `subscribeToRetryForwarderTopics(...)`
 * * `.subscribe(...)` with a manual `RecoveryStep` wrapper
 *
 * ---
 *
 * @receiver The **`QuafkaConsumerBuilder`** you are currently configuring.
 *
 * @param topics         Business topics together with their retry settings.
 * @param danglingTopic  Topic to which *exhausted* or *non-retryable* messages
 *                       will be forwarded.
 * @param producer       A producer used internally to publish retry / dangling
 *                       messages.
 * @param messageDelayer Helper that adds a delay header when messages must be
 *                       retried at a later time.  A new instance is provided
 *                       by default.
 * @param retryBuilder   DSL block executed on a [SubscriptionRecoveryConfigStep].  Inside
 *                       this block you typically:
 *
 *                       1. Call **`recovery { … }`** to tune retry policy
 *                          (max attempts, back-off, etc.) – *optional*.
 *                       2. Choose withSingleMessageHandler handler via ` – *required*.
 *                       3. Optionally set subscription-level options
 *                          (`autoAck`, `backpressure`, `onError`).
 *
 * @return The next builder stage – [QuafkaConsumerBuilder.SubscriptionStep] –
 *         allowing you to continue fluent configuration (e.g., commit policy,
 *         metrics, etc.).
 *
 * ---
 *
 * ### Example
 * ```kotlin
 * .subscribeWithErrorHandling(topics, danglingTopic, quafkaProducer) {
 *     this
 *         .withRecoverable {
 *             withExceptionDetailsProvider { throwable: Throwable -> ExceptionDetails(throwable, throwable.message!!) }
 *                 .withMessageDelayer(MessageDelayer())
 *                 .withPolicyProvider({ message, consumerContext, exceptionReport ->
 *                     when (exceptionReport.exception) {
 *                         is DatabaseConcurrencyException -> RetryPolicy.FullRetry(
 *                             identifier = "DatabaseConcurrencyException",
 *                             inMemoryConfig = InMemoryRetryConfig.basic(maxAttempts = 3, initialDelay = 100.milliseconds),
 *                             nonBlockingConfig = NonBlockingRetryConfig(
 *                                 maxAttempts = 10,
 *                                 initialDelay = 1.seconds,
 *                                 maxDelay = 50.seconds,
 *                                 isExponential = true
 *                             )
 *                         )
 *                         is DomainException -> RetryPolicy.NoRetry
 *                         else -> RetryPolicy.InMemoryOnly(
 *                             identifier = "DatabaseConcurrencyException",
 *                             config = InMemoryRetryConfig.basic(maxAttempts = 3, initialDelay = 100.milliseconds)
 *                         )
 *                     }
 *                 })
 *                 .withOutgoingMessageModifier({ message, consumerContext, exceptionReport ->
 *                     val newHeaders = this.headers.toMutableList()
 *                     newHeaders.add(header("X-Handled", true))
 *                     this.copy(headers = newHeaders)
 *                 })
 *         }
 *         .withSingleMessageHandler { incomingMessage, consumerContext ->
 *             when {
 *                 topic1.isSuitableTopic(incomingMessage.topic) -> {
 *                     // process topic.v1 and topic.v1.retry
 *                 }
 *                 topic2.isSuitableTopic(incomingMessage.topic) -> {
 *                     // process topic.v2 and topic.v2.retry.10sec, topic.v2.retry.30sec, topic.v2.retry.1min, topic.v2.retry.5min
 *                 }
 *                 topic3.isSuitableTopic(incomingMessage.topic) -> {
 *                     // process topic.v3
 *                 }
 *                 topic4.isSuitableTopic(incomingMessage.topic) -> {
 *                     // process topic.v4 and topic.v4.retry
 *                 }
 *             }
 *         }
 *         .withBackpressure(20)
 *         .autoAckAfterProcess(true)
 * }
 * .build()
 * ```
 */
fun <TKey, TValue> SubscriptionStep<TKey, TValue>.subscribeWithErrorHandling(
    topics: Collection<TopicConfiguration>,
    danglingTopic: String,
    producer: QuafkaProducer<TKey, TValue>,
    messageDelayer: MessageDelayer = MessageDelayer(),
    retryBuilder: SubscriptionRecoveryConfigStep<TKey, TValue>.() -> SubscriptionOptionsStep<TKey, TValue>
): SubscriptionBuildStep<TKey, TValue> = this
    .subscribe(*topics.getAllSubscriptionTopics().toTypedArray()) { subscriptionParameters ->
        val sub = SubscriptionRecoveryStepImpl(
            topicResolver = TopicResolver(topics),
            danglingTopic = danglingTopic,
            producer = producer,
            messageDelayer = messageDelayer,
            subscriptionHandlerChoiceStep = this
        )

        retryBuilder.invoke(sub)
    }.subscribeToDelayedTopics(
        topicConfigurations = topics,
        producer = producer,
        messageDelayer = messageDelayer,
        danglingTopic = danglingTopic
    )

fun <TKey, TValue> SubscriptionHandlerChoiceStep<TKey, TValue>.withRecoverableSingleMessageHandler(
    topics: Collection<TopicConfiguration>,
    danglingTopic: String,
    producer: QuafkaProducer<TKey, TValue>,
    messageDelayer: MessageDelayer = MessageDelayer(),
    handler: SingleMessageHandler<TKey, TValue>
): SubscriptionOptionsStep<TKey, TValue> {
    val innerBuilder = RecoverableMessageExecutorBuilder(
        topicResolver = TopicResolver(topics),
        danglingTopic = danglingTopic,
        producer = producer,
        messageDelayer = messageDelayer
    )
    val executor = innerBuilder.build()
    return this.withSingleMessageHandler { incomingMessage, consumerContext ->
        executor.execute(incomingMessage, consumerContext) {
            handler(incomingMessage, consumerContext)
        }
    }
}

fun <TKey, TValue> SubscriptionStep<TKey, TValue>.subscribeToDelayedTopics(
    topicConfigurations: Collection<TopicConfiguration>,
    producer: QuafkaProducer<TKey, TValue>,
    messageDelayer: MessageDelayer,
    danglingTopic: String
): SubscriptionBuildStep<TKey, TValue> {
    val forwarderTopics = topicConfigurations
        .flatMap { tc ->
            if (tc.retry is TopicConfiguration.TopicRetryStrategy.ExponentialBackoffToSingleTopicRetry) {
                tc.retry.delayTopics.map { it.topic }
            } else {
                emptyList()
            }
        }.distinct()
    if (forwarderTopics.isEmpty()) {
        return this.withoutSubscriptions()
    }
    return this.subscribe(*forwarderTopics.toTypedArray()) {
        withSingleMessageHandler { incomingMessage, consumerContext ->
            messageDelayer.delayIfNeeded(incomingMessage, consumerContext)
            val forwardingTopic = incomingMessage.headers.getForwardingTopic() ?: danglingTopic
            producer.send(
                OutgoingMessage.create(
                    topic = forwardingTopic,
                    partition = null,
                    key = incomingMessage.key,
                    headers = incomingMessage.headers.toMutableList().clearDelay(),
                    value = incomingMessage.value
                )
            )
        }.autoAckAfterProcess(true)
    }
}

fun Collection<TopicConfiguration>.getAllSubscriptionTopics(): Collection<String> = this
    .map {
        listOf(it.topic) + when (it.retry) {
            is TopicConfiguration.TopicRetryStrategy.ExponentialBackoffMultiTopicRetry -> it.retry.delayTopics.map { it.topic }
            is TopicConfiguration.TopicRetryStrategy.ExponentialBackoffToSingleTopicRetry -> listOf(it.retry.retryTopic)
            TopicConfiguration.TopicRetryStrategy.NoneStrategy -> emptyList()
            is TopicConfiguration.TopicRetryStrategy.SingleTopicRetry -> listOf(it.retry.retryTopic)
        }
    }.flatten()
    .toSet()
