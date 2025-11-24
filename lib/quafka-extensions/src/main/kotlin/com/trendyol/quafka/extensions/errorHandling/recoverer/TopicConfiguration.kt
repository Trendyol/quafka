package com.trendyol.quafka.extensions.errorHandling.recoverer

import com.trendyol.quafka.consumer.*
import kotlin.time.Duration

/**
 * Encapsulates the complete configuration for a message topic, including its primary source,
 * retry strategy, and dead-letter topic.
 *
 * @property topic The primary topic from which messages are consumed.
 * @property retry How to handle failures (see subclasses of [TopicRetryStrategy])
 * @property deadLetterTopic The topic where messages are sent after all retry attempts are exhausted.
 * @property policyBuilder A lambda to configure exception-specific retry policies for this topic's strategy.
 *
 * > **⚠️ Alpha Feature Warning**
 * >
 * > The `policyBuilder` parameter is in alpha state and its API may change in future releases.
 * > Use with caution in production environments.
 */
data class TopicConfiguration<TStrategy : TopicRetryStrategy<*>>(
    val topic: String,
    val retry: TStrategy,
    val deadLetterTopic: String,
    val policyBuilder: TStrategy.() -> Unit = {}
) {
    init {
        require(topic.isNotBlank()) { "topic cannot be blank" }
        require(deadLetterTopic.isNotBlank()) { "deadLetterTopic cannot be blank" }
        retry.validate()
    }

    /**
     * Checks whether the given topic name matches this configuration's main topic
     * or any of its associated retry topics.
     *
     * @param topic The topic name to check.
     * @return `true` if the topic is handled by this configuration, `false` otherwise.
     */
    fun isSuitableTopic(topic: String): Boolean {
        if (this.topic == topic) {
            return true
        }
        return when (retry) {
            is TopicRetryStrategy.ExponentialBackoffToSingleTopicRetry -> retry.delayTopics.any { dt -> dt.topic == topic }
            is TopicRetryStrategy.ExponentialBackoffMultiTopicRetry -> retry.delayTopics.any { dt -> dt.topic == topic }
            is TopicRetryStrategy.SingleTopicRetry -> retry.retryTopic == topic
            TopicRetryStrategy.NoneStrategy -> false
        }
    }
}

/**
 * Defines the retry strategy for a topic configuration.
 *
 * This sealed class hierarchy represents different approaches to handling message failures,
 * from no retries at all to complex exponential backoff strategies across multiple topics.
 * Each strategy can have exception-specific retry policies configured via [onException] and [onCondition].
 *
 * @param TBuilder The type of [RetryPolicyBuilder] this strategy uses for building retry policies.
 */
sealed class TopicRetryStrategy<TBuilder : RetryPolicyBuilder> {
    private data class PolicyBinding<TBuilder : RetryPolicyBuilder>(
        val identifierFactory: (Throwable) -> PolicyIdentifier,
        val predicate: (IncomingMessage<*, *>, ConsumerContext, ExceptionDetails) -> Boolean,
        val builder: TBuilder
    )

    private val policyBindings = mutableListOf<PolicyBinding<TBuilder>>()

    protected abstract fun createBuilder(): TBuilder
    protected open fun validateBuilder(builder: TBuilder) {}

    /**
     * Converts this strategy into a [RetryPolicy] for the given message and exception.
     *
     * Iterates through all registered policy bindings and returns the first policy
     * whose predicate matches the current message and exception.
     * The policy identifier is computed dynamically from the exception.
     *
     * @param message The incoming message that failed processing.
     * @param context The consumer context.
     * @param exceptionDetails Details about the exception that occurred.
     * @return A matching [RetryPolicy], or `null` if no policies match.
     */
    internal fun toPolicyProvider(
        message: IncomingMessage<*, *>,
        context: ConsumerContext,
        exceptionDetails: ExceptionDetails
    ): RetryPolicy? =
        policyBindings.firstOrNull { binding ->
            binding.predicate(message, context, exceptionDetails)
        }?.let { binding ->
            val identifier = binding.identifierFactory(exceptionDetails.exception)
            binding.builder.toPolicy(identifier)
        }

    /**
     * Registers a retry policy for a specific exception type with a static identifier.
     *
     * > **⚠️ Alpha Feature Warning**
     * >
     * > This method is in alpha state and its API may change in future releases.
     * > Use with caution in production environments.
     *
     * @param exceptionType The class of the exception to match.
     * @param identifier The unique identifier for this policy.
     * @param configBlock A lambda to configure the retry policy builder.
     */
    fun <T : Throwable> onException(
        exceptionType: Class<T>,
        identifier: PolicyIdentifier,
        configBlock: TBuilder.() -> Unit
    ) = apply {
        val builder = createBuilder()
        builder.configBlock()
        validateBuilder(builder)
        policyBindings += PolicyBinding(
            identifierFactory = { identifier },
            predicate = { _, _, ex -> exceptionType.isInstance(ex.exception) },
            builder = builder
        )
    }

    /**
     * Registers a retry policy for a specific exception type with a dynamic identifier factory.
     *
     * The identifier is computed from the actual exception thrown at runtime, allowing you to
     * create unique identifiers for different exception instances or subtypes.
     *
     * > **⚠️ Alpha Feature Warning**
     * >
     * > This method is in alpha state and its API may change in future releases.
     * > Use with caution in production environments.
     *
     * @param exceptionType The class of the exception to match.
     * @param identifierFactory A function that computes the identifier from the exception.
     * @param configBlock A lambda to configure the retry policy builder.
     */
    fun <T : Throwable> onException(
        exceptionType: Class<T>,
        identifierFactory: (Throwable) -> PolicyIdentifier,
        configBlock: TBuilder.() -> Unit
    ) = apply {
        val builder = createBuilder()
        builder.configBlock()
        validateBuilder(builder)
        policyBindings += PolicyBinding(
            identifierFactory = identifierFactory,
            predicate = { _, _, ex -> exceptionType.isInstance(ex.exception) },
            builder = builder
        )
    }

    /**
     * Registers a retry policy for a specific exception type (reified version with static identifier).
     *
     * > **⚠️ Alpha Feature Warning**
     * >
     * > This method is in alpha state and its API may change in future releases.
     * > Use with caution in production environments.
     *
     * @param T The type of exception to match.
     * @param identifier The unique identifier for this policy. Defaults to the exception's simple name.
     * @param policyFactory A lambda to configure the retry policy builder.
     */
    inline fun <reified T : Throwable> onException(
        identifier: PolicyIdentifier = PolicyIdentifier(T::class.simpleName ?: "unknown"),
        crossinline policyFactory: TBuilder.() -> Unit
    ) = apply {
        this.onException(T::class.java, identifier) {
            policyFactory(this)
        }
    }

    /**
     * Registers a retry policy for a specific exception type with dynamic identifier derivation.
     *
     * The identifier is automatically derived from the actual exception type at runtime.
     * This is useful for catching broad exception types (like Throwable) while maintaining
     * unique identifiers for each specific exception subtype.
     *
     * > **⚠️ Alpha Feature Warning**
     * >
     * > This method is in alpha state and its API may change in future releases.
     * > Use with caution in production environments.
     *
     * @param T The type of exception to match.
     * @param identifierFactory A function that computes the identifier from the exception.
     * @param policyFactory A lambda to configure the retry policy builder.
     */
    inline fun <reified T : Throwable> onException(
        noinline identifierFactory: (Throwable) -> PolicyIdentifier,
        crossinline policyFactory: TBuilder.() -> Unit
    ) = apply {
        this.onException(T::class.java, identifierFactory) {
            policyFactory(this)
        }
    }

    /**
     * Registers a retry policy based on a custom condition/predicate with a static identifier.
     *
     * This method allows for more complex matching logic beyond simple exception type matching,
     * such as checking message headers, exception messages, or other contextual information.
     *
     * > **⚠️ Alpha Feature Warning**
     * >
     * > This method is in alpha state and its API may change in future releases.
     * > Use with caution in production environments.
     *
     * @param identifier The unique identifier for this policy.
     * @param predicate A function that determines whether this policy should apply to a given message/exception.
     * @param configBlock A lambda to configure the retry policy builder.
     */
    fun onCondition(
        identifier: PolicyIdentifier,
        predicate: (IncomingMessage<*, *>, ConsumerContext, ExceptionDetails) -> Boolean,
        configBlock: TBuilder.() -> Unit
    ) = apply {
        val builder = createBuilder()
        builder.configBlock()
        validateBuilder(builder)
        policyBindings += PolicyBinding(
            identifierFactory = { identifier },
            predicate = predicate,
            builder = builder
        )
    }

    /**
     * Registers a retry policy based on a custom condition/predicate with dynamic identifier.
     *
     * This method allows for more complex matching logic and dynamic identifier derivation.
     *
     * > **⚠️ Alpha Feature Warning**
     * >
     * > This method is in alpha state and its API may change in future releases.
     * > Use with caution in production environments.
     *
     * @param identifierFactory A function that computes the identifier from the exception.
     * @param predicate A function that determines whether this policy should apply to a given message/exception.
     * @param configBlock A lambda to configure the retry policy builder.
     */
    fun onCondition(
        identifierFactory: (Throwable) -> PolicyIdentifier,
        predicate: (IncomingMessage<*, *>, ConsumerContext, ExceptionDetails) -> Boolean,
        configBlock: TBuilder.() -> Unit
    ) = apply {
        val builder = createBuilder()
        builder.configBlock()
        validateBuilder(builder)
        policyBindings += PolicyBinding(
            identifierFactory = identifierFactory,
            predicate = predicate,
            builder = builder
        )
    }

    /**
     * Validates all registered policy bindings for this strategy.
     *
     * Checks strategy-specific validation rules for each policy builder.
     * Note: Duplicate identifier checks are not performed since identifiers are computed dynamically at runtime.
     *
     * @throws IllegalStateException if validation fails with details about what went wrong.
     */
    internal fun validate() {
        if (policyBindings.isEmpty()) return

        val errors = mutableListOf<String>()

        // Strategy-specific validation
        policyBindings.forEachIndexed { index, binding ->
            try {
                validateBuilder(binding.builder)
            } catch (ex: IllegalArgumentException) {
                errors += "Policy at index $index invalid: ${ex.message}"
            }
        }

        if (errors.isNotEmpty()) {
            throw IllegalStateException(
                "Strategy validation failed for ${this::class.simpleName}:\n" +
                    errors.joinToString("\n")
            )
        }
    }

    /**
     * Disable retries entirely – failed records are sent directly to the
     * [TopicConfiguration.deadLetterTopic].
     *
     * Useful for idempotent workflows where a single failure is considered fatal.
     */
    data object NoneStrategy : TopicRetryStrategy<RetryPolicyBuilder.InMemory>() {
        override fun createBuilder(): RetryPolicyBuilder.InMemory = RetryPolicyBuilder.InMemory()
    }

    /**
     * A simple strategy that forwards all failed messages to a single topic for reprocessing.
     *
     * @property retryTopic The single topic where all failed messages are sent for retrying.
     * @property maxOverallAttempts The total number of times a message should be processed
     */
    data class SingleTopicRetry(val retryTopic: String, val maxOverallAttempts: Int) : TopicRetryStrategy<RetryPolicyBuilder.Full>() {
        init {
            require(retryTopic.isNotBlank()) { "retryTopic cannot be blank" }
            require(maxOverallAttempts > 0) { "maxOverallAttempts must be positive" }
        }

        override fun createBuilder() = RetryPolicyBuilder.Full()

        override fun validateBuilder(builder: RetryPolicyBuilder.Full) {
            val inMem = builder.inMemoryConfig?.retryConfig?.maxAttempts ?: 0
            val nonBlocking = builder.nonBlockingConfig?.maxAttempts ?: 0
            val total = inMem + nonBlocking
            require(total <= maxOverallAttempts) {
                "Total retry attempts ($total) exceeds maxOverallAttempts ($maxOverallAttempts)"
            }
        }
    }

    /**
     * Defines a single "level" or "bucket" in an exponential backoff strategy,
     * associating a topic with a maximum delay threshold.
     *
     * @property topic The name of the topic for this specific delay level.
     * @property maxDelay The maximum delay this level can handle. A calculated delay will be routed to the first level whose threshold is greater than or equal to the delay.
     */
    data class DelayTopicConfiguration(val topic: String, val maxDelay: Duration) {
        init {
            require(topic.isNotBlank()) { "topic cannot be blank" }
        }
    }

    /**
     * Base class for retry strategies that use multiple topics with different delay thresholds.
     *
     * Messages are routed to different topics based on their calculated retry delay, allowing
     * for efficient processing of messages with varying delay requirements.
     *
     * @property delayTopics A list of delay topic configurations, each representing a "bucket" for a delay range.
     * @property maxOverallAttempts The total number of times a message should be processed across all retry attempts.
     */
    sealed class MultiTopicRetry(open val delayTopics: List<DelayTopicConfiguration>, val maxOverallAttempts: Int) :
        TopicRetryStrategy<RetryPolicyBuilder.Full>() {

        private val sortedDelayedTopics = delayTopics.sortedBy { it.maxDelay }

        init {
            require(delayTopics.isNotEmpty()) { "delayTopics cannot be empty" }
            require(maxOverallAttempts > 0) { "maxOverallAttempts must be positive" }
        }

        /**
         * Finds the appropriate delay level for a given retry delay.
         *
         * If no bucket can accommodate the delay, falls back to the maximum bucket
         * to prevent message loss.
         *
         * @param delay The calculated delay for the current retry attempt.
         * @return The delay topic configuration, never null. Falls back to max bucket if delay exceeds all thresholds.
         */
        open fun findBucket(delay: Duration): DelayTopicConfiguration =
            sortedDelayedTopics.firstOrNull { it.maxDelay >= delay }
                ?: sortedDelayedTopics.lastOrNull()
                ?: throw IllegalStateException("No delay topics configured for ${this.javaClass.simpleName}")

        override fun createBuilder() = RetryPolicyBuilder.Full()

        override fun validateBuilder(builder: RetryPolicyBuilder.Full) {
            val inMem = builder.inMemoryConfig?.retryConfig?.maxAttempts ?: 0
            val nonBlocking = builder.nonBlockingConfig?.maxAttempts ?: 0
            val total = inMem + nonBlocking
            require(total <= maxOverallAttempts) {
                "Total retry attempts ($total) exceeds maxOverallAttempts ($maxOverallAttempts)"
            }
        }
    }

    /**
     * An exponential backoff strategy that forwards a failed message to a temporary delay topic.
     * After the delay, the message is then forwarded to a **single, final retry topic** for consumption.
     * This simplifies the consumer, which only needs to subscribe to one topic for all retries.
     *
     * ### Example Flow:
     * 1. Message fails on `input-topic`.
     * 2. First retry is calculated to be 8s. It's sent to delay topic `delay-10s`.
     * 3. After the delay, the message is forwarded to `input-retry-topic`.
     * 4. Application consumes from `input-retry-topic` and it fails again.
     * 5. Second retry is calculated to be 15s. It's sent to delay topic `delay-30s`.
     * 6. After the delay, the message is forwarded back to `input-retry-topic`.
     *
     * @property retryTopic The single, final topic the application subscribes to for consuming all retried messages.
     * @property maxOverallAttempts The total number of times a message should be processed
     * @property delayTopics A list of topic configurations that act as buckets for different delay durations.
     */
    class ExponentialBackoffToSingleTopicRetry(
        val retryTopic: String,
        maxOverallAttempts: Int,
        delayTopics: List<DelayTopicConfiguration>
    ) : MultiTopicRetry(delayTopics, maxOverallAttempts) {
        init {
            require(retryTopic.isNotBlank()) { "retryTopic cannot be blank" }
        }
    }

    /**
     * An exponential backoff strategy that forwards a failed message to a specific delay topic
     * based on the retry attempt. The application must **subscribe to each of the delay topics**
     * to process retried messages.
     *
     * ### Example Flow:
     * 1. Message fails on `input-topic`.
     * 2. First retry is calculated to be 8s. It's sent to delay topic `input-retry-10s`.
     * 3. Application consumes from `input-retry-10s` and it fails again.
     * 4. Second retry is calculated to be 15s. It's sent to delay topic `input-retry-30s`.
     * 5. Application consumes from `input-retry-30s`.
     *
     * @property delayTopics A list of topic configurations that act as buckets for different delay durations.
     * @property maxOverallAttempts The total number of times a message should be processed
     */
    class ExponentialBackoffMultiTopicRetry(retryTopics: List<DelayTopicConfiguration>, maxOverallAttempts: Int) :
        MultiTopicRetry(retryTopics, maxOverallAttempts)
}
