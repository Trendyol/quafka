package com.trendyol.quafka.extensions.consumer.batch.pipelines.middlewares

import com.trendyol.quafka.extensions.common.pipelines.*
import com.trendyol.quafka.extensions.consumer.batch.pipelines.*
import com.trendyol.quafka.extensions.consumer.single.pipelines.*
import kotlinx.coroutines.*
import kotlinx.coroutines.flow.*

/**
 * Adapter middleware that processes each message in a batch through a single message pipeline.
 *
 * This middleware allows you to reuse single message pipelines within a batch processing context.
 * Each message in the batch is individually processed through the provided single message pipeline,
 * while maintaining the batch context and attributes.
 *
 * Processing mode is determined automatically based on concurrency level:
 * - concurrencyLevel <= 1: Sequential processing (maintains message order)
 * - concurrencyLevel > 1: Concurrent processing (improves throughput)
 *
 * Features:
 * - Automatic sequential/concurrent mode based on concurrency level
 * - Configurable concurrency level for parallel processing
 * - Optionally shares batch-level attributes with each single message envelope
 * - Optionally collects single message attributes back to batch envelope
 *
 * @param TKey The type of the message key.
 * @param TValue The type of the message value.
 * @property singleMessagePipeline The pipeline to execute for each individual message.
 * @property concurrencyLevel Maximum number of messages to process concurrently.
 *                           - 1 or less: Sequential processing
 *                           - Greater than 1: Concurrent processing with specified level
 * @property shareAttributes If true, batch attributes are copied to each single message envelope.
 * @property collectAttributes If true, single message attributes are merged back to the batch envelope.
 *
 * @sample Sequential processing (concurrencyLevel = 1):
 * ```kotlin
 * val adapter = SingleMessagePipelineAdapter(
 *     singlePipeline,
 *     concurrencyLevel = 1  // Sequential
 * )
 * ```
 *
 * @sample Concurrent processing (concurrencyLevel > 1):
 * ```kotlin
 * val adapter = SingleMessagePipelineAdapter(
 *     singlePipeline,
 *     concurrencyLevel = 10  // Process up to 10 messages concurrently
 * )
 * ```
 */
class SingleMessagePipelineAdapter<TKey, TValue>(
    private val singleMessagePipeline: Pipeline<SingleMessageEnvelope<TKey, TValue>>,
    private val concurrencyLevel: Int = 1,
    private val shareAttributes: Boolean = false,
    private val collectAttributes: Boolean = false
) : BatchMessageBaseMiddleware<BatchMessageEnvelope<TKey, TValue>, TKey, TValue>() {

    private val isSequential = concurrencyLevel <= 1

    override suspend fun execute(
        envelope: BatchMessageEnvelope<TKey, TValue>,
        next: suspend (BatchMessageEnvelope<TKey, TValue>) -> Unit
    ) {
        if (isSequential) {
            processSequentially(envelope)
        } else {
            processConcurrently(envelope)
        }

        // Continue with the batch pipeline
        next(envelope)
    }

    /**
     * Processes messages sequentially, one after another.
     * Maintains message order and is safer for stateful operations.
     */
    private suspend fun processSequentially(envelope: BatchMessageEnvelope<TKey, TValue>) {
        // Collect attributes from each message if needed
        val collectedAttributes = if (collectAttributes) {
            mutableListOf<Attributes>()
        } else {
            null
        }

        for (message in envelope.messages) {
            val singleEnvelope = createSingleEnvelope(message, envelope)
            singleMessagePipeline.execute(singleEnvelope)

            // Collect attributes for later merge
            if (collectAttributes && collectedAttributes != null) {
                collectedAttributes.add(singleEnvelope.attributes)
            }
        }

        // Merge all collected attributes once at the end
        if (collectAttributes && collectedAttributes != null) {
            collectedAttributes.forEach { attrs ->
                envelope.attributes.putAll(attrs)
            }
        }
    }

    /**
     * Processes messages concurrently with flow and flatMapMerge.
     * Improves throughput but doesn't guarantee message order.
     */
    @OptIn(ExperimentalCoroutinesApi::class)
    private suspend fun processConcurrently(envelope: BatchMessageEnvelope<TKey, TValue>) {
        // Collect attributes from each message in a thread-safe collection
        val collectedAttributes = if (collectAttributes) {
            java.util.concurrent.ConcurrentHashMap<Int, Attributes>()
        } else {
            null
        }

        coroutineScope {
            envelope.messages
                .withIndex()
                .asFlow()
                .flatMapMerge(concurrency = concurrencyLevel) { (index, message) ->
                    flow {
                        val singleEnvelope = createSingleEnvelope(message, envelope)
                        singleMessagePipeline.execute(singleEnvelope)

                        // Store attributes with message index if collecting
                        if (collectAttributes && collectedAttributes != null) {
                            collectedAttributes[index] = singleEnvelope.attributes
                        }

                        emit(Unit)
                    }
                }
                .collect()
        }

        // Merge all collected attributes once at the end (only one operation)
        if (collectAttributes && collectedAttributes != null) {
            collectedAttributes.values.forEach { attrs ->
                envelope.attributes.putAll(attrs)
            }
        }
    }

    /**
     * Creates a single message envelope from a batch message.
     */
    private fun createSingleEnvelope(
        message: com.trendyol.quafka.consumer.IncomingMessage<TKey, TValue>,
        batchEnvelope: BatchMessageEnvelope<TKey, TValue>
    ): SingleMessageEnvelope<TKey, TValue> = SingleMessageEnvelope(
        message = message,
        consumerContext = batchEnvelope.consumerContext,
        attributes = if (shareAttributes) {
            Attributes().apply { putAll(batchEnvelope.attributes) }
        } else {
            Attributes()
        }
    )
}

/**
 * Extension function to easily add a single message pipeline adapter to a batch pipeline.
 *
 * @param TKey The type of the message key.
 * @param TValue The type of the message value.
 * @param concurrencyLevel Maximum number of messages to process concurrently.
 *                        - 1 or less: Sequential processing (default)
 *                        - Greater than 1: Concurrent processing
 * @param shareAttributes If true, batch attributes are copied to each single message envelope.
 * @param collectAttributes If true, single message attributes are merged back to the batch envelope.
 * @param configure Lambda to configure the single message pipeline.
 * @return This [PipelineBuilder] instance for method chaining.
 *
 * @sample Sequential processing (default):
 * ```kotlin
 * PipelineBuilder<BatchMessageEnvelope<String, String>>()
 *     .useSingleMessagePipeline { // Sequential by default (concurrencyLevel = 1)
 *         use { envelope, next ->
 *             logger.info("Message: ${envelope.message.value}")
 *             next()
 *         }
 *     }
 *     .build()
 * ```
 *
 * @sample Concurrent processing:
 * ```kotlin
 * PipelineBuilder<BatchMessageEnvelope<String, String>>()
 *     .useSingleMessagePipeline(concurrencyLevel = 10) {
 *         use { envelope, next ->
 *             logger.info("Message: ${envelope.message.value}")
 *             next()
 *         }
 *     }
 *     .build()
 * ```
 */
fun <TKey, TValue> PipelineBuilder<BatchMessageEnvelope<TKey, TValue>>.useSingleMessagePipeline(
    concurrencyLevel: Int = 1,
    shareAttributes: Boolean = false,
    collectAttributes: Boolean = false,
    configure: PipelineBuilder<SingleMessageEnvelope<TKey, TValue>>.() -> Unit
): PipelineBuilder<BatchMessageEnvelope<TKey, TValue>> {
    val singlePipelineBuilder = PipelineBuilder<SingleMessageEnvelope<TKey, TValue>>()
    singlePipelineBuilder.apply(configure)
    val singlePipeline = singlePipelineBuilder.build()

    val adapter = SingleMessagePipelineAdapter<TKey, TValue>(
        singlePipeline,
        concurrencyLevel,
        shareAttributes,
        collectAttributes
    )

    return this.useMiddleware(adapter)
}
