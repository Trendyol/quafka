package com.trendyol.quafka.producer

import com.trendyol.quafka.logging.LogParameters
import com.trendyol.quafka.logging.LoggerHelper.createLogger
import com.trendyol.quafka.producer.configuration.*
import kotlinx.coroutines.*
import org.apache.kafka.clients.producer.*
import org.slf4j.event.Level
import java.io.Closeable
import java.util.concurrent.atomic.*
import kotlin.concurrent.atomics.ExperimentalAtomicApi
import kotlin.time.measureTimedValue

/**
 * A Kafka producer wrapper with enhanced functionality and coroutine support.
 *
 * The `QuafkaProducer` class simplifies message production to Kafka by integrating coroutine-based operations,
 * configurable options, and logging capabilities. It also ensures efficient batch processing and error handling.
 *
 * @param TKey The type of the message key.
 * @param TValue The type of the message value.
 * @property quafkaProducerOptions The configuration options for the producer.
 */
class QuafkaProducer<TKey, TValue> internal constructor(val quafkaProducerOptions: QuafkaProducerOptions<TKey, TValue>) : Closeable {
    /**
     * The underlying Kafka producer instance used for sending messages.
     */
    val underlyingProducer: KafkaProducer<TKey, TValue> =
        KafkaProducer(
            quafkaProducerOptions.properties,
            quafkaProducerOptions.keySerializer,
            quafkaProducerOptions.valueSerializer
        )

    private val logger = createLogger(
        clazz = QuafkaProducer::class.java,
        suffix = "producer_${this.quafkaProducerOptions.clientId()}",
        keyValuePairs = mutableMapOf(
            LogParameters.CLIENT_ID to (quafkaProducerOptions.clientId() ?: "")
        )
    )

    private val outgoingMessageStringFormatter = quafkaProducerOptions.outgoingMessageStringFormatter
    private val stopping: AtomicBoolean = AtomicBoolean(false)

    /**
     * Sends a single message to Kafka.
     *
     * @param outgoingMessage The message to be sent.
     * @return The result of the message delivery.
     */
    suspend fun send(outgoingMessage: OutgoingMessage<TKey, TValue>): DeliveryResult =
        send(outgoingMessage, quafkaProducerOptions.producingOptions)

    /**
     * Sends a single message to Kafka with specified producing options.
     *
     * @param outgoingMessage The message to be sent.
     * @param options The options controlling error handling and retries.
     * @return The result of the message delivery.
     */
    suspend fun send(
        outgoingMessage: OutgoingMessage<TKey, TValue>,
        options: ProducingOptions
    ): DeliveryResult = sendAll(listOf(outgoingMessage), options).single()

    /**
     * Sends a collection of messages to Kafka using default producing options.
     *
     * @param outgoingMessages The messages to be sent.
     * @return A collection of delivery results for the messages.
     */
    suspend fun sendAll(
        outgoingMessages: Collection<OutgoingMessage<TKey, TValue>>
    ): Collection<DeliveryResult> = sendAll(outgoingMessages, quafkaProducerOptions.producingOptions)

    /**
     * Sends a collection of messages to Kafka with specified producing options.
     *
     * @param outgoingMessages The messages to be sent.
     * @param options The options controlling error handling and retries.
     * @return A collection of delivery results for the messages.
     */
    suspend fun sendAll(
        outgoingMessages: Collection<OutgoingMessage<TKey, TValue>>,
        options: ProducingOptions
    ): Collection<DeliveryResult> {
        if (logger.isTraceEnabled) {
            outgoingMessages.forEach { message ->
                logger.trace(
                    "message will be sent with ack waiting. | {}",
                    message.toString(
                        logLevel = Level.TRACE,
                        addValue = true,
                        addHeaders = true,
                        outgoingMessageStringFormatter = outgoingMessageStringFormatter
                    )
                )
            }
        }

        val result = measureTimedValue {
            val deferredBatchSender = DeferredBatchSender(outgoingMessages, options)
            deferredBatchSender.start().await()
        }
        if (logger.isTraceEnabled) {
            logger.trace("${outgoingMessages.size} messages were sent to Kafka in ${result.duration}.")
        }

        return result.value
    }

    /**
     * Closes the producer, flushing any pending messages and releasing resources.
     */
    override fun close() {
        if (!stopping.compareAndSet(false, true)) {
            return
        }
        underlyingProducer.flush()
        underlyingProducer.close()
        logger.info("Producer flushed and closed.")
    }

    /**
     * Inner class to handle batch message sending with coroutine support and error handling.
     *
     * @param messages The collection of messages to send.
     * @param producingOptions The options controlling error handling and retries.
     */
    private inner class DeferredBatchSender(
        private val messages: Collection<OutgoingMessage<TKey, TValue>>,
        private val producingOptions: ProducingOptions
    ) {
        @OptIn(ExperimentalAtomicApi::class)
        private val deliveryResults = AtomicReferenceArray<DeliveryResult>(messages.size)
        private val inflightMessages = AtomicInteger()
        private val firstException = AtomicReference<Throwable>()
        private val continuation = CompletableDeferred<Collection<DeliveryResult>>()
        private val completed = AtomicBoolean()
        private val logLevel = if (producingOptions.stopOnFirstError) Level.ERROR else Level.WARN

        /**
         * Starts sending messages in the batch and returns a deferred result.
         *
         * @return A deferred collection of delivery results.
         */
        fun start(): CompletableDeferred<Collection<DeliveryResult>> {
            inflightMessages.set(messages.size)
            messages.forEachIndexed { i, m -> send(i, m) }
            return continuation
        }

        @OptIn(ExperimentalAtomicApi::class)
        private fun send(index: Int, message: OutgoingMessage<TKey, TValue>) {
            if (completed.get()) return

            val callback = Callback { metadata, exception ->
                if (completed.get()) return@Callback

                val deliveryResult = DeliveryResult(
                    metadata,
                    exception,
                    message.correlationMetadata,
                    quafkaProducerOptions.outgoingMessageStringFormatter
                )
                deliveryResults.set(index, deliveryResult)

                if (exception != null) {
                    if (logger.isEnabledForLevel(logLevel)) {
                        logger.atLevel(logLevel).log(
                            "Message could not be sent to Kafka. | {} | {}",
                            message.toString(
                                logLevel = logLevel,
                                outgoingMessageStringFormatter = outgoingMessageStringFormatter
                            ),
                            deliveryResult.toString(logLevel),
                            exception
                        )
                    }
                    firstException.compareAndSet(null, exception)
                    if (producingOptions.stopOnFirstError || producingOptions.isUnrecoverableException(exception)) {
                        tryToComplete()
                        return@Callback
                    }
                } else {
                    if (logger.isTraceEnabled) {
                        logger.trace(
                            "Message successfully sent to Kafka. | {} | {}",
                            message.toString(logLevel = Level.TRACE, outgoingMessageStringFormatter = outgoingMessageStringFormatter),
                            deliveryResult.toString(logLevel = Level.TRACE)
                        )
                    }
                }

                if (inflightMessages.decrementAndGet() == 0) {
                    tryToComplete()
                }
            }

            try {
                underlyingProducer.send(message.toProducerRecord(), callback)
            } catch (exception: Throwable) {
                if (logger.isEnabledForLevel(logLevel)) {
                    logger.atLevel(logLevel).log(
                        "Error occurred while sending message. | {}",
                        message.toString(
                            logLevel = logLevel,
                            addHeaders = true,
                            outgoingMessageStringFormatter = outgoingMessageStringFormatter
                        ),
                        exception
                    )
                }

                firstException.compareAndSet(null, exception)
                if (producingOptions.stopOnFirstError || producingOptions.isUnrecoverableException(exception)) {
                    tryToComplete()
                }
            }
        }

        private fun tryToComplete() {
            val ex = firstException.get()
            if (completed.compareAndSet(false, true)) {
                if (ex != null && producingOptions.throwException) {
                    continuation.completeExceptionally(ex)
                } else {
                    val list = ArrayList<DeliveryResult>(deliveryResults.length())
                    for (i in 0 until deliveryResults.length()) {
                        list.add(deliveryResults.get(i))
                    }
                    continuation.complete(list)
                }
            }
        }
    }
}
