package com.trendyol.quafka.examples.console.consumers

import com.trendyol.quafka.common.TimeProvider
import com.trendyol.quafka.consumer.*
import com.trendyol.quafka.consumer.configuration.*
import io.github.resilience4j.retry.*
import kotlinx.coroutines.CoroutineExceptionHandler
import org.apache.kafka.common.serialization.StringDeserializer
import org.slf4j.event.Level
import java.time.Instant
import kotlin.time.Duration.Companion.seconds

/**
 * Consumer demonstrating advanced configuration options.
 *
 * **What it shows:**
 * - Auto client ID generation
 * - Custom commit options (interval)
 * - Custom time provider
 * - Custom poll duration
 * - Custom message formatter for logging
 * - Connection retry policy
 * - Coroutine exception handler
 *
 * **Use when:** Need fine-grained control over consumer behavior and customization.
 */
class ConsumerParameters(properties: MutableMap<String, Any>) : ConsumerBase(properties) {
    override suspend fun run() {
        val consumer =
            QuafkaConsumerBuilder<String, String>(properties)
                .withDeserializer(StringDeserializer())
                .withAutoClientId()
                .withCommitOptions(CommitOptions(10.seconds))
                .withTimeProvider(MyTimeProvider())
                .withPollDuration(5.seconds)
                .withMessageFormatter(MyMessageFormatter())
                .withConnectionRetryPolicy(Retry.of("retry", RetryConfig.custom<String>().maxAttempts(1).build()))
                .withCoroutineExceptionHandler(
                    CoroutineExceptionHandler { coroutineContext, throwable ->
                        logger.error(
                            "Coroutine $coroutineContext failed with an uncaught exception.",
                            throwable
                        )
                    }
                ).subscribe("topic-example") {
                    withSingleMessageHandler { incomingMessage, consumerContext ->
                        logger.info("Received message: ${incomingMessage.toString(addValue = true, addHeaders = true)}")
                        incomingMessage.ack()
                    }
                }.build()

        consumer.start()
    }

    class MyMessageFormatter : IncomingMessageStringFormatter() {
        override fun format(
            incomingMessage: IncomingMessage<*, *>,
            logLevel: Level,
            addValue: Boolean,
            addHeaders: Boolean
        ): String = "MyMessageFormatter: " + super.format(incomingMessage, logLevel, addValue, addHeaders)
    }

    class MyTimeProvider : TimeProvider {
        override fun now(): Instant = Instant.now()
    }
}
