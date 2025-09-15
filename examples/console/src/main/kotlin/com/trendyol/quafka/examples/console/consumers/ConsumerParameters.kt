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

class ConsumerParameters(
    servers: String
) : ConsumerBase(servers) {
    override fun run(properties: MutableMap<String, Any>) {
        val deserializer = StringDeserializer()
        val consumer =
            QuafkaConsumerBuilder<String, String>(properties)
                .withDeserializer(deserializer, deserializer)
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
