## Consumer Examples

### Single message handling
```kotlin
import com.trendyol.quafka.consumer.configuration.*
import org.apache.kafka.common.serialization.StringDeserializer

val props = HashMap<String, Any>()
props[org.apache.kafka.clients.consumer.ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG] = "localhost:9092"
props[org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_ID_CONFIG] = "example-group"

val consumer = QuafkaConsumerBuilder<String, String>(props)
    .withDeserializer(StringDeserializer(), StringDeserializer())
    .subscribe("example-topic") {
        withSingleMessageHandler { incomingMessage, consumerContext ->
            println("Received: ${incomingMessage.toString(addValue = true, addHeaders = true)}")
            incomingMessage.ack()
        }.autoAckAfterProcess(true)
    }
    .build()

consumer.start()
```

### Batch message handling
```kotlin
import com.trendyol.quafka.consumer.configuration.*
import com.trendyol.quafka.consumer.ackAll
import org.apache.kafka.common.serialization.StringDeserializer

val consumer = QuafkaConsumerBuilder<String, String>(props)
    .withDeserializer(StringDeserializer(), StringDeserializer())
    .subscribe("example-topic") {
        withBatchMessageHandler { incomingMessages, consumerContext ->
            incomingMessages.forEach { msg ->
                println("Batch item: ${msg.toString(addValue = true, addHeaders = true)}")
            }
            incomingMessages.ackAll()
        }
    }
    .build()
```

### Backpressure handling
```kotlin
import kotlin.time.Duration.Companion.minutes

val consumer = QuafkaConsumerBuilder<String, String>(props)
    .withDeserializer(StringDeserializer(), StringDeserializer())
    .subscribe("example-topic") {
        withSingleMessageHandler { msg, ctx ->
            // slow work here
            msg.ack()
        }.withBackpressure(backpressureBufferSize = 10_000, backpressureReleaseTimeout = 1.minutes)
    }
    .build()
```

### Message ack vs commit
```kotlin
import kotlinx.coroutines.runBlocking

val consumer = QuafkaConsumerBuilder<String, String>(props)
    .withDeserializer(StringDeserializer(), StringDeserializer())
    .subscribe("example-topic") {
        withSingleMessageHandler { msg, ctx ->
            // Manual control: ack marks processed in Quafka, commit persists offset to Kafka
            msg.ack()
            runBlocking { msg.commit().await() }
        }.autoAckAfterProcess(false)
    }
    .build()
```

### Retry strategies (extensions)
```kotlin
import com.trendyol.quafka.extensions.errorHandling.configuration.subscribeWithErrorHandling
import com.trendyol.quafka.extensions.errorHandling.recoverer.*
import com.trendyol.quafka.extensions.delaying.MessageDelayer
import kotlin.time.Duration.Companion.seconds
import kotlin.time.Duration.Companion.minutes
import kotlin.time.Duration.Companion.milliseconds

val topic = TopicConfiguration(
    topic = "orders.v1",
    retry = TopicConfiguration.TopicRetryStrategy.ExponentialBackoffMultiTopicRetry(
        maxTotalRetryAttempts = 5,
        delayTopics = listOf(
            TopicConfiguration.TopicRetryStrategy.DelayTopicConfiguration("orders.v1.retry.10s", 10.seconds),
            TopicConfiguration.TopicRetryStrategy.DelayTopicConfiguration("orders.v1.retry.1m", 1.minutes)
        )
    ),
    deadLetterTopic = "orders.v1.error"
)

val consumer = QuafkaConsumerBuilder<String, String>(props)
    .withDeserializer(StringDeserializer(), StringDeserializer())
    .subscribeWithErrorHandling(listOf(topic), danglingTopic = "orders.dangling", producer = producer) {
        this
            .withRecoverable {
                withMessageDelayer(MessageDelayer())
                .withPolicyProvider { message, context, details ->
                    when (details.exception) {
                        is IllegalStateException -> RetryPolicy.FullRetry(
                            identifier = "IllegalState",
                            inMemoryConfig = InMemoryRetryConfig.basic(maxAttempts = 3, initialDelay = 100.milliseconds),
                            nonBlockingConfig = NonBlockingRetryConfig.exponentialRandomBackoff(
                                maxAttempts = 5,
                                initialDelay = 1.seconds,
                                maxDelay = 60.seconds
                            )
                        )
                        else -> RetryPolicy.NoRetry
                    }
                }
            }
            .withSingleMessageHandler { msg, ctx ->
                // business logic may throw; policy above will drive retry
                msg.ack()
            }
            .autoAckAfterProcess(true)
    }
    .build()
```

### Message consuming middlewares (pipeline)
```kotlin
import com.trendyol.quafka.extensions.consumer.single.pipelines.PipelineMessageHandler.Companion.usePipelineMessageHandler
import com.trendyol.quafka.extensions.common.pipelines.*

val consumer = QuafkaConsumerBuilder<String, String>(props)
    .withDeserializer(StringDeserializer(), StringDeserializer())
    .subscribe("example-topic") {
        usePipelineMessageHandler {
            use { envelope: SingleMessageEnvelope<String, String>, next ->
                // middleware A
                next()
            }
            use { envelope: SingleMessageEnvelope<String, String>, next ->
                // middleware B
                next()
            }
        }.autoAckAfterProcess(true)
    }
    .build()
```
