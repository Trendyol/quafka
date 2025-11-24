# Single Consumer Examples

This document provides comprehensive examples for single message consumer patterns in Quafka.

## Table of Contents
- [Basic Single Consumer](#basic-single-consumer)
- [Consumer with Backpressure](#consumer-with-backpressure)
- [Pipeline Consumer](#pipeline-consumer)
- [Retryable Consumer](#retryable-consumer)
- [Custom Middleware](#custom-middleware)

---

## Basic Single Consumer

The simplest form of a Quafka consumer that processes messages one at a time.

### Concept
A basic single consumer receives messages from Kafka and processes them sequentially. Each message is handled individually, and you have full control over acknowledgment.

### Use Cases
- Simple message processing
- Sequential processing requirements
- Low throughput scenarios
- Learning Quafka basics

### Example

```kotlin
package com.trendyol.quafka.examples.console.consumers.single

import com.trendyol.quafka.consumer.configuration.*
import org.apache.kafka.common.serialization.StringDeserializer

class BasicConsumer(servers: String) {
    fun run() {
        val properties = mutableMapOf<String, Any>(
            "bootstrap.servers" to servers,
            "group.id" to "basic-consumer-group",
            "auto.offset.reset" to "earliest"
        )
        
        val deserializer = StringDeserializer()
        val consumer = QuafkaConsumerBuilder<String, String>(properties)
            .withDeserializer(deserializer, deserializer)
            .subscribe("topic-example") {
                withSingleMessageHandler { incomingMessage, consumerContext ->
                    // Your business logic here
                    println("Received: ${incomingMessage.value}")
                    println("Topic: ${incomingMessage.topic}")
                    println("Partition: ${incomingMessage.partition}")
                    println("Offset: ${incomingMessage.offset}")
                    
                    // Acknowledge the message after successful processing
                    incomingMessage.ack()
                }
            }.build()
        
        consumer.start()
    }
}
```

### Key Points
- **Message Handler**: `withSingleMessageHandler` processes one message at a time
- **Message Properties**: Access to topic, partition, offset, key, value, and headers
- **Acknowledgment**: Call `ack()` to mark the message as processed
- **Auto Commit**: Quafka handles offset commits automatically after acknowledgment

---

## Consumer with Backpressure

A consumer that implements backpressure to control the rate of message processing.

### Concept
Backpressure allows you to control how many messages are being processed concurrently. This prevents overwhelming your application with too many messages at once, especially when processing is slow or resource-intensive.

### Use Cases
- Rate limiting message processing
- Protecting downstream services from overload
- Memory management for large message volumes
- Slow processing operations (database writes, API calls)

### Example

```kotlin
package com.trendyol.quafka.examples.console.consumers.single

import com.trendyol.quafka.consumer.configuration.*
import org.apache.kafka.common.serialization.StringDeserializer
import kotlin.time.Duration.Companion.minutes

class BasicConsumerWithBackpressure(servers: String) {
    fun run() {
        val properties = mutableMapOf<String, Any>(
            "bootstrap.servers" to servers,
            "group.id" to "backpressure-consumer-group"
        )
        
        val deserializer = StringDeserializer()
        val consumer = QuafkaConsumerBuilder<String, String>(properties)
            .withDeserializer(deserializer, deserializer)
            .subscribe("topic-example") {
                withSingleMessageHandler { incomingMessage, consumerContext ->
                    // Simulate slow processing
                    delay(100)
                    
                    println("Processing: ${incomingMessage.value}")
                    incomingMessage.ack()
                }
                .withBackpressure(
                    backpressureBufferSize = 10000,
                    backpressureReleaseTimeout = 1.minutes
                )
            }.build()
        
        consumer.start()
    }
}
```

### Parameters
- **backpressureBufferSize**: Maximum number of messages that can be in the processing buffer
- **backpressureReleaseTimeout**: How long to wait before releasing pressure if buffer isn't full

### Key Points
- Prevents memory overflow from too many concurrent messages
- Blocks Kafka polling when buffer is full
- Automatically resumes polling when buffer has space
- Essential for production systems with variable load

---

## Pipeline Consumer

A consumer using the pipeline pattern to chain multiple processing steps.

### Concept
Pipeline pattern allows you to compose message processing logic from multiple middleware components. Each middleware can perform pre-processing, pass control to the next middleware, and perform post-processing. This creates a clean, modular architecture.

### Use Cases
- Logging and monitoring
- Validation and filtering
- Transformation and enrichment
- Error handling and retry logic
- Metrics collection
- Multiple processing stages

### Example

```kotlin
package com.trendyol.quafka.examples.console.consumers.single

import com.trendyol.quafka.consumer.configuration.*
import com.trendyol.quafka.extensions.consumer.single.pipelines.*
import com.trendyol.quafka.extensions.consumer.single.pipelines.PipelineMessageHandler.Companion.usePipelineMessageHandler
import com.trendyol.quafka.extensions.common.pipelines.*
import org.apache.kafka.common.serialization.StringDeserializer
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import kotlin.system.measureTimeMillis

class PipelinedConsumer(servers: String) {
    private val logger: Logger = LoggerFactory.getLogger(PipelinedConsumer::class.java)
    
    fun run() {
        val properties = mutableMapOf<String, Any>(
            "bootstrap.servers" to servers,
            "group.id" to "pipeline-consumer-group"
        )
        
        val deserializer = StringDeserializer()
        val consumer = QuafkaConsumerBuilder<String, String>(properties)
            .withDeserializer(deserializer, deserializer)
            .subscribe("topic-example") {
                usePipelineMessageHandler {
                    // Middleware 1: Logging
                    use { envelope: SingleMessageEnvelope<String, String>, next ->
                        logger.info("Starting to process message at offset: ${envelope.message.offset}")
                        next()
                        logger.info("Finished processing message at offset: ${envelope.message.offset}")
                    }
                    
                    // Middleware 2: Validation
                    use { envelope: SingleMessageEnvelope<String, String>, next ->
                        if (envelope.message.value.isNotBlank()) {
                            next()
                        } else {
                            logger.warn("Empty message value, skipping")
                            envelope.message.ack() // Still ack to avoid reprocessing
                        }
                    }
                    
                    // Middleware 3: Performance measurement
                    useMiddleware(MeasureMiddleware(logger))
                    
                    // Middleware 4: Business logic
                    use { envelope: SingleMessageEnvelope<String, String>, next ->
                        // Your actual business logic
                        processMessage(envelope.message.value)
                        
                        // Store metadata in attributes for downstream middleware
                        envelope.attributes.put(
                            AttributeKey("processedAt"),
                            System.currentTimeMillis()
                        )
                        next()
                    }
                    
                    // Middleware 5: Acknowledgment
                    use { envelope: SingleMessageEnvelope<String, String>, next ->
                        envelope.message.ack()
                        next()
                    }
                }.autoAckAfterProcess(true)
            }.build()
        
        consumer.start()
    }
    
    private fun processMessage(value: String) {
        // Your business logic here
        println("Processing: $value")
    }
}

// Custom middleware example
class MeasureMiddleware<TEnvelope, TKey, TValue>(
    private val logger: Logger
) : SingleMessageBaseMiddleware<TEnvelope, TKey, TValue>() 
    where TEnvelope : SingleMessageEnvelope<TKey, TValue> {
    
    override suspend fun execute(
        envelope: TEnvelope,
        next: MiddlewareFn<TEnvelope>
    ) {
        val ms = measureTimeMillis {
            next(envelope)
        }
        logger.info("Message processing took $ms ms")
    }
}
```

### Pipeline Benefits
1. **Separation of Concerns**: Each middleware has a single responsibility
2. **Reusability**: Middleware can be reused across different consumers
3. **Testability**: Each middleware can be tested independently
4. **Flexibility**: Easy to add, remove, or reorder processing steps
5. **Attributes**: Share data between middleware using attributes

### Key Concepts

#### Middleware Execution Order
Middleware executes in the order they are added:
```
Request Flow: Middleware 1 → Middleware 2 → Middleware 3
Response Flow: Middleware 3 → Middleware 2 → Middleware 1
```

#### Attributes for Data Sharing
```kotlin
use { envelope, next ->
    // Store data
    envelope.attributes.put(AttributeKey<String>("userId"), "12345")
    next()
}

use { envelope, next ->
    // Retrieve data
    val userId = envelope.attributes.getOrNull(AttributeKey<String>("userId"))
    println("Processing for user: $userId")
    next()
}
```

---

## Retryable Consumer

A consumer with advanced retry mechanisms for handling failures.

### Concept
Real-world message processing often encounters transient errors (network issues, temporary service unavailability). A retryable consumer implements sophisticated retry strategies with in-memory retries for fast recovery and non-blocking retries for longer delays.

### Use Cases
- Transient network failures
- Database connection issues
- Downstream service temporary unavailability
- Rate-limited API calls
- Business logic that may need retry

### Retry Strategies

#### 1. Single Topic Retry
All retry attempts go to a single retry topic.

```kotlin
TopicConfiguration(
    topic = "orders.v1",
    retry = TopicConfiguration.TopicRetryStrategy.SingleTopicRetry(
        retryTopic = "orders.v1.retry",
        maxOverallAttempts = 3
    ),
    deadLetterTopic = "orders.v1.error"
)
```

#### 2. Exponential Backoff Multi-Topic Retry
Different retry topics with increasing delays.

```kotlin
TopicConfiguration(
    topic = "orders.v1",
    retry = TopicConfiguration.TopicRetryStrategy.ExponentialBackoffMultiTopicRetry(
        maxOverallAttempts = 5,
        delayTopics = listOf(
            TopicConfiguration.TopicRetryStrategy.DelayTopicConfiguration("orders.v1.retry.10s", 10.seconds),
            TopicConfiguration.TopicRetryStrategy.DelayTopicConfiguration("orders.v1.retry.30s", 30.seconds),
            TopicConfiguration.TopicRetryStrategy.DelayTopicConfiguration("orders.v1.retry.1m", 1.minutes),
            TopicConfiguration.TopicRetryStrategy.DelayTopicConfiguration("orders.v1.retry.5m", 5.minutes)
        )
    ),
    deadLetterTopic = "orders.v1.error"
)
```

#### 3. No Retry Strategy
Directly send to dead letter topic on failure.

```kotlin
TopicConfiguration(
    topic = "orders.v1",
    retry = TopicConfiguration.TopicRetryStrategy.NoneStrategy,
    deadLetterTopic = "orders.v1.error"
)
```

### Example

```kotlin
package com.trendyol.quafka.examples.console.consumers.single

import com.trendyol.quafka.consumer.configuration.*
import com.trendyol.quafka.extensions.errorHandling.configuration.subscribeWithErrorHandling
import com.trendyol.quafka.extensions.errorHandling.recoverer.*
import com.trendyol.quafka.extensions.delaying.MessageDelayer
import com.trendyol.quafka.producer.QuafkaProducer
import org.apache.kafka.common.serialization.StringDeserializer
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds

class RetryableConsumer(
    private val quafkaProducer: QuafkaProducer<String, String>,
    servers: String
) {
    val topic1 = TopicConfiguration(
        topic = "orders.v1",
        retry = TopicConfiguration.TopicRetryStrategy.ExponentialBackoffMultiTopicRetry(
            maxOverallAttempts = 5,
            delayTopics = listOf(
                TopicConfiguration.TopicRetryStrategy.DelayTopicConfiguration("orders.v1.retry.10s", 10.seconds),
                TopicConfiguration.TopicRetryStrategy.DelayTopicConfiguration("orders.v1.retry.30s", 30.seconds),
                TopicConfiguration.TopicRetryStrategy.DelayTopicConfiguration("orders.v1.retry.1m", 1.minutes),
                TopicConfiguration.TopicRetryStrategy.DelayTopicConfiguration("orders.v1.retry.5m", 5.minutes)
            )
        ),
        deadLetterTopic = "orders.v1.error"
    )
    
    fun run() {
        val properties = mutableMapOf<String, Any>(
            "bootstrap.servers" to servers,
            "group.id" to "retryable-consumer-group"
        )
        
        val deserializer = StringDeserializer()
        val danglingTopic = "orders.dangling"
        
        val consumer = QuafkaConsumerBuilder<String, String>(properties)
            .withDeserializer(deserializer, deserializer)
            .subscribeWithErrorHandling(listOf(topic1), danglingTopic, quafkaProducer) {
                this
                    .withRecoverable {
                        withExceptionDetailsProvider { throwable: Throwable ->
                            ExceptionDetails(throwable, throwable.message ?: "Unknown error")
                        }
                        .withMessageDelayer(MessageDelayer())
                        .withPolicyProvider { message, consumerContext, exceptionDetails ->
                            when (exceptionDetails.exception) {
                                // Retry transient errors
                                is DatabaseConcurrencyException -> RetryPolicy.FullRetry(
                                    identifier = "DatabaseConcurrencyException",
                                    inMemoryConfig = InMemoryRetryConfig.basic(
                                        maxAttempts = 3,
                                        initialDelay = 100.milliseconds
                                    ),
                                    nonBlockingConfig = NonBlockingRetryConfig.exponentialRandomBackoff(
                                        maxAttempts = 5,
                                        initialDelay = 1.seconds,
                                        maxDelay = 60.seconds
                                    )
                                )
                                
                                // Don't retry business errors
                                is DomainException -> RetryPolicy.NoRetry
                                
                                // Default: in-memory only
                                else -> RetryPolicy.InMemoryOnly(
                                    identifier = "DefaultRetry",
                                    config = InMemoryRetryConfig.basic(
                                        maxAttempts = 3,
                                        initialDelay = 100.milliseconds
                                    )
                                )
                            }
                        }
                        .withOutgoingMessageModifier { message, consumerContext, exceptionDetails ->
                            // Add custom headers to retry messages
                            val newHeaders = this.headers.toMutableList()
                            newHeaders.add(header("X-Error-Type", exceptionDetails.exception::class.simpleName))
                            newHeaders.add(header("X-Retry-Timestamp", System.currentTimeMillis()))
                            this.copy(headers = newHeaders)
                        }
                    }
                    .withSingleMessageHandler { incomingMessage, consumerContext ->
                        when {
                            topic1.isSuitableTopic(incomingMessage.topic) -> {
                                // Your business logic that may throw exceptions
                                processOrder(incomingMessage.value)
                            }
                        }
                    }
                    .withBackpressure(20)
                    .autoAckAfterProcess(true)
            }.build()
        
        consumer.start()
    }
    
    private fun processOrder(value: String) {
        // Your business logic here
    }
}

// Custom exceptions
class DatabaseConcurrencyException(message: String) : Exception(message)
class DomainException(message: String) : Exception(message)
```

### Retry Flow

```
Original Topic (orders.v1)
    ↓ (failure)
In-Memory Retry (3 attempts, fast)
    ↓ (still failing)
Retry Topic 1 (orders.v1.retry.10s) - wait 10s
    ↓ (failure)
Retry Topic 2 (orders.v1.retry.30s) - wait 30s
    ↓ (failure)
Retry Topic 3 (orders.v1.retry.1m) - wait 1m
    ↓ (failure)
Retry Topic 4 (orders.v1.retry.5m) - wait 5m
    ↓ (still failing)
Dead Letter Topic (orders.v1.error)
```

### Retry Policies

#### FullRetry
Combines in-memory (fast) and non-blocking (delayed) retries.

```kotlin
RetryPolicy.FullRetry(
    identifier = "DatabaseConcurrencyException",
    inMemoryConfig = InMemoryRetryConfig.basic(maxAttempts = 3, initialDelay = 100.milliseconds),
    nonBlockingConfig = NonBlockingRetryConfig.exponentialRandomBackoff(
        maxAttempts = 5,
        initialDelay = 1.seconds,
        maxDelay = 60.seconds
    )
)
```

#### InMemoryOnly
Only retries in memory, no delay topics.

```kotlin
RetryPolicy.InMemoryOnly(
    identifier = "FastRetry",
    config = InMemoryRetryConfig.basic(maxAttempts = 3, initialDelay = 100.milliseconds)
)
```

#### NoRetry
Immediately send to dead letter topic.

```kotlin
RetryPolicy.NoRetry
```

### Key Points
- **In-Memory Retries**: Fast retries without producing to Kafka
- **Non-Blocking Retries**: Delayed retries using separate topics
- **Exception-Based Policies**: Different retry strategies per exception type
- **Message Modification**: Add headers or modify messages during retry
- **Dangling Topic**: Catches messages that don't match any configured topic
- **Dead Letter Topic**: Final destination for permanently failed messages

---

## Custom Middleware

Creating reusable middleware components for your consumers.

### Concept
Custom middleware allows you to encapsulate common processing logic into reusable components. This promotes code reuse, testability, and maintainability across your application.

### Example

```kotlin
import com.trendyol.quafka.extensions.consumer.single.pipelines.*
import com.trendyol.quafka.extensions.common.pipelines.*
import org.slf4j.Logger
import kotlin.system.measureTimeMillis

// Performance measurement middleware
class MeasureMiddleware<TEnvelope, TKey, TValue>(
    private val logger: Logger
) : SingleMessageBaseMiddleware<TEnvelope, TKey, TValue>() 
    where TEnvelope : SingleMessageEnvelope<TKey, TValue> {
    
    override suspend fun execute(
        envelope: TEnvelope,
        next: MiddlewareFn<TEnvelope>
    ) {
        val ms = measureTimeMillis {
            next(envelope)
        }
        logger.info("Message took $ms ms to process")
    }
}

// Validation middleware
class ValidationMiddleware<TEnvelope, TKey, TValue>(
    private val validator: (TValue) -> Boolean
) : SingleMessageBaseMiddleware<TEnvelope, TKey, TValue>() 
    where TEnvelope : SingleMessageEnvelope<TKey, TValue> {
    
    override suspend fun execute(
        envelope: TEnvelope,
        next: MiddlewareFn<TEnvelope>
    ) {
        if (validator(envelope.message.value)) {
            next(envelope)
        } else {
            // Skip processing but acknowledge to avoid reprocessing
            envelope.message.ack()
        }
    }
}

// Enrichment middleware
class EnrichmentMiddleware<TEnvelope, TKey>(
    private val enrichmentService: EnrichmentService
) : SingleMessageBaseMiddleware<TEnvelope, TKey, String>() 
    where TEnvelope : SingleMessageEnvelope<TKey, String> {
    
    override suspend fun execute(
        envelope: TEnvelope,
        next: MiddlewareFn<TEnvelope>
    ) {
        // Fetch additional data
        val enrichedData = enrichmentService.enrich(envelope.message.value)
        
        // Store in attributes for downstream middleware
        envelope.attributes.put(
            AttributeKey("enrichedData"),
            enrichedData
        )
        
        next(envelope)
    }
}

// Usage
class ConsumerWithCustomMiddleware(servers: String) {
    fun run() {
        val consumer = QuafkaConsumerBuilder<String, String>(properties)
            .withDeserializer(StringDeserializer(), StringDeserializer())
            .subscribe("topic-example") {
                usePipelineMessageHandler {
                    // Add custom middleware
                    useMiddleware(MeasureMiddleware(logger))
                    useMiddleware(ValidationMiddleware { value -> value.isNotBlank() })
                    useMiddleware(EnrichmentMiddleware(enrichmentService))
                    
                    // Business logic
                    use { envelope, next ->
                        val enrichedData = envelope.attributes.get(AttributeKey("enrichedData"))
                        processWithEnrichedData(envelope.message.value, enrichedData)
                        next()
                    }
                }.autoAckAfterProcess(true)
            }.build()
        
        consumer.start()
    }
}
```

### Best Practices
1. **Single Responsibility**: Each middleware should do one thing well
2. **Error Handling**: Properly handle and log errors within middleware
3. **Performance**: Be mindful of performance impact, especially for I/O operations
4. **Attributes**: Use attributes to pass data between middleware
5. **Reusability**: Design middleware to be reusable across different consumers
6. **Testing**: Write unit tests for each middleware independently


