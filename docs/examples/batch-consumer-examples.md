# Batch Consumer Examples

This document provides comprehensive examples for batch message consumer patterns in Quafka.

## Table of Contents
- [Basic Batch Consumer](#basic-batch-consumer)
- [Pipelined Batch Consumer](#pipelined-batch-consumer)
- [Advanced Pipelined Batch Consumer](#advanced-pipelined-batch-consumer)
- [Batch with Single Message Pipeline](#batch-with-single-message-pipeline)
- [Advanced Batch with Single Message Pipeline](#advanced-batch-with-single-message-pipeline)
- [Parallel Batch Processing](#parallel-batch-processing)
- [Flexible Batch Processing](#flexible-batch-processing)
- [Concurrent with Attributes](#concurrent-with-attributes)
- [Custom Batch Middleware](#custom-batch-middleware)

---

## Basic Batch Consumer

The simplest form of a batch consumer that processes multiple messages together.

### Concept
A basic batch consumer receives a collection of messages from Kafka and processes them as a batch. This is more efficient than processing messages one by one when you can optimize batch operations (like database bulk inserts).

### Use Cases
- Bulk database operations (INSERT, UPDATE, DELETE)
- Batch API calls to external services
- Aggregating data before processing
- Reducing overhead of per-message operations
- Optimizing throughput

### Example

```kotlin
package com.trendyol.quafka.examples.console.consumers.batch

import com.trendyol.quafka.consumer.ackAll
import com.trendyol.quafka.consumer.configuration.QuafkaConsumerBuilder
import org.apache.kafka.common.serialization.StringDeserializer

class BasicBatchConsumer(servers: String) {
    fun run() {
        val properties = mutableMapOf<String, Any>(
            "bootstrap.servers" to servers,
            "group.id" to "basic-batch-consumer-group",
            "auto.offset.reset" to "earliest",
            "max.poll.records" to "500" // Kafka will fetch up to 500 records per poll
        )
        
        val deserializer = StringDeserializer()
        val consumer = QuafkaConsumerBuilder<String, String>(properties)
            .withDeserializer(deserializer, deserializer)
            .subscribe("topic-example") {
                withBatchMessageHandler { incomingMessages, consumerContext ->
                    println("=== Processing batch of ${incomingMessages.size} messages ===")
                    
                    // Collect all values for bulk operation
                    val messagesToProcess = mutableListOf<String>()
                    
                    for (incomingMessage in incomingMessages) {
                        println("Message offset: ${incomingMessage.offset}, value: ${incomingMessage.value}")
                        messagesToProcess.add(incomingMessage.value)
                    }
                    
                    // Perform bulk operation
                    // Example: database.bulkInsert(messagesToProcess)
                    processBatch(messagesToProcess)
                    
                    // Acknowledge all messages at once
                    incomingMessages.ackAll()
                    
                    println("=== Batch processing completed ===")
                }
            }.build()
        
        consumer.start()
    }
    
    private fun processBatch(messages: List<String>) {
        // Your bulk processing logic here
        println("Bulk processing ${messages.size} messages")
    }
}
```

### Key Points
- **Batch Size**: Controlled by Kafka's `max.poll.records` configuration
- **Batch Processing**: Process multiple messages in a single operation
- **Bulk Acknowledgment**: Use `ackAll()` to acknowledge all messages at once
- **Efficiency**: Significantly faster than single message processing for bulk operations

### Performance Benefits
- Fewer database connections/transactions
- Reduced network overhead
- Better throughput for high-volume scenarios
- More efficient resource utilization

---

## Pipelined Batch Consumer

A batch consumer using the pipeline pattern for modular processing.

### Concept
Pipeline pattern for batch processing allows you to compose batch processing logic from multiple middleware components. Each middleware can operate on the entire batch, individual messages, or share context between stages.

### Use Cases
- Logging and monitoring batch operations
- Filtering unwanted messages from batch
- Transforming batch data
- Batch validation
- Metrics collection for batches

### Example

```kotlin
package com.trendyol.quafka.examples.console.consumers.batch

import com.trendyol.quafka.consumer.configuration.*
import com.trendyol.quafka.extensions.consumer.batch.pipelines.*
import com.trendyol.quafka.extensions.consumer.batch.pipelines.PipelineBatchMessageHandler.Companion.usePipelineBatchMessageHandler
import org.apache.kafka.common.serialization.StringDeserializer

class PipelinedBatchConsumer(servers: String) {
    fun run() {
        val properties = mutableMapOf<String, Any>(
            "bootstrap.servers" to servers,
            "group.id" to "pipelined-batch-consumer-group",
            "max.poll.records" to "100"
        )
        
        val deserializer = StringDeserializer()
        val consumer = QuafkaConsumerBuilder<String, String>(properties)
            .withDeserializer(deserializer, deserializer)
            .subscribe("topic-example") {
                usePipelineBatchMessageHandler {
                    // Middleware 1: Logging
                    use { envelope: BatchMessageEnvelope<String, String>, next ->
                        println("=== Starting batch processing ===")
                        println("Batch size: ${envelope.messages.size}")
                        val startTime = System.currentTimeMillis()
                        
                        next()
                        
                        val duration = System.currentTimeMillis() - startTime
                        println("=== Batch completed in ${duration}ms ===")
                    }
                    
                    // Middleware 2: Business logic
                    use { envelope: BatchMessageEnvelope<String, String>, next ->
                        for (incomingMessage in envelope.messages) {
                            println("Processing message: ${incomingMessage.value}")
                            // Your business logic here
                        }
                        next()
                    }
                    
                    // Middleware 3: Acknowledgment
                    use { envelope: BatchMessageEnvelope<String, String>, next ->
                        envelope.messages.ackAll()
                        println("Acknowledged ${envelope.messages.size} messages")
                        next()
                    }
                }
            }.build()
        
        consumer.start()
    }
}
```

### Pipeline Execution Flow

```
Middleware 1 (Start) → Log batch start
    ↓
Middleware 2 → Process messages
    ↓
Middleware 3 → Acknowledge messages
    ↓
Middleware 2 (Complete)
    ↓
Middleware 1 (Complete) → Log batch completion
```

### Key Points
- **Middleware Order**: Executes in the order they are added
- **Pre/Post Processing**: Code before `next()` runs before, code after runs after
- **Modularity**: Each middleware has a single responsibility
- **Composability**: Easy to add, remove, or reorder middleware

---

## Advanced Pipelined Batch Consumer

A batch consumer with custom middleware and attribute sharing.

### Concept
Advanced pipeline usage demonstrates how to use custom middleware, filter messages, measure performance, and share context between middleware using attributes.

### Use Cases
- Complex batch processing workflows
- Message filtering and routing
- Performance monitoring
- Batch metadata tracking
- Context sharing across processing stages

### Example

```kotlin
package com.trendyol.quafka.examples.console.consumers.batch

import com.trendyol.quafka.consumer.configuration.*
import com.trendyol.quafka.extensions.consumer.batch.pipelines.*
import com.trendyol.quafka.extensions.consumer.batch.pipelines.PipelineBatchMessageHandler.Companion.usePipelineBatchMessageHandler
import com.trendyol.quafka.extensions.common.pipelines.*
import org.apache.kafka.common.serialization.StringDeserializer

class AdvancedPipelinedBatchConsumer(servers: String) {
    fun run() {
        val properties = mutableMapOf<String, Any>(
            "bootstrap.servers" to servers,
            "group.id" to "advanced-batch-consumer-group"
        )
        
        val deserializer = StringDeserializer()
        val consumer = QuafkaConsumerBuilder<String, String>(properties)
            .withDeserializer(deserializer, deserializer)
            .subscribe("topic-example") {
                usePipelineBatchMessageHandler {
                    // Middleware 1: Measure execution time
                    useMiddleware(MeasureBatchMiddleware())
                    
                    // Middleware 2: Logging
                    useMiddleware(LoggingBatchMiddleware(logger))
                    
                    // Middleware 3: Filter messages (only messages with specific header)
                    useMiddleware(
                        FilterBatchMiddleware { message ->
                            message.headers.get("type")?.asString() != null
                        }
                    )
                    
                    // Middleware 4: Store batch metadata
                    use { envelope: BatchMessageEnvelope<String, String>, next ->
                        val batchId = envelope.messages.firstOrNull()?.offset?.toString() ?: "unknown"
                        envelope.attributes.put(AttributeKey("batchId"), batchId)
                        envelope.attributes.put(AttributeKey("originalSize"), envelope.messages.size)
                        next()
                    }
                    
                    // Middleware 5: Business logic with context
                    use { envelope: BatchMessageEnvelope<String, String>, next ->
                        val batchId = envelope.attributes.getOrNull(AttributeKey<String>("batchId"))
                        val originalSize = envelope.attributes.getOrNull(AttributeKey<Int>("originalSize"))
                        
                        println("Processing batch $batchId (original size: $originalSize, filtered: ${envelope.messages.size})")
                        
                        for (incomingMessage in envelope.messages) {
                            // Your business logic here
                            processMessage(incomingMessage.value)
                        }
                        next()
                    }
                    
                    // Middleware 6: Acknowledgment
                    use { envelope: BatchMessageEnvelope<String, String>, next ->
                        envelope.messages.ackAll()
                        println("Acknowledged ${envelope.messages.size} messages")
                        next()
                    }
                }
            }.build()
        
        consumer.start()
    }
}
```

### Custom Middleware Examples

#### MeasureBatchMiddleware
```kotlin
class MeasureBatchMiddleware<TKey, TValue> : 
    BatchMessageBaseMiddleware<BatchMessageEnvelope<TKey, TValue>, TKey, TValue>() {
    
    private val logger: Logger = LoggerFactory.getLogger(this::class.java)
    
    override suspend fun execute(
        envelope: BatchMessageEnvelope<TKey, TValue>,
        next: suspend (BatchMessageEnvelope<TKey, TValue>) -> Unit
    ) {
        val batchSize = envelope.messages.size
        val elapsed = measureTimeMillis {
            next(envelope)
        }
        logger.info("Batch processing: $batchSize messages in $elapsed ms (${elapsed / batchSize} ms/msg)")
    }
}
```

#### LoggingBatchMiddleware
```kotlin
class LoggingBatchMiddleware<TKey, TValue>(
    private val logger: Logger
) : BatchMessageBaseMiddleware<BatchMessageEnvelope<TKey, TValue>, TKey, TValue>() {
    
    override suspend fun execute(
        envelope: BatchMessageEnvelope<TKey, TValue>,
        next: suspend (BatchMessageEnvelope<TKey, TValue>) -> Unit
    ) {
        logger.info("Starting batch: ${envelope.messages.size} messages")
        try {
            next(envelope)
            logger.info("Successfully processed batch")
        } catch (e: Throwable) {
            logger.error("Error processing batch", e)
            throw e
        }
    }
}
```

#### FilterBatchMiddleware
```kotlin
class FilterBatchMiddleware<TKey, TValue>(
    private val predicate: suspend (IncomingMessage<TKey, TValue>) -> Boolean
) : BatchMessageBaseMiddleware<BatchMessageEnvelope<TKey, TValue>, TKey, TValue>() {
    
    override suspend fun execute(
        envelope: BatchMessageEnvelope<TKey, TValue>,
        next: suspend (BatchMessageEnvelope<TKey, TValue>) -> Unit
    ) {
        val filteredMessages = envelope.messages.filter { predicate(it) }
        
        if (filteredMessages.isNotEmpty()) {
            val filteredEnvelope = BatchMessageEnvelope(
                filteredMessages,
                envelope.consumerContext,
                envelope.attributes
            )
            next(filteredEnvelope)
        }
    }
}
```

### Key Points
- **Custom Middleware**: Encapsulate reusable logic
- **Filtering**: Remove unwanted messages from batch
- **Attributes**: Share context between middleware
- **Performance Monitoring**: Track batch processing metrics
- **Error Handling**: Proper exception handling in middleware

---

## Batch with Single Message Pipeline

Process each message in a batch through a single message pipeline.

### Concept
This powerful adapter allows you to process each message in a batch individually through a single message pipeline. This combines the efficiency of batch fetching with the flexibility of per-message processing logic.

### Use Cases
- Reusing single message middleware in batch context
- Per-message validation within a batch
- Individual message transformation
- Per-message error handling
- Sequential processing of batch items

### Example

```kotlin
package com.trendyol.quafka.examples.console.consumers.batch

import com.trendyol.quafka.consumer.configuration.*
import com.trendyol.quafka.extensions.consumer.batch.pipelines.PipelineBatchMessageHandler.Companion.usePipelineBatchMessageHandler
import com.trendyol.quafka.extensions.consumer.batch.pipelines.middlewares.*
import org.apache.kafka.common.serialization.StringDeserializer

class BatchWithSingleMessagePipelineConsumer(servers: String) {
    fun run() {
        val properties = mutableMapOf<String, Any>(
            "bootstrap.servers" to servers,
            "group.id" to "batch-single-pipeline-consumer-group"
        )
        
        val deserializer = StringDeserializer()
        val consumer = QuafkaConsumerBuilder<String, String>(properties)
            .withDeserializer(deserializer, deserializer)
            .subscribe("topic-example") {
                usePipelineBatchMessageHandler {
                    // Middleware 1: Batch-level logging
                    use { envelope, next ->
                        println("=== Starting batch of ${envelope.messages.size} messages ===")
                        next()
                        println("=== Completed batch of ${envelope.messages.size} messages ===")
                    }
                    
                    // Middleware 2: Process each message individually
                    useSingleMessagePipeline(
                        shareAttributes = false,
                        collectAttributes = false
                    ) {
                        // This runs for EACH message individually
                        
                        // Single message middleware 1: Validation
                        use { envelope, next ->
                            val message = envelope.message
                            if (message.value.isNotBlank()) {
                                println("Valid message: ${message.value}")
                                next()
                            } else {
                                println("Invalid message: empty value, skipping")
                                // Don't call next() to skip processing this message
                            }
                        }
                        
                        // Single message middleware 2: Business logic
                        use { envelope, next ->
                            val message = envelope.message
                            println("Processing: offset=${message.offset}, value=${message.value}")
                            
                            // Your per-message business logic
                            processMessage(message.value)
                            
                            next()
                        }
                        
                        // Single message middleware 3: Per-message acknowledgment
                        use { envelope, next ->
                            envelope.message.ack()
                            next()
                        }
                    }
                    
                    // Middleware 3: Batch-level finalization
                    use { envelope, next ->
                        println("All ${envelope.messages.size} messages processed successfully")
                        next()
                    }
                }
            }.build()
        
        consumer.start()
    }
}
```

### Execution Flow

```
Batch Level:
    Batch Start Middleware
        ↓
    Single Message Pipeline Adapter
        ↓
        For each message:
            Message 1 → Validation → Business Logic → Ack
            Message 2 → Validation → Business Logic → Ack
            Message 3 → Validation → Business Logic → Ack
            ...
        ↓
    Batch Finalization Middleware
```

### Parameters

- **shareAttributes** (default: false): Share batch-level attributes with each message
- **collectAttributes** (default: false): Collect message-level attributes back to batch
- **concurrencyLevel** (default: 1): Process multiple messages concurrently

### Key Points
- **Best of Both Worlds**: Batch efficiency + per-message control
- **Middleware Reuse**: Use existing single message middleware
- **Selective Processing**: Skip invalid messages without failing the batch
- **Per-Message Ack**: Acknowledge messages individually
- **Clean Separation**: Clear distinction between batch and message-level concerns

---

## Advanced Batch with Single Message Pipeline

Advanced usage with attribute sharing between batch and single message pipelines.

### Concept
This demonstrates how to share context between batch-level and message-level processing. Batch metadata (like batch ID, timestamps) can be accessed by individual message processors.

### Use Cases
- Passing batch context to message processors
- Tracking message relationship to batch
- Batch-level transaction management
- Collecting per-message results at batch level
- Coordinated batch and message processing

### Example

```kotlin
package com.trendyol.quafka.examples.console.consumers.batch

import com.trendyol.quafka.consumer.ackAll
import com.trendyol.quafka.consumer.configuration.QuafkaConsumerBuilder
import com.trendyol.quafka.extensions.common.pipelines.AttributeKey
import com.trendyol.quafka.extensions.consumer.batch.pipelines.PipelineBatchMessageHandler.Companion.usePipelineBatchMessageHandler
import com.trendyol.quafka.extensions.consumer.batch.pipelines.middlewares.useSingleMessagePipeline
import org.apache.kafka.common.serialization.StringDeserializer

class AdvancedBatchWithSingleMessagePipelineConsumer(servers: String) {
    fun run() {
        val properties = mutableMapOf<String, Any>(
            "bootstrap.servers" to servers,
            "group.id" to "advanced-batch-single-pipeline-group"
        )
        
        val deserializer = StringDeserializer()
        val consumer = QuafkaConsumerBuilder<String, String>(properties)
            .withDeserializer(deserializer, deserializer)
            .subscribe("topic-example") {
                usePipelineBatchMessageHandler {
                    // Set batch-level metadata
                    use { envelope, next ->
                        val batchIdKey = AttributeKey<String>("batchId")
                        val batchId = "batch-${System.currentTimeMillis()}"
                        envelope.attributes.put(batchIdKey, batchId)
                        
                        val startTimeKey = AttributeKey<Long>("startTime")
                        envelope.attributes.put(startTimeKey, System.currentTimeMillis())
                        
                        println("Processing batch: $batchId")
                        next()
                        
                        val duration = System.currentTimeMillis() - envelope.attributes.get(startTimeKey)
                        println("Batch $batchId completed in ${duration}ms")
                    }
                    
                    // Process each message with access to batch metadata
                    useSingleMessagePipeline(shareAttributes = true) {
                        use { envelope, next ->
                            val batchIdKey = AttributeKey<String>("batchId")
                            val batchId = envelope.attributes.getOrNull(batchIdKey)
                            
                            println("Message offset=${envelope.message.offset} belongs to batch: $batchId")
                            
                            // Process message with batch context
                            // Example: save to database with batch ID for tracking
                            saveToDatabase(
                                message = envelope.message.value,
                                batchId = batchId,
                                offset = envelope.message.offset
                            )
                            
                            next()
                        }
                    }
                    
                    // Final acknowledgment at batch level
                    use { envelope, next ->
                        envelope.messages.ackAll()
                        next()
                    }
                }
            }.build()
        
        consumer.start()
    }
    
    private fun saveToDatabase(message: String, batchId: String?, offset: Long) {
        // Your database logic here
        println("Saving message (batch=$batchId, offset=$offset): $message")
    }
}
```

### Attribute Sharing Patterns

#### Pattern 1: Batch Context to Messages
```kotlin
// Batch level - set context
use { envelope, next ->
    envelope.attributes.put(AttributeKey<String>("userId"), "user123")
    envelope.attributes.put(AttributeKey<String>("sessionId"), "session456")
    next()
}

// Message level - access context
useSingleMessagePipeline(shareAttributes = true) {
    use { envelope, next ->
        val userId = envelope.attributes.getOrNull(AttributeKey<String>("userId"))
        val sessionId = envelope.attributes.getOrNull(AttributeKey<String>("sessionId"))
        processWithContext(envelope.message, userId, sessionId)
        next()
    }
}
```

#### Pattern 2: Collecting Message Results
```kotlin
// Batch level - initialize collector
use { envelope, next ->
    val resultsKey = AttributeKey<MutableList<ProcessingResult>>("results")
    envelope.attributes.put(resultsKey, mutableListOf())
    next()
    
    // After processing, access collected results
    val results = envelope.attributes.get(resultsKey)
    println("Processed ${results.size} messages successfully")
}

// Message level - collect results
useSingleMessagePipeline(shareAttributes = true, collectAttributes = true) {
    use { envelope, next ->
        val result = processMessage(envelope.message)
        
        val resultsKey = AttributeKey<ConcurrentLinkedQueue<ProcessingResult>>("results")
        envelope.attributes.get(resultsKey).add(result)
        next()
    }
}
```

### Key Points
- **Shared Context**: Batch metadata available to all messages
- **Bidirectional Communication**: Both batch→message and message→batch
- **Transaction Coordination**: Use batch context for transaction management
- **Result Collection**: Gather per-message results at batch level
- **Traceability**: Track message relationships to batches

---

## Parallel Batch Processing

Process batch messages concurrently for improved throughput.

### Concept
When individual message processing is I/O bound (database queries, API calls), processing messages in parallel can significantly improve throughput while still benefiting from batch fetching.

### Use Cases
- I/O-bound operations (database, network, file I/O)
- Independent message processing (no ordering requirements)
- High-throughput scenarios
- Reducing overall batch processing time
- Maximizing resource utilization

### Example

```kotlin
package com.trendyol.quafka.examples.console.consumers.batch

import com.trendyol.quafka.consumer.configuration.*
import com.trendyol.quafka.extensions.consumer.batch.pipelines.PipelineBatchMessageHandler.Companion.usePipelineBatchMessageHandler
import com.trendyol.quafka.extensions.consumer.batch.pipelines.middlewares.*
import org.apache.kafka.common.serialization.StringDeserializer

class ParallelBatchProcessingConsumer(servers: String) {
    fun run() {
        val properties = mutableMapOf<String, Any>(
            "bootstrap.servers" to servers,
            "group.id" to "parallel-batch-consumer-group",
            "max.poll.records" to "100"
        )
        
        val deserializer = StringDeserializer()
        val consumer = QuafkaConsumerBuilder<String, String>(properties)
            .withDeserializer(deserializer, deserializer)
            .subscribe("topic-example") {
                usePipelineBatchMessageHandler {
                    // Middleware 1: Batch-level logging
                    use { envelope, next ->
                        val startTime = System.currentTimeMillis()
                        println("=== Starting parallel batch: ${envelope.messages.size} messages ===")
                        
                        next()
                        
                        val duration = System.currentTimeMillis() - startTime
                        println("=== Completed parallel batch in ${duration}ms ===")
                    }
                    
                    // Middleware 2: Process messages concurrently (up to 10 in parallel)
                    useSingleMessagePipeline(concurrencyLevel = 10) {
                        // Single message middleware 1: Validation
                        use { envelope, next ->
                            val message = envelope.message
                            if (message.value.isNotBlank()) {
                                val threadName = Thread.currentThread().name
                                println("[$threadName] Processing: ${message.value}")
                                next()
                            } else {
                                println("Invalid message: empty value")
                            }
                        }
                        
                        // Single message middleware 2: Simulate heavy processing
                        use { envelope, next ->
                            // Simulate API call or database operation
                            delay(100)
                            
                            // Your actual business logic here
                            processMessage(envelope.message.value)
                            
                            println("Processed: ${envelope.message.value}")
                            next()
                        }
                        
                        // Single message middleware 3: Per-message acknowledgment
                        use { envelope, next ->
                            envelope.message.ack()
                            next()
                        }
                    }
                    
                    // Middleware 3: Batch-level finalization
                    use { envelope, next ->
                        println("All ${envelope.messages.size} messages processed in parallel")
                        next()
                    }
                }
            }.build()
        
        consumer.start()
    }
}
```

### Concurrency Model

```
Batch of 100 messages, concurrencyLevel = 10

Thread Pool (10 threads):
    Thread 1: Message 1, 11, 21, 31, ...
    Thread 2: Message 2, 12, 22, 32, ...
    Thread 3: Message 3, 13, 23, 33, ...
    ...
    Thread 10: Message 10, 20, 30, 40, ...

All threads process concurrently
Batch completes when all messages are processed
```

### Performance Comparison

#### Sequential Processing (concurrencyLevel = 1)
```
100 messages × 100ms each = 10,000ms total
```

#### Parallel Processing (concurrencyLevel = 10)
```
100 messages / 10 threads × 100ms each ≈ 1,000ms total
```

**~10x faster for I/O-bound operations!**

### Concurrency Considerations

#### Safe for Parallel Processing
- Database reads
- HTTP API calls
- File I/O operations
- Independent calculations
- Idempotent operations

#### Requires Sequential Processing
- Maintaining message order
- Shared state modifications
- Non-idempotent operations
- Resource contention scenarios

### Key Points
- **I/O Bound**: Best for I/O-bound operations
- **Concurrency Level**: Choose based on your resource capacity
- **Thread Safety**: Ensure your code is thread-safe
- **Order**: Messages may complete out of order
- **Error Handling**: Failed messages don't block others

---

## Flexible Batch Processing

Toggle between sequential and concurrent processing modes.

### Concept
Sometimes you need the flexibility to switch between sequential and parallel processing based on configuration, environment, or runtime conditions.

### Use Cases
- Environment-based processing (dev: sequential, prod: parallel)
- A/B testing different processing strategies
- Dynamic optimization based on load
- Debugging with sequential mode
- Gradual rollout of parallel processing

### Example

```kotlin
package com.trendyol.quafka.examples.console.consumers.batch

import com.trendyol.quafka.consumer.ackAll
import com.trendyol.quafka.consumer.configuration.QuafkaConsumerBuilder
import com.trendyol.quafka.extensions.consumer.batch.pipelines.PipelineBatchMessageHandler.Companion.usePipelineBatchMessageHandler
import com.trendyol.quafka.extensions.consumer.batch.pipelines.middlewares.useSingleMessagePipeline
import org.apache.kafka.common.serialization.StringDeserializer

class FlexibleBatchProcessingConsumer(servers: String) {
    // Configuration - can be loaded from environment, config file, or feature flag
    private val useParallelProcessing = System.getenv("PARALLEL_PROCESSING")?.toBoolean() ?: true
    private val concurrencyLevel = System.getenv("CONCURRENCY_LEVEL")?.toInt() ?: 5
    
    fun run() {
        val properties = mutableMapOf<String, Any>(
            "bootstrap.servers" to servers,
            "group.id" to "flexible-batch-consumer-group"
        )
        
        println("Starting consumer with parallel=$useParallelProcessing, concurrency=$concurrencyLevel")
        
        val deserializer = StringDeserializer()
        val consumer = QuafkaConsumerBuilder<String, String>(properties)
            .withDeserializer(deserializer, deserializer)
            .subscribe("topic-example") {
                usePipelineBatchMessageHandler {
                    // Determine concurrency mode
                    val concurrency = if (useParallelProcessing) concurrencyLevel else 1
                    
                    useSingleMessagePipeline(concurrencyLevel = concurrency) {
                        use { envelope, next ->
                            val mode = if (concurrency > 1) "Concurrent($concurrency)" else "Sequential"
                            val threadName = Thread.currentThread().name
                            
                            println("[$mode - $threadName] Processing: ${envelope.message.value}")
                            
                            // Your business logic
                            processMessage(envelope.message.value)
                            
                            next()
                        }
                    }
                    
                    // Final acknowledgment
                    use { envelope, next ->
                        envelope.messages.ackAll()
                        next()
                    }
                }
            }.build()
        
        consumer.start()
    }
}
```

### Configuration Options

#### Environment Variables
```bash
# Sequential processing
PARALLEL_PROCESSING=false ./consumer

# Parallel with 10 threads
PARALLEL_PROCESSING=true CONCURRENCY_LEVEL=10 ./consumer
```

#### Configuration File
```yaml
consumer:
  parallelProcessing: true
  concurrencyLevel: 5
  batchSize: 100
```

#### Feature Flags
```kotlin
val config = FeatureFlagService.getConsumerConfig("orders-consumer")
val concurrency = if (config.parallelEnabled) config.concurrencyLevel else 1
```

### Key Points
- **Flexibility**: Easy to switch processing modes
- **Configuration-Driven**: External configuration control
- **Testing**: Test both modes easily
- **Gradual Rollout**: Start sequential, move to parallel
- **Environment-Specific**: Different modes per environment

---

## Concurrent with Attributes

Combine parallel processing with batch context sharing.

### Concept
Advanced pattern combining concurrent message processing with batch-level context sharing. Each concurrent worker can access shared batch metadata while processing independently.

### Use Cases
- Parallel processing with shared transaction
- Batch-level rate limiting across concurrent workers
- Collecting metrics from parallel processors
- Coordinated parallel processing
- Batch-level resource pooling

### Example

```kotlin
package com.trendyol.quafka.examples.console.consumers.batch

import com.trendyol.quafka.consumer.ackAll
import com.trendyol.quafka.consumer.configuration.QuafkaConsumerBuilder
import com.trendyol.quafka.extensions.common.pipelines.AttributeKey
import com.trendyol.quafka.extensions.consumer.batch.pipelines.PipelineBatchMessageHandler.Companion.usePipelineBatchMessageHandler
import com.trendyol.quafka.extensions.consumer.batch.pipelines.middlewares.useSingleMessagePipeline
import org.apache.kafka.common.serialization.StringDeserializer
import java.util.concurrent.atomic.AtomicInteger

class ConcurrentWithAttributesConsumer(servers: String) {
    fun run() {
        val properties = mutableMapOf<String, Any>(
            "bootstrap.servers" to servers,
            "group.id" to "concurrent-attributes-consumer-group"
        )
        
        val deserializer = StringDeserializer()
        val consumer = QuafkaConsumerBuilder<String, String>(properties)
            .withDeserializer(deserializer, deserializer)
            .subscribe("topic-example") {
                usePipelineBatchMessageHandler {
                    // Set batch-level context
                    use { envelope, next ->
                        val batchIdKey = AttributeKey<String>("batchId")
                        val startTimeKey = AttributeKey<Long>("startTime")
                        val processedCountKey = AttributeKey<AtomicInteger>("processedCount")
                        
                        val batchId = "batch-${System.currentTimeMillis()}"
                        envelope.attributes.put(batchIdKey, batchId)
                        envelope.attributes.put(startTimeKey, System.currentTimeMillis())
                        envelope.attributes.put(processedCountKey, AtomicInteger(0))
                        
                        println("Starting batch: $batchId with ${envelope.messages.size} messages")
                        
                        next()
                        
                        val duration = System.currentTimeMillis() - envelope.attributes.get(startTimeKey)
                        val processed = envelope.attributes.get(processedCountKey).get()
                        println("Batch $batchId completed: $processed messages in ${duration}ms")
                    }
                    
                    // Process concurrently with shared batch context
                    useSingleMessagePipeline(
                        concurrencyLevel = 10,
                        shareAttributes = true
                    ) {
                        use { envelope, next ->
                            val batchId = envelope.attributes.getOrNull(
                                AttributeKey<String>("batchId")
                            )
                            val processedCount = envelope.attributes.get(
                                AttributeKey<AtomicInteger>("processedCount")
                            )
                            
                            val threadName = Thread.currentThread().name
                            println("[$threadName] [Batch: $batchId] Processing message: ${envelope.message.offset}")
                            
                            // Process with batch context
                            saveToDatabase(
                                message = envelope.message.value,
                                batchId = batchId,
                                offset = envelope.message.offset
                            )
                            
                            // Thread-safe increment
                            val count = processedCount.incrementAndGet()
                            println("[$threadName] Processed $count messages so far")
                            
                            next()
                        }
                    }
                    
                    // Final acknowledgment
                    use { envelope, next ->
                        envelope.messages.ackAll()
                        next()
                    }
                }
            }.build()
        
        consumer.start()
    }
    
    private suspend fun saveToDatabase(message: String, batchId: String?, offset: Long) {
        // Your database logic here
        delay(50) // Simulate I/O
    }
}
```

### Thread-Safe Attributes

When using concurrent processing with shared attributes, ensure thread safety:

#### Thread-Safe Types
```kotlin
// Good: Thread-safe atomic counter
envelope.attributes.put(AttributeKey("counter"), AtomicInteger(0))

// Good: Thread-safe concurrent map
envelope.attributes.put(AttributeKey("cache"), ConcurrentHashMap<String, String>())

// Good: Immutable value
envelope.attributes.put(AttributeKey("batchId"), "batch-123")
```

#### Not Thread-Safe
```kotlin
// Bad: Regular mutable list
envelope.attributes.put(AttributeKey("results"), mutableListOf<Result>())

// Bad: Regular mutable map
envelope.attributes.put(AttributeKey("cache"), mutableMapOf<String, String>())

// Bad: Regular counter
envelope.attributes.put(AttributeKey("counter"), 0)
```

### Key Points
- **Thread Safety**: Use thread-safe types for mutable shared state
- **Atomic Operations**: Use `AtomicInteger`, `AtomicLong`, etc.
- **Concurrent Collections**: Use `ConcurrentHashMap`, etc.
- **Immutable Data**: Prefer immutable shared data
- **Performance**: Balance concurrency vs synchronization overhead

---

## Custom Batch Middleware

Creating reusable middleware components for batch consumers.

### Concept
Custom batch middleware encapsulates common batch processing patterns into reusable components. This promotes code reuse, testability, and maintainability.

### Common Middleware Patterns

#### 1. Performance Measurement Middleware

```kotlin
import com.trendyol.quafka.extensions.consumer.batch.pipelines.*
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import kotlin.system.measureTimeMillis

class MeasureBatchMiddleware<TKey, TValue> : 
    BatchMessageBaseMiddleware<BatchMessageEnvelope<TKey, TValue>, TKey, TValue>() {
    
    private val logger: Logger = LoggerFactory.getLogger(this::class.java)
    
    override suspend fun execute(
        envelope: BatchMessageEnvelope<TKey, TValue>,
        next: suspend (BatchMessageEnvelope<TKey, TValue>) -> Unit
    ) {
        val batchSize = envelope.messages.size
        val elapsed = measureTimeMillis {
            next(envelope)
        }
        
        val avgPerMessage = if (batchSize > 0) elapsed / batchSize else 0
        logger.info("Batch: $batchSize messages in ${elapsed}ms (${avgPerMessage}ms/msg)")
    }
}
```

#### 2. Logging Middleware

```kotlin
class LoggingBatchMiddleware<TKey, TValue>(
    private val logger: Logger
) : BatchMessageBaseMiddleware<BatchMessageEnvelope<TKey, TValue>, TKey, TValue>() {
    
    override suspend fun execute(
        envelope: BatchMessageEnvelope<TKey, TValue>,
        next: suspend (BatchMessageEnvelope<TKey, TValue>) -> Unit
    ) {
        logger.info("Starting batch: ${envelope.messages.size} messages")
        try {
            next(envelope)
            logger.info("Successfully processed batch of ${envelope.messages.size}")
        } catch (e: Throwable) {
            logger.error("Error processing batch of ${envelope.messages.size}", e)
            throw e
        }
    }
}
```

#### 3. Filtering Middleware

```kotlin
class FilterBatchMiddleware<TKey, TValue>(
    private val predicate: suspend (IncomingMessage<TKey, TValue>) -> Boolean
) : BatchMessageBaseMiddleware<BatchMessageEnvelope<TKey, TValue>, TKey, TValue>() {
    
    override suspend fun execute(
        envelope: BatchMessageEnvelope<TKey, TValue>,
        next: suspend (BatchMessageEnvelope<TKey, TValue>) -> Unit
    ) {
        val originalSize = envelope.messages.size
        val filteredMessages = envelope.messages.filter { predicate(it) }
        
        if (filteredMessages.isNotEmpty()) {
            val filteredEnvelope = BatchMessageEnvelope(
                filteredMessages,
                envelope.consumerContext,
                envelope.attributes
            )
            logger.info("Filtered batch: $originalSize → ${filteredMessages.size} messages")
            next(filteredEnvelope)
        } else {
            logger.info("All $originalSize messages filtered out")
        }
    }
}
```

#### 4. Batch Size Limiter Middleware

```kotlin
class BatchSizeLimiterMiddleware<TKey, TValue>(
    private val maxBatchSize: Int
) : BatchMessageBaseMiddleware<BatchMessageEnvelope<TKey, TValue>, TKey, TValue>() {
    
    override suspend fun execute(
        envelope: BatchMessageEnvelope<TKey, TValue>,
        next: suspend (BatchMessageEnvelope<TKey, TValue>) -> Unit
    ) {
        if (envelope.messages.size > maxBatchSize) {
            // Process in chunks
            envelope.messages.chunked(maxBatchSize).forEach { chunk ->
                val chunkEnvelope = BatchMessageEnvelope(
                    chunk,
                    envelope.consumerContext,
                    envelope.attributes
                )
                next(chunkEnvelope)
            }
        } else {
            next(envelope)
        }
    }
}
```

#### 5. Metrics Collection Middleware

```kotlin
class MetricsCollectionMiddleware<TKey, TValue>(
    private val metricsService: MetricsService
) : BatchMessageBaseMiddleware<BatchMessageEnvelope<TKey, TValue>, TKey, TValue>() {
    
    override suspend fun execute(
        envelope: BatchMessageEnvelope<TKey, TValue>,
        next: suspend (BatchMessageEnvelope<TKey, TValue>) -> Unit
    ) {
        val startTime = System.currentTimeMillis()
        metricsService.incrementCounter("batch.started")
        metricsService.recordGauge("batch.size", envelope.messages.size.toDouble())
        
        try {
            next(envelope)
            metricsService.incrementCounter("batch.success")
        } catch (e: Throwable) {
            metricsService.incrementCounter("batch.failure")
            throw e
        } finally {
            val duration = System.currentTimeMillis() - startTime
            metricsService.recordHistogram("batch.duration", duration.toDouble())
        }
    }
}
```

### Usage Example

```kotlin
class ProductionBatchConsumer(servers: String) {
    fun run() {
        val consumer = QuafkaConsumerBuilder<String, String>(properties)
            .withDeserializer(StringDeserializer(), StringDeserializer())
            .subscribe("topic-example") {
                usePipelineBatchMessageHandler {
                    // Add custom middleware
                    useMiddleware(MeasureBatchMiddleware())
                    useMiddleware(LoggingBatchMiddleware(logger))
                    useMiddleware(MetricsCollectionMiddleware(metricsService))
                    useMiddleware(FilterBatchMiddleware { msg -> msg.value.isNotBlank() })
                    useMiddleware(BatchSizeLimiterMiddleware(maxBatchSize = 50))
                    
                    // Business logic
                    use { envelope, next ->
                        processBatch(envelope.messages)
                        next()
                    }
                    
                    // Acknowledgment
                    use { envelope, next ->
                        envelope.messages.ackAll()
                        next()
                    }
                }
            }.build()
        
        consumer.start()
    }
}
```

### Best Practices

1. **Single Responsibility**: Each middleware should do one thing well
2. **Error Handling**: Properly handle and log errors
3. **Performance**: Be mindful of performance impact
4. **Reusability**: Design for reuse across different consumers
5. **Testing**: Write unit tests for each middleware
6. **Documentation**: Document middleware behavior and parameters
7. **Thread Safety**: Ensure thread safety for concurrent processing

---

## Summary

### When to Use Each Pattern

- **Basic Batch**: Simple bulk database operations
- **Pipelined**: Need modular, testable architecture
- **Advanced Pipelined**: Complex processing with filtering/metrics
- **With Single Pipeline**: Reuse existing single message middleware
- **Advanced with Attributes**: Need batch context in message processing
- **Parallel Processing**: I/O-bound operations, high throughput needs
- **Flexible Processing**: Multi-environment deployments
- **Concurrent with Attributes**: Complex parallel scenarios with shared state

### Performance Tips

1. **Batch Size**: Tune `max.poll.records` based on your workload
2. **Concurrency**: Start with low concurrency and increase gradually
3. **I/O Operations**: Use parallel processing for I/O-bound tasks
4. **CPU Operations**: Use sequential processing for CPU-bound tasks
5. **Monitoring**: Always add performance measurement middleware
6. **Testing**: Test both sequential and parallel modes
7. **Resource Limits**: Consider memory, connections, and thread pool limits

