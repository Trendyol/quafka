# Batch Pipeline Message Handler

This documentation explains the pipeline structure for Quafka batch message processing.

## Overview

`PipelineBatchMessageHandler` allows you to process batch messages through middlewares. This structure is the batch version of the single message pipeline structure and uses the same principles.

## Core Components

### 1. BatchMessageEnvelope
Wrapper class containing batch messages and related metadata:

```kotlin
open class BatchMessageEnvelope<TKey, TValue>(
    val messages: Collection<IncomingMessage<TKey, TValue>>,
    val consumerContext: ConsumerContext,
    val attributes: Attributes = Attributes()
)
```

### 2. BatchMessageBaseMiddleware
Used to create custom middleware for batch messages:

```kotlin
abstract class BatchMessageBaseMiddleware<TEnvelope, TKey, TValue> : 
    Middleware<TEnvelope> where TEnvelope : BatchMessageEnvelope<*, *>
```

### 3. PipelineBatchMessageHandler
Handler that processes batch messages through the pipeline:

```kotlin
class PipelineBatchMessageHandler<TKey, TValue>(
    val pipeline: Pipeline<BatchMessageEnvelope<TKey, TValue>>
) : BatchMessageHandler<TKey, TValue>
```

## Usage Examples

### Basic Usage

```kotlin
import com.trendyol.quafka.extensions.consumer.batch.pipelines.PipelineBatchMessageHandler.Companion.usePipelineBatchMessageHandler

QuafkaConsumerBuilder<String, String>(properties)
    .withDeserializer(deserializer, deserializer)
    .subscribe("topic-example") {
        usePipelineBatchMessageHandler {
            // Middleware 1: Logging
            use { envelope: BatchMessageEnvelope<String, String>, next ->
                logger.info("Processing batch of ${envelope.messages.size} messages")
                next()
            }
            
            // Middleware 2: Business logic
            use { envelope: BatchMessageEnvelope<String, String>, next ->
                for (message in envelope.messages) {
                    // Process each message
                }
                next()
            }
            
            // Middleware 3: Acknowledgment
            use { envelope: BatchMessageEnvelope<String, String>, next ->
                envelope.messages.ackAll()
                next()
            }
        }
    }
    .build()
```

### Advanced Usage (with Custom Middlewares)

```kotlin
usePipelineBatchMessageHandler {
    // Time measurement
    useMiddleware(MeasureBatchMiddleware())
    
    // Logging
    useMiddleware(LoggingBatchMiddleware(logger))
    
    // Filtering
    useMiddleware(FilterBatchMiddleware { message ->
        message.header("type") != null
    })
    
    // Custom operations
    use { envelope: BatchMessageEnvelope<String, String>, next ->
        // Your business logic
        next()
    }
}
```

## Built-in Middlewares

### SingleMessagePipelineAdapter 🆕
**Processes each message in the batch through a single message pipeline**

This special adapter middleware allows you to process each message separately through a single message pipeline within a batch pipeline. This way, you can reuse your single message middlewares in batch processing.

**Features:**
- Each message passes through the single message pipeline individually
- Batch and single message attributes can be shared
- Single message attributes can be collected back to the batch envelope

**Basic Usage:**

```kotlin
usePipelineBatchMessageHandler {
    // Batch-level middleware
    use { envelope, next ->
        logger.info("Processing batch of ${envelope.messages.size}")
        next()
    }
    
    // Process each message through single message pipeline
    useSingleMessagePipeline {
        // These middlewares run for each message separately
        use { envelope, next ->
            logger.info("Processing message: ${envelope.message.value}")
            // Validation, transformation, etc.
            next()
        }
        
        use { envelope, next ->
            // Business logic
            envelope.message.ack()
            next()
        }
    }
    
    // Batch-level finalization
    use { envelope, next ->
        logger.info("Batch completed")
        next()
    }
}
```

**Attribute Sharing:**

```kotlin
usePipelineBatchMessageHandler {
    // Add batch-level metadata
    use { envelope, next ->
        envelope.attributes.put(AttributeKey<String>("batchId"), "batch-123")
        next()
    }
    
    // Share attributes to single message pipeline
    useSingleMessagePipeline(shareAttributes = true) {
        use { envelope, next ->
            // Can access batch batchId
            val batchId = envelope.attributes.getOrNull(AttributeKey<String>("batchId"))
            logger.info("Message in batch: $batchId")
            next()
        }
    }
}
```

**Parameters:**
- `shareAttributes: Boolean = false` - Copies batch attributes to single message envelopes
- `collectAttributes: Boolean = false` - Collects single message attributes back to batch envelope

### MeasureBatchMiddleware
Measures and logs batch processing time:

```kotlin
useMiddleware(MeasureBatchMiddleware())
```

### LoggingBatchMiddleware
Adds batch processing start and end logs:

```kotlin
useMiddleware(LoggingBatchMiddleware(logger))
```

### FilterBatchMiddleware
Filters messages based on specific criteria:

```kotlin
useMiddleware(FilterBatchMiddleware { message ->
    message.header("type") == "order"
})
```

## Using Attributes

You can use attributes for data sharing between middlewares during batch processing:

```kotlin
use { envelope: BatchMessageEnvelope<String, String>, next ->
    // Save data
    envelope.attributes.put(
        AttributeKey<String>("batchId"),
        UUID.randomUUID().toString()
    )
    next()
}

use { envelope: BatchMessageEnvelope<String, String>, next ->
    // Read data
    val batchId = envelope.attributes.getOrNull(
        AttributeKey<String>("batchId")
    )
    logger.info("Processing batch: $batchId")
    next()
}
```

## Creating Custom Middleware

To create your own middleware:

```kotlin
class CustomBatchMiddleware<TKey, TValue> : 
    BatchMessageBaseMiddleware<BatchMessageEnvelope<TKey, TValue>, TKey, TValue>() {
    
    override suspend fun execute(
        envelope: BatchMessageEnvelope<TKey, TValue>,
        next: suspend (BatchMessageEnvelope<TKey, TValue>) -> Unit
    ) {
        // Pre-processing
        logger.info("Before processing batch")
        
        try {
            next(envelope)
            // Post-processing success
            logger.info("After processing batch - success")
        } catch (e: Exception) {
            // Post-processing error
            logger.error("After processing batch - error", e)
            throw e
        }
    }
}
```

## Single vs Batch Pipeline Comparison

| Feature | Single Pipeline | Batch Pipeline |
|---------|----------------|----------------|
| Envelope | `SingleMessageEnvelope` | `BatchMessageEnvelope` |
| Handler | `PipelineMessageHandler` | `PipelineBatchMessageHandler` |
| Base Middleware | `SingleMessageBaseMiddleware` | `BatchMessageBaseMiddleware` |
| Extension Function | `usePipelineMessageHandler` | `usePipelineBatchMessageHandler` |
| Processed Data | Single message | Message collection |

## Best Practices

1. **Middleware Order**: Add middlewares in logical order (logging → validation → processing → acknowledgment)
2. **Error Handling**: Add appropriate error handling in each middleware
3. **Performance**: Add performance measurement for batch processing (MeasureBatchMiddleware)
4. **Attributes**: Use attributes for data sharing between middlewares
5. **Immutability**: Create a new envelope instead of modifying the existing one

## Using Single Message Pipeline within Batch

Thanks to `SingleMessagePipelineAdapter`, you can use your single message middlewares during batch processing:

```kotlin
usePipelineBatchMessageHandler {
    // 1. Batch start
    use { envelope, next ->
        logger.info("=== Batch Start: ${envelope.messages.size} messages ===")
        next()
    }
    
    // 2. Process each message through single message pipeline
    useSingleMessagePipeline {
        // Validation middleware
        use { envelope, next ->
            if (envelope.message.value.isNotBlank()) {
                next()
            } else {
                logger.warn("Invalid message skipped")
            }
        }
        
        // Processing middleware
        use { envelope, next ->
            processMessage(envelope.message)
            next()
        }
        
        // Acknowledgment middleware
        use { envelope, next ->
            envelope.message.ack()
            next()
        }
    }
    
    // 3. Batch end
    use { envelope, next ->
        logger.info("=== Batch Complete ===")
        next()
    }
}
```

## Use Cases

### 1. Using Existing Single Message Middlewares in Batch
If you have middlewares written for single messages, you can use them in batch processing:

```kotlin
// Existing single message pipeline
val validationPipeline = PipelineBuilder<SingleMessageEnvelope<String, String>>()
    .use { envelope, next -> /* validation */ next() }
    .build()

// Use within batch pipeline
usePipelineBatchMessageHandler {
    useMiddleware(SingleMessagePipelineAdapter(validationPipeline))
}
```

### 2. Single Message Processing with Batch Context
Collecting information at batch level and passing it to each message:

```kotlin
usePipelineBatchMessageHandler {
    // Batch metadata
    use { envelope, next ->
        val stats = envelope.attributes.computeIfAbsent(AttributeKey("stats")) {
            BatchStatistics()
        }
        next()
    }
    
    // Each message can access batch context
    useSingleMessagePipeline(shareAttributes = true) {
        use { envelope, next ->
            val stats = envelope.attributes.get(AttributeKey<BatchStatistics>("stats"))
            stats.incrementProcessed()
            next()
        }
    }
}
```

### 3. Per-Message and Batch-Level Acknowledgment
Acknowledging messages individually and committing at batch end:

```kotlin
usePipelineBatchMessageHandler {
    useSingleMessagePipeline {
        use { envelope, next ->
            processMessage(envelope.message)
            envelope.message.ack()  // Per-message ack
            next()
        }
    }
    
    use { envelope, next ->
        envelope.messages.commitAll()  // Batch commit
        next()
    }
}
```

## Example Projects

For more examples, see:
- `BasicBatchConsumer.kt` - Basic batch usage
- `BatchWithSingleMessagePipelineConsumer.kt` - Single message pipeline adapter usage
- `AdvancedBatchWithSingleMessagePipelineConsumer.kt` - Attribute sharing examples

