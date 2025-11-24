# Quafka Console Demo Application

An interactive CLI application demonstrating various Quafka producer and consumer patterns.

## Overview

This console application provides a hands-on way to explore and test different Quafka patterns:
- **Producer Examples**: Basic, Batch, JSON, and Header producers
- **Single Consumer Examples**: Basic, Backpressure, Pipeline, and Retryable consumers
- **Batch Consumer Examples**: Various batch processing patterns

## Prerequisites

- Kafka cluster running and accessible
- Java 11 or higher
- Gradle (for building)

## Quick Start

### 1. Start Kafka

If you don't have a Kafka cluster running, you can use Docker:

```bash
docker-compose up -d  # If docker-compose.yml exists
# OR use Confluent Platform, or standalone Kafka
```

### 2. Set Kafka Bootstrap Servers

```bash
export Q_SERVERS="localhost:9092"
```

Or you'll be prompted to enter servers when you run the application.

### 3. Build the Application

```bash
cd examples/console
./gradlew build
```

### 4. Run the Application

```bash
./gradlew run
```

Or with servers as parameter:

```bash
Q_SERVERS="localhost:9092" ./gradlew run
```

## Using the Application

### Main Menu

When you start the application, you'll see:

```
╔══════════════════════════════════════════════════════════╗
║                                                          ║
║               QUAFKA DEMO APPLICATION                    ║
║                                                          ║
╚══════════════════════════════════════════════════════════╝

============================================================
QUAFKA CONSOLE - DEMO APPLICATION
============================================================
1. Producer Examples
2. Consumer Examples
3. Exit
============================================================
Select an option (1-3):
```

### Producer Examples

Select `1` to see producer examples:

```
------------------------------------------------------------
PRODUCER EXAMPLES
------------------------------------------------------------
1. Basic Producer - Simple message publishing
2. Batch Producer - High-throughput batch sending
3. Header Producer - Messages with custom headers
4. JSON Producer - Type-safe JSON serialization
5. Back to Main Menu
------------------------------------------------------------
```

#### Producer Descriptions

**1. Basic Producer**
- Sends individual messages one at a time
- Basic producer configuration (retries, max in-flight requests)
- Getting send results with partition and offset metadata
- Good for understanding Kafka fundamentals or sending low-volume messages

**2. Batch Producer**
- Batch sending with `sendAll()` for better throughput
- Performance optimizations: larger batch size (32KB), linger time (20ms), LZ4 compression
- Performance measurement and throughput calculation
- Configurable message count
- Ideal for high-throughput scenarios or bulk operations

**3. Header Producer**
- Adding custom headers to messages
- Distributed tracing headers (trace-id, span-id, service-name)
- Versioning and schema headers (message-version, content-type)
- Correlation IDs for request tracking
- Essential for production systems requiring tracing, versioning, or metadata

**4. JSON Producer**
- Type-safe JSON serialization with `JsonSerializer`
- Demonstrates producer side of JSON messaging (see [JSON Serialization](#json-serialization) section)

### Consumer Examples

Select `2` from main menu, then choose consumer type:

```
------------------------------------------------------------
CONSUMER TYPE
------------------------------------------------------------
1. Single Message Consumers
2. Batch Message Consumers
3. Back to Main Menu
------------------------------------------------------------
```

#### Single Message Consumers

```
------------------------------------------------------------
SINGLE MESSAGE CONSUMER EXAMPLES
------------------------------------------------------------
1. Basic Consumer - Simple message processing
2. Consumer with Backpressure - Rate limiting
3. Pipelined Consumer - Middleware-based processing
4. Retryable Consumer - Advanced error handling
5. JSON Consumer - Type-safe JSON deserialization
6. Back to Consumer Menu
------------------------------------------------------------
```

**Consumer Descriptions:**

1. **Basic Consumer**: Single message handler with manual acknowledgment and synchronous processing
2. **Backpressure**: Configurable backpressure buffer (10,000 messages) with release timeout for rate limiting
3. **Pipelined**: Middleware-based processing with multiple retry strategies (single topic, exponential backoff, no retry)
4. **Retryable**: Simplified retry setup using `subscribeWithErrorHandling()` with exception-based retry policies
5. **JSON Consumer**: Type-safe JSON deserialization with `JsonDeserializer` (see [JSON Serialization](#json-serialization) section) 

#### Batch Message Consumers

```
------------------------------------------------------------
BATCH MESSAGE CONSUMER EXAMPLES
------------------------------------------------------------
1. Basic Batch Consumer - Simple batch processing
2. Pipelined Batch Consumer - Middleware for batches
3. Advanced Pipelined Batch - Complex workflows
4. Batch with Single Message Pipeline - Hybrid approach
5. Advanced Batch with Single Pipeline - Context sharing
6. Parallel Batch Processing - Concurrent processing
7. Flexible Batch Processing - Configurable modes
8. Concurrent with Attributes - Parallel + shared state
9. Message Result Collector - Collect processing results
10. Back to Consumer Menu
------------------------------------------------------------
```

**Consumer Descriptions:**

1. **Basic Batch**: Batch message handler with bulk acknowledgment using `ackAll()` for simple sequential batch processing
2. **Pipelined Batch**: Middleware pattern with composable processing steps (logging, business logic, acknowledgment)
3. **Advanced Pipelined**: Reusable custom middlewares with message filtering, performance measurement, and attribute sharing
4. **Batch with Single Pipeline**: Hybrid approach processing each message individually within batch context using `useSingleMessagePipeline()`
5. **Advanced with Single**: Attribute sharing between batch and message pipelines for context propagation (batch ID, timestamps)
6. **Parallel Processing**: Concurrent processing with configurable concurrency level (10 workers) for I/O-bound operations
7. **Flexible**: Toggle between sequential (concurrency=1) and parallel modes (concurrency=5) with same codebase
8. **Concurrent with Attributes**: Parallel processing (10 workers) with thread-safe shared state and batch-level metrics
9. **Message Result Collector**: Thread-safe result aggregation with detailed analytics (success/failure counts, processing times)

## JSON Serialization

Quafka provides a **simplified and decoupled** JSON serialization system with `JsonSerializer` and `JsonDeserializer` for type-safe messaging.

### Architecture Overview

The new design separates producer and consumer concerns:

```
Producer Side                          Consumer Side
─────────────                          ─────────────
JsonSerializer                         JsonDeserializer
     │                                       │
     ├─ Serializes objects to JSON           ├─ Requires typeResolver
     ├─ Adds type info to headers            ├─ Single-pass JSON parsing
     └─ Sends to Kafka                       └─ Type-safe deserialization
```

### Producer Setup

```kotlin
val objectMapper = ObjectMapper().registerKotlinModule()

// Create serializer
val serializer = JsonSerializer.byteArray(objectMapper)
val messageBuilder = OutgoingMessageBuilder.create<String?, ByteArray?>(serializer)
```

**Step 2: Build and Send Messages**

```kotlin
// Use newMessageWithTypeInfo() to automatically add type headers
val message = messageBuilder
    .newMessageWithTypeInfo(
        topic = "demo-orders",
        key = order.orderId,  // String key, serialized by standard StringSerializer
        value = order         // Object serialized to JSON by JsonSerializer
    )
    .build()

producer.send(message)
```

The `newMessageWithTypeInfo()` extension automatically adds an `X-MessageType` header with the type name, making it easy for consumers to deserialize.

### Consumer Setup

**Step 1: Choose a Type Resolution Strategy**

The consumer needs a `TypeResolver` to determine which class to deserialize to. Unlike the producer, the consumer doesn't inherently know the target type from the JSON alone.

```kotlin
val objectMapper = ObjectMapper().registerKotlinModule()

// Choose one of these strategies (see below for details)
val typeResolver = TypeResolvers.fromHeader("X-MessageType", "com.example.events")

// Create deserializer with the type resolver
val deserializer = JsonDeserializer.byteArray(objectMapper, typeResolver = typeResolver)
```

**Step 2: Handle Deserialization Results**

```kotlin
consumer.subscribe("demo-orders") {
    withSingleMessageHandler { incomingMessage, _ ->
        // Deserialize returns a sealed result type
        when (val result = deserializer.deserialize<OrderCreated>(incomingMessage)) {
            is DeserializationResult.Deserialized -> {
                val order = result.value
                println("✅ Received order: ${order.orderId}")
                processOrder(order)
            }
            is DeserializationResult.Error -> {
                println("❌ Deserialization failed: ${result.message}")
                handleError(result.cause)
            }
            DeserializationResult.Null -> {
                println("⚠️ Null value received (tombstone)")
            }
        }
        incomingMessage.ack()
    }
}
```

### Type Resolution Strategies

The consumer needs a `TypeResolver` to determine which class to deserialize JSON to. This is a key difference from the producer side - the consumer must explicitly decide how to resolve types.

**Why Type Resolution is Needed:**
- JSON doesn't carry type information natively
- Kafka messages can contain multiple event types on the same topic
- The deserializer needs to know which class to instantiate

#### 1. Fixed Type (Simplest)

Use when a topic has **only one message type**:

```kotlin
val resolver = TypeResolvers.fixed(OrderCreated::class.java)
```

**When to use:** Single-purpose topics with homogeneous message types.

#### 2. Header-Based (Most Common)

Extract type name from message headers (set by producer using `newMessageWithTypeInfo()`):

```kotlin
// Simple header extraction
val resolver = TypeResolvers.fromHeader(
    headerKey = "X-MessageType",  // Default header key
    packageName = "com.trendyol.quafka.examples.console"  // Optional package prefix
)

// Check multiple headers in order
val resolver = TypeResolvers.fromHeader(
    headerNames = listOf("X-MessageType", "X-EventType"),
    packagePrefix = "com.example.events"
)
```

**When to use:** Standard use case with well-defined event types.

#### 3. Mapping-Based (Type-Safe)

Explicit mapping between type names and classes - **most type-safe approach**:

```kotlin
val resolver = TypeResolvers.fromMapping(
    mapping = mapOf(
        "OrderCreated" to OrderCreated::class.java,
        "OrderUpdated" to OrderUpdated::class.java,
        "OrderCancelled" to OrderCancelled::class.java
    ),
    headerKey = "X-MessageType"  // Where to read the type name from
)
```

**Advantages:**
- Compile-time safety for known types
- Clear documentation of supported types
- Prevents arbitrary class loading

**When to use:** Production systems with a fixed set of known event types.

#### 4. JSON Field-Based

Extract type from a field within the JSON payload itself:

```kotlin
// From single field
val resolver = TypeResolvers.fromJsonField(
    fieldName = "@type",
    packagePrefix = "com.example"
)

// From multiple fields with fallback
val resolver = TypeResolvers.fromJsonField(
    fieldNames = listOf("@type", "eventType", "type"),
    packagePrefix = "com.example.events"
)
```

**When to use:** When type information is embedded in JSON (e.g., Jackson polymorphic types).

#### 5. Custom Lambda (Maximum Flexibility)

Custom logic for complex scenarios:

```kotlin
val resolver: TypeResolver<ByteArray?, ByteArray?> = { jsonNode, message ->
    // Option 1: Examine headers
    val typeFromHeader = message.headers.get("X-EventType")?.asString()
    
    // Option 2: Examine JSON content
    val typeFromJson = jsonNode.get("@type")?.asText()
    
    // Option 3: Combine multiple sources
    val typeName = typeFromHeader ?: typeFromJson
    
    // Return the target class, or null if cannot resolve
    when (typeName) {
        "OrderCreated" -> OrderCreated::class.java
        "UserRegistered" -> UserRegistered::class.java
        "PaymentProcessed" -> PaymentProcessed::class.java
        else -> {
            // Could also log warning or throw exception
            null
        }
    }
}
```

**When to use:** Complex routing logic, migrations, or gradual rollouts.

#### 6. Chained (Fallback Strategy)

Try multiple strategies in order, using the first successful match:

```kotlin
val resolver = TypeResolvers.chain(
    // Try explicit mapping first (most controlled)
    TypeResolvers.fromMapping(knownTypes, "X-MessageType"),
    
    // Fallback to header with package prefix
    TypeResolvers.fromHeader("X-EventType", "com.example"),
    
    // Final fallback to custom logic
    { jsonNode, message -> 
        // Custom fallback logic
        DefaultEvent::class.java
    }
)
```

**When to use:** 
- Migrating between serialization strategies
- Supporting multiple event versions
- Graceful degradation

**Advantages:**
- Handles multiple event formats
- Smooth migrations
- Backwards compatibility

### Running the JSON Examples

**Start JSON Producer:**
```bash
./gradlew run
# Select: 1 (Producer Examples)
# Select: 4 (JSON Producer)
```

**Start JSON Consumer (in another terminal):**
```bash
./gradlew run
# Select: 2 (Consumer Examples)
# Select: 1 (Single Message Consumers)
# Select: 5 (JSON Consumer)
```

The JSON Consumer example is self-contained and produces its own test messages, demonstrating all four type resolution strategies.

## Usage Workflow

### Testing Producers

1. Select "Producer Examples" from main menu
2. Choose a producer type (e.g., "Batch Producer")
3. Producer will send messages and show results
4. Press Enter to return to menu

### Testing Consumers

1. Select "Consumer Examples" from main menu
2. Choose consumer type (Single or Batch)
3. Select specific consumer pattern
4. Consumer will start processing messages
5. Press Ctrl+C to stop the consumer

**Note:** 
- The JSON Consumer example is self-contained and produces its own test messages
- For understanding the concepts, see the [JSON Serialization](#json-serialization) section

## Example Session

```bash
# Terminal 1: Start the application
./gradlew run

# Select: 1 (Producer Examples)
# Select: 2 (Batch Producer)
# Enter message count: 1000
# Watch messages being sent

# Select: 4 (Back to menu)
# Select: 2 (Consumer Examples)
# Select: 1 (Single Message Consumers)
# Select: 1 (Basic Consumer)
# Watch messages being consumed
# Press Ctrl+C to stop

# Terminal 2: Monitor Kafka topics (optional)
kafka-console-consumer --bootstrap-server localhost:9092 \
  --topic demo-topic \
  --from-beginning
```

## Topics Used

The demo application uses these Kafka topics:

- `demo-topic` - Basic producer/consumer examples
- `demo-orders` - JSON OrderCreated events (used by JSON Consumer)
- Various retry topics (for retryable consumer)

## Configuration

### Environment Variables

- `Q_SERVERS` - Kafka bootstrap servers (default: localhost:9092)

### Consumer Configuration

All consumers use:
- Auto-generated group IDs (with timestamp)
- `earliest` auto-offset-reset
- Manual commit mode (auto-commit disabled)

### Producer Configuration

All producers use:
- `acks=all` for reliability
- Idempotence enabled
- Appropriate retries

## Troubleshooting

### Connection Refused

```
Error: Connection to localhost:9092 refused
```

**Solution**: Make sure Kafka is running on the specified servers.

### No Messages in Consumer

**Solution**: Run a producer first to create messages in the topics.

### Consumer Group ID Conflicts

Each consumer run uses a unique group ID with timestamp, so there shouldn't be conflicts.

## Project Structure

```
examples/console/
├── src/main/kotlin/com/trendyol/quafka/examples/console/
│   ├── App.kt                          # Main CLI application
│   ├── producers/
│   │   ├── ProducerBase.kt             # Base class
│   │   ├── BasicProducer.kt            # Simple producer
│   │   ├── BatchProducer.kt            # Batch producer
│   │   ├── JsonProducer.kt             # JSON producer (deprecated)
│   │   └── HeaderProducer.kt           # Header producer
│   ├── consumers/
│   │   ├── single/
│   │   │   ├── BasicConsumer.kt
│   │   │   ├── BasicConsumerWithBackpressure.kt
│   │   │   ├── PipelinedConsumer.kt
│   │   │   ├── RetryableConsumer.kt
│   │   │   └── JsonConsumer.kt
│   │   └── batch/
│   │       ├── BasicBatchConsumer.kt
│   │       ├── PipelinedBatchConsumer.kt
│   │       ├── AdvancedPipelinedBatchConsumer.kt
│   │       ├── BatchWithSingleMessagePipelineConsumer.kt
│   │       ├── AdvancedBatchWithSingleMessagePipelineConsumer.kt
│   │       ├── ParallelBatchProcessingConsumer.kt
│   │       ├── FlexibleBatchProcessingConsumer.kt
│   │       ├── ConcurrentWithAttributesConsumer.kt
│   │       └── MessageResultCollectorConsumer.kt
│   └── ...
├── build.gradle.kts
└── README.md (this file)
```

## Learning Path

Recommended order for learning:

1. **Start with Basic Producer** - Understand fundamentals
2. **Try Basic Consumer** - See message consumption
3. **Explore JSON Producer and Consumer** - Learn type-safe JSON messaging (see [JSON Serialization](#json-serialization))
4. **Explore Batch Producer** - See performance benefits
5. **Try Consumer with Backpressure** - Understand flow control
6. **Explore Pipelined patterns** - Learn middleware architecture
7. **Try Batch Consumers** - Understand batch processing
8. **Try Advanced patterns** - Master complex scenarios

## Documentation

For detailed explanations and concepts, see:

- [Single Consumer Examples](../../docs/examples/single-consumer-examples.md)
- [Batch Consumer Examples](../../docs/examples/batch-consumer-examples.md)
- [Producer Examples](../../docs/examples/producer-examples.md)

## Contributing

This demo application is part of the Quafka project. To add new examples:

1. Create new producer/consumer class extending base classes
2. Add to menu in `App.kt`
3. Update this README
4. Add to documentation if it demonstrates new patterns

## License

Same as Quafka main project - Apache License 2.0

