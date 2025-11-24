# Producer Examples

This document provides comprehensive examples for producer patterns in Quafka.

## Table of Contents
- [Basic Producer](#basic-producer)
- [Batch Producer](#batch-producer)
- [JSON Producer with Extensions](#json-producer-with-extensions)
- [Producer with Error Handling](#producer-with-error-handling)
- [Producer with Custom Headers](#producer-with-custom-headers)
- [Best Practices](#best-practices)

---

## Basic Producer

The simplest form of a Quafka producer for sending single messages.

### Concept
A basic producer creates and sends messages to Kafka topics. Each message consists of a topic, optional key, value, and headers. The producer returns a `DeliveryResult` that contains metadata about the sent message (offset, partition, timestamp).

### Use Cases
- Simple message publishing
- Event publishing
- Command sending
- Notification dispatch
- Log aggregation

### Example

```kotlin
import com.trendyol.quafka.producer.*
import com.trendyol.quafka.producer.configuration.QuafkaProducerBuilder
import org.apache.kafka.common.serialization.StringSerializer

class BasicProducer(servers: String) {
    private val producer: QuafkaProducer<String?, String?>
    
    init {
        val props = mutableMapOf<String, Any>(
            "bootstrap.servers" to servers,
            "acks" to "all",
            "retries" to 3,
            "max.in.flight.requests.per.connection" to 5,
            "enable.idempotence" to true
        )
        
        producer = QuafkaProducerBuilder<String?, String?>(props)
            .withSerializer(StringSerializer(), StringSerializer())
            .build()
    }
    
    fun sendMessage(topic: String, key: String?, value: String): DeliveryResult {
        val message = OutgoingMessage.create(
            topic = topic,
            key = key,
            value = value,
            headers = emptyList()
        )
        
        val result = producer.send(message)
        
        println("Message sent successfully:")
        println("  Topic: ${result.topic}")
        println("  Partition: ${result.partition}")
        println("  Offset: ${result.offset}")
        println("  Timestamp: ${result.timestamp}")
        
        return result
    }
    
    fun close() {
        producer.close()
    }
}

// Usage
fun main() {
    val producer = BasicProducer("localhost:9092")
    
    producer.sendMessage(
        topic = "orders",
        key = "order-123",
        value = "Order created for user 456"
    )
    
    producer.close()
}
```

### Key Points
- **Synchronous**: `send()` blocks until the message is acknowledged by Kafka
- **Delivery Result**: Contains metadata about where the message was stored
- **Idempotence**: `enable.idempotence=true` prevents duplicate messages
- **Retries**: Automatic retry on transient failures
- **Resource Management**: Always close the producer when done

### Configuration Options

```kotlin
val props = mutableMapOf<String, Any>(
    // Required
    "bootstrap.servers" to "localhost:9092",
    
    // Reliability
    "acks" to "all",                    // Wait for all replicas
    "retries" to 3,                      // Retry on failure
    "enable.idempotence" to true,        // Prevent duplicates
    
    // Performance
    "batch.size" to 16384,               // Batch size in bytes
    "linger.ms" to 10,                   // Wait time for batching
    "compression.type" to "snappy",      // Compress messages
    "buffer.memory" to 33554432,         // Buffer size
    
    // Ordering
    "max.in.flight.requests.per.connection" to 5  // With idempotence
)
```

---

## Batch Producer

Efficiently send multiple messages in a single operation.

### Concept
Batch producing sends multiple messages together, significantly reducing network overhead and improving throughput. This is ideal for high-volume scenarios where you need to send many messages at once.

### Use Cases
- Bulk data ingestion
- Log aggregation
- Event batch publishing
- Data migration
- High-throughput scenarios

### Example

```kotlin
import com.trendyol.quafka.producer.*
import com.trendyol.quafka.producer.configuration.QuafkaProducerBuilder
import org.apache.kafka.common.serialization.StringSerializer

class BatchProducer(servers: String) {
    private val producer: QuafkaProducer<String?, String?>
    
    init {
        val props = mutableMapOf<String, Any>(
            "bootstrap.servers" to servers,
            "acks" to "all",
            "enable.idempotence" to true,
            "batch.size" to 32768,        // Larger batch size
            "linger.ms" to 20,            // Wait longer for batching
            "compression.type" to "lz4"   // Better compression for batches
        )
        
        producer = QuafkaProducerBuilder<String?, String?>(props)
            .withSerializer(StringSerializer(), StringSerializer())
            .build()
    }
    
    fun sendBatch(topic: String, messages: List<Pair<String?, String>>): Collection<DeliveryResult> {
        val outgoingMessages = messages.map { (key, value) ->
            OutgoingMessage.create(
                topic = topic,
                key = key,
                value = value
            )
        }
        
        val results = producer.sendAll(outgoingMessages)
        
        println("Batch sent successfully: ${results.size} messages")
        println("First offset: ${results.first().offset}")
        println("Last offset: ${results.last().offset}")
        
        return results
    }
    
    fun sendLargeBatch(topic: String, count: Int): Collection<DeliveryResult> {
        val messages = (1..count).map { index ->
            OutgoingMessage.create(
                topic = topic,
                key = "key-$index",
                value = "value-$index"
            )
        }
        
        return producer.sendAll(messages)
    }
    
    fun close() {
        producer.close()
    }
}

// Usage
fun main() {
    val producer = BatchProducer("localhost:9092")
    
    // Send a batch of order events
    val orders = listOf(
        "order-1" to "Order created for user 101",
        "order-2" to "Order created for user 102",
        "order-3" to "Order created for user 103"
    )
    producer.sendBatch("orders", orders)
    
    // Send a large batch
    producer.sendLargeBatch("events", 1000)
    
    producer.close()
}
```

### Performance Comparison

#### Single Messages (1000 messages)
```
Time: ~5000ms
Network Calls: 1000
Throughput: ~200 msg/sec
```

#### Batch Messages (1000 messages)
```
Time: ~500ms
Network Calls: ~10-20 (depending on batch.size)
Throughput: ~2000 msg/sec
```

**~10x faster for bulk operations!**

### Key Points
- **Efficiency**: Significantly reduces network overhead
- **Throughput**: Much higher messages per second
- **Batching**: Kafka internally batches based on `batch.size` and `linger.ms`
- **All or Nothing**: All messages in a batch succeed or fail together
- **Order Preservation**: Messages within a batch maintain order

---

## JSON Producer with Extensions

Produce structured JSON messages using Quafka extensions.

### Concept
The JSON producer extension provides type-safe serialization of Kotlin objects to JSON. It uses `JsonSerializer` for converting objects to JSON and automatically handles type information in headers, making it easy for consumers to deserialize messages back to the correct types.

### Use Cases
- Domain events with rich data structures
- Commands with complex payloads
- API-style messaging
- Microservices communication
- Type-safe messaging

### Example

```kotlin
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import com.trendyol.quafka.producer.*
import com.trendyol.quafka.producer.configuration.QuafkaProducerBuilder
import com.trendyol.quafka.extensions.producer.OutgoingMessageBuilder
import com.trendyol.quafka.extensions.serialization.json.*
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.kafka.common.serialization.ByteArraySerializer

// Domain models
data class OrderCreated(
    val orderId: String,
    val userId: String,
    val amount: Double,
    val items: List<OrderItem>,
    val timestamp: Long = System.currentTimeMillis()
)

data class OrderItem(
    val productId: String,
    val quantity: Int,
    val price: Double
)

data class UserRegistered(
    val userId: String,
    val email: String,
    val name: String,
    val timestamp: Long = System.currentTimeMillis()
)

class JsonProducer(servers: String) {
    private val producer: QuafkaProducer<String?, ByteArray?>
    private val messageBuilder: OutgoingMessageBuilder<String?, ByteArray?>
    
    init {
        // Configure Jackson ObjectMapper
        val objectMapper = ObjectMapper().apply {
            registerKotlinModule()
            // Add any custom configurations
        }
        
        // Create JSON serializer (no type resolver needed for serialization!)
        val serializer = JsonSerializer.byteArray(objectMapper)
        
        // Create message builder
        messageBuilder = OutgoingMessageBuilder.create(serializer)
        
        // Create producer
        val props = mutableMapOf<String, Any>(
            "bootstrap.servers" to servers,
            "acks" to "all",
            "enable.idempotence" to true
        )
        
        producer = QuafkaProducerBuilder<String?, ByteArray?>(props)
            .withSerializer(StringSerializer(), ByteArraySerializer())
            .build()
    }
    
    fun sendOrderCreated(order: OrderCreated): DeliveryResult {
        val message = messageBuilder
            .newMessageWithTypeInfo(
                topic = "orders.created",
                key = order.orderId,
                value = order
            )
            .build()
        
        val result = producer.send(message)
        println("OrderCreated event sent: orderId=${order.orderId}, offset=${result.offset}")
        return result
    }
    
    fun sendUserRegistered(user: UserRegistered): DeliveryResult {
        val message = messageBuilder
            .newMessageWithTypeInfo(
                topic = "users.registered",
                key = user.userId,
                value = user
            )
            .build()
        
        val result = producer.send(message)
        println("UserRegistered event sent: userId=${user.userId}, offset=${result.offset}")
        return result
    }
    
    fun sendBatchEvents(orders: List<OrderCreated>): Collection<DeliveryResult> {
        val messages = orders.map { order ->
            messageBuilder
                .newMessageWithTypeInfo(
                    topic = "orders.created",
                    key = order.orderId,
                    value = order
                )
                .build()
        }
        
        return producer.sendAll(messages)
    }
    
    fun close() {
        producer.close()
    }
}

// Usage
fun main() {
    val producer = JsonProducer("localhost:9092")
    
    // Send order created event
    val order = OrderCreated(
        orderId = "order-123",
        userId = "user-456",
        amount = 299.99,
        items = listOf(
            OrderItem("product-1", 2, 149.99),
            OrderItem("product-2", 1, 0.01)
        )
    )
    producer.sendOrderCreated(order)
    
    // Send user registered event
    val user = UserRegistered(
        userId = "user-789",
        email = "user@example.com",
        name = "John Doe"
    )
    producer.sendUserRegistered(user)
    
    // Send batch of orders
    val orders = (1..10).map { index ->
        OrderCreated(
            orderId = "order-$index",
            userId = "user-$index",
            amount = 100.0 * index,
            items = listOf(OrderItem("product-$index", 1, 100.0 * index))
        )
    }
    producer.sendBatchEvents(orders)
    
    producer.close()
}
```

### Message Structure

When a JSON message is sent, it includes type information in headers:

```
Headers:
  - X-MessageType: com.example.OrderCreated
  
Body (JSON):
{
  "orderId": "order-123",
  "userId": "user-456",
  "amount": 299.99,
  "items": [
    {
      "productId": "product-1",
      "quantity": 2,
      "price": 149.99
    }
  ],
  "timestamp": 1234567890
}
```

### Key Points
- **Type Safety**: Compile-time type checking for messages
- **Automatic Serialization**: No manual JSON handling needed
- **Type Information**: Automatically added to headers via `newMessageWithTypeInfo()`
- **Jackson Integration**: Full Jackson features available (custom serializers, date formats, etc.)
- **Simple API**: Just create `JsonSerializer` and use `OutgoingMessageBuilder`

---

## Producer with Error Handling

Implement robust error handling for producer operations.

### Concept
Production environments require robust error handling to deal with network failures, broker unavailability, and other issues. Quafka provides configurable error handling options and callbacks.

### Use Cases
- Mission-critical message publishing
- Guaranteed delivery requirements
- Error monitoring and alerting
- Fallback mechanisms
- Dead letter queue patterns



### Error Handling Strategies

#### 1. Immediate Failure
```kotlin
.withErrorOptions(ProducingOptions(stopOnFirstError = true))
```

#### 2. Continue on Error
```kotlin
.withErrorOptions(ProducingOptions(stopOnFirstError = false))
```


### Key Points
- **Retry Logic**: Implement exponential backoff
- **DLQ Pattern**: Send failed messages to dead letter queue
- **Monitoring**: Log all failures for monitoring
- **Idempotence**: Prevent duplicates during retries
- **Graceful Degradation**: Fallback mechanisms for critical failures

---

## Producer with Custom Headers

Add metadata to messages using custom headers.

### Concept
Headers allow you to attach metadata to messages without modifying the message payload. This is useful for routing, filtering, tracing, and adding contextual information.

### Use Cases
- Distributed tracing (trace IDs, span IDs)
- Message routing and filtering
- Audit information (user, timestamp, source)
- Message versioning
- Correlation IDs

### Example

```kotlin
import com.trendyol.quafka.producer.*
import com.trendyol.quafka.producer.configuration.QuafkaProducerBuilder
import org.apache.kafka.common.header.Header
import org.apache.kafka.common.header.internals.RecordHeader
import org.apache.kafka.common.serialization.StringSerializer
import java.util.UUID

class HeaderProducer(servers: String) {
    private val producer: QuafkaProducer<String?, String?>
    
    init {
        val props = mutableMapOf<String, Any>(
            "bootstrap.servers" to servers,
            "acks" to "all",
            "enable.idempotence" to true
        )
        
        producer = QuafkaProducerBuilder<String?, String?>(props)
            .withSerializer(StringSerializer(), StringSerializer())
            .build()
    }
    
    fun sendWithTracing(
        topic: String,
        key: String?,
        value: String,
        traceId: String = UUID.randomUUID().toString(),
        spanId: String = UUID.randomUUID().toString()
    ): DeliveryResult {
        val headers = listOf(
            header("trace-id", traceId),
            header("span-id", spanId),
            header("service-name", "order-service"),
            header("timestamp", System.currentTimeMillis().toString())
        )
        
        val message = OutgoingMessage.create(
            topic = topic,
            key = key,
            value = value,
            headers = headers
        )
        
        return producer.send(message)
    }
    
    fun sendWithVersion(
        topic: String,
        key: String?,
        value: String,
        version: String
    ): DeliveryResult {
        val headers = listOf(
            header("message-version", version),
            header("schema-version", "1.0"),
            header("content-type", "application/json")
        )
        
        val message = OutgoingMessage.create(
            topic = topic,
            key = key,
            value = value,
            headers = headers
        )
        
        return producer.send(message)
    }
    
    fun sendWithCorrelation(
        topic: String,
        key: String?,
        value: String,
        correlationId: String,
        causationId: String? = null
    ): DeliveryResult {
        val headers = mutableListOf(
            header("correlation-id", correlationId)
        )
        
        causationId?.let {
            headers.add(header("causation-id", it))
        }
        
        headers.addAll(listOf(
            header("message-id", UUID.randomUUID().toString()),
            header("timestamp", System.currentTimeMillis().toString())
        ))
        
        val message = OutgoingMessage.create(
            topic = topic,
            key = key,
            value = value,
            headers = headers
        )
        
        return producer.send(message)
    }
    
    fun sendWithAudit(
        topic: String,
        key: String?,
        value: String,
        userId: String,
        action: String
    ): DeliveryResult {
        val headers = listOf(
            header("user-id", userId),
            header("user-action", action),
            header("source-service", "api-gateway"),
            header("timestamp", System.currentTimeMillis().toString()),
            header("environment", System.getenv("ENVIRONMENT") ?: "dev")
        )
        
        val message = OutgoingMessage.create(
            topic = topic,
            key = key,
            value = value,
            headers = headers
        )
        
        return producer.send(message)
    }
    
    fun close() {
        producer.close()
    }
}

// Usage
fun main() {
    val producer = HeaderProducer("localhost:9092")
    
    // Send with distributed tracing
    producer.sendWithTracing(
        topic = "orders",
        key = "order-123",
        value = "Order data",
        traceId = "trace-abc-123",
        spanId = "span-xyz-456"
    )
    
    // Send with versioning
    producer.sendWithVersion(
        topic = "events",
        key = "event-1",
        value = """{"eventType": "OrderCreated", "orderId": "123"}""",
        version = "2.0"
    )
    
    // Send with correlation
    producer.sendWithCorrelation(
        topic = "commands",
        key = "cmd-1",
        value = "Process order",
        correlationId = "corr-123",
        causationId = "order-created-event-456"
    )
    
    // Send with audit
    producer.sendWithAudit(
        topic = "audit-log",
        key = "audit-1",
        value = "User deleted order",
        userId = "user-789",
        action = "DELETE_ORDER"
    )
    
    producer.close()
}
```

### Common Header Patterns

#### 1. Distributed Tracing
```kotlin
headers = listOf(
    header("trace-id", traceId),
    header("span-id", spanId),
    header("parent-span-id", parentSpanId)
)
```

#### 2. Message Versioning
```kotlin
headers = listOf(
    header("message-version", "2.0"),
    header("schema-version", "1.5"),
    header("content-type", "application/json")
)
```

#### 3. Event Sourcing
```kotlin
headers = listOf(
    header("event-type", "OrderCreated"),
    header("event-version", "1"),
    header("aggregate-id", orderId),
    header("aggregate-type", "Order"),
    header("sequence-number", "5")
)
```

#### 4. Audit Trail
```kotlin
headers = listOf(
    header("user-id", userId),
    header("user-email", email),
    header("action", action),
    header("timestamp", timestamp),
    header("ip-address", ipAddress)
)
```

### Key Points
- **Metadata**: Add context without modifying payload
- **Filtering**: Consumers can filter based on headers
- **Routing**: Route messages based on header values
- **Tracing**: Implement distributed tracing
- **Versioning**: Track message and schema versions

---

## Best Practices

### 1. Resource Management

```kotlin
// Always close producers
class MyService {
    private val producer = QuafkaProducerBuilder<String?, String?>(props)
        .withSerializer(StringSerializer(), StringSerializer())
        .build()
    
    fun close() {
        producer.close() // Ensures all messages are flushed
    }
}

// Or use use() function
fun sendMessages() {
    QuafkaProducerBuilder<String?, String?>(props)
        .withSerializer(StringSerializer(), StringSerializer())
        .build()
        .use { producer ->
            // Use producer
        } // Automatically closed
}
```

### 2. Configuration for Different Scenarios

#### High Throughput
```kotlin
val props = mutableMapOf(
    "acks" to "1",
    "batch.size" to 32768,
    "linger.ms" to 20,
    "compression.type" to "lz4",
    "buffer.memory" to 67108864
)
```

#### High Reliability
```kotlin
val props = mutableMapOf(
    "acks" to "all",
    "retries" to Int.MAX_VALUE,
    "enable.idempotence" to true,
    "max.in.flight.requests.per.connection" to 5
)
```

#### Low Latency
```kotlin
val props = mutableMapOf(
    "acks" to "1",
    "linger.ms" to 0,
    "batch.size" to 0,
    "compression.type" to "none"
)
```
