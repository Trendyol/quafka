[![OpenSSF Scorecard](https://api.scorecard.dev/projects/github.com/Trendyol/quafka/badge)](https://scorecard.dev/viewer/?uri=github.com/Trendyol/quafka)

<h1 align="center"><img alt="Quafka" src="./docs/assets/logo.jpeg" width="400px">
  <br>Quafka
</h1>

<p align="center">
  <strong>A non-blocking Kafka client library for Kotlin with first-class coroutine support</strong>
</p>

<p align="center">
  <a href="https://codecov.io/gh/Trendyol/quafka">
    <img src="https://codecov.io/gh/Trendyol/quafka/graph/badge.svg?token=HcKBT3chO7" alt="codecov">
  </a>
  <a href="LICENSE">
    <img src="https://img.shields.io/badge/License-Apache%202.0-blue.svg" alt="License">
  </a>
</p>

<p align="center">
  <a href="#features">Features</a> •
  <a href="#getting-started">Getting Started</a> •
  <a href="#examples">Examples</a> •
  <a href="#documentation">Documentation</a> •
  <a href="#contributing">Contributing</a>
</p>

---

## About

Quafka is a non-blocking Kafka client library built on top of the Apache Kafka Java client, designed for Kotlin with first-class coroutine support. It separates polling from processing, preserves per-partition ordering, and provides flexible single and batch message handling, backpressure control, and a fluent, type-safe configuration DSL.

Quafka also ships with an extensions module that adds higher-level capabilities like retry orchestration, delayed processing, and a middleware pipeline for consumer logic.

## Features

### Core Features

- **🚀 Non-Blocking & Coroutine-First**
  - Fully supports Kotlin coroutines, enabling highly efficient, asynchronous operations
  - Ensures your Kafka client remains responsive and scalable under heavy loads without thread management complexity

- **🔄 Separated Polling and Processing Model**
  - Decoupled threading model where polling and processing happen independently
  - Prevents consumer group rebalancing due to slow processing
  - Ensures consistent throughput even for long-running tasks

- **⚡ Worker per Topic and Partition**
  - Dedicated workers for each topic-partition pair
  - Sequential processing within a partition for order preservation
  - Maximum parallelism across partitions for optimal resource utilization

- **📦 Batch and Single Message Handling**
  - Seamless support for both batch and individual message processing
  - Flexible callbacks to choose the most efficient approach
  - Reduced boilerplate code

- **🎛️ Backpressure Management**
  - Advanced backpressure controls that dynamically adjust message fetching
  - Prevents memory overload and ensures graceful degradation
  - Configurable using message count, timeouts, or both

- **🔧 Fluent and Safe Configuration**
  - Developer-friendly configuration DSL
  - Type-safe defaults and validations
  - Step-by-step guidance for setup

- **📤 Batch Publishing**
  - Efficiently publish multiple messages in a single operation
  - Reduced network overhead for improved performance
  - Ideal for high-throughput scenarios

- **🛡️ Error Handling**
  - Robust error handling with automatic retries
  - Error callbacks and configurable failure strategies
  - Graceful recovery from unexpected issues

## Why Quafka?

### Compared to Apache Kafka Java Client

| Feature | Quafka | Apache Kafka Java Client |
|---------|--------|--------------------------|
| **Coroutine Support** | ✅ Native coroutine APIs | ❌ Blocking/callback-based |
| **Boilerplate** | ✅ Minimal with builders | ❌ Verbose configuration |
| **Poll/Process Separation** | ✅ Built-in decoupling | ❌ Manual implementation |
| **Backpressure** | ✅ Simple configuration | ❌ Manual implementation |
| **Retry Mechanism** | ✅ Built-in with extensions | ❌ Custom implementation needed |

### Compared to Spring Kafka

| Feature | Quafka | Spring Kafka |
|---------|--------|--------------|
| **API Style** | ✅ Lightweight Kotlin API | ⚠️ Spring-based abstractions |
| **Threading Model** | ✅ Coroutine-based | ⚠️ Spring threading |
| **Control** | ✅ Explicit per-topic DSL | ⚠️ Annotation-based |
| **Framework Coupling** | ✅ None required | ❌ Requires Spring |
| **Extensions** | ✅ Opt-in middleware | ⚠️ Framework-tied |

**Choose Quafka** when you want a coroutine-first, lean, and highly controllable Kafka client with strong defaults and optional high-level extensions.

## Getting Started

### Installation

Add the following dependency to your `build.gradle.kts`:

```kotlin
dependencies {
    implementation("com.trendyol:quafka:0.2.0")
}
```

For extensions (retry, pipelines, etc.):
```kotlin
dependencies {
    implementation("com.trendyol:quafka-extensions:0.2.0")
}
```

### Try the Interactive Demo 🎮

The fastest way to explore Quafka is through our interactive console application:

```bash
cd examples/console
export Q_SERVERS="localhost:9092"
./gradlew run
```

The console app provides a menu-driven interface to try all producer and consumer patterns. Perfect for learning and testing!

See the [Console Demo README](examples/console/README.md) for details.

### Quick Start

#### Simple Consumer

```kotlin
import com.trendyol.quafka.consumer.configuration.*
import org.apache.kafka.common.serialization.StringDeserializer

val props = mapOf(
    "bootstrap.servers" to "localhost:9092",
    "group.id" to "example-group"
)

val consumer = QuafkaConsumerBuilder<String, String>(props)
    .withDeserializer(StringDeserializer(), StringDeserializer())
    .subscribe("example-topic") {
        withSingleMessageHandler { message, context ->
            println("Received: ${message.value}")
            message.ack()
        }
    }
    .build()

consumer.start()
```

#### Simple Producer

```kotlin
import com.trendyol.quafka.producer.*
import com.trendyol.quafka.producer.configuration.QuafkaProducerBuilder
import org.apache.kafka.common.serialization.StringSerializer

val props = mapOf("bootstrap.servers" to "localhost:9092")

val producer = QuafkaProducerBuilder<String?, String?>(props)
    .withSerializer(StringSerializer(), StringSerializer())
    .build()

val result = producer.send(
    OutgoingMessage.create(
        topic = "test-topic",
        key = "key",
        value = "Hello, Quafka!"
    )
)
```

## Examples

### 📖 Consumer Examples

#### Single Message Processing

- **[Single Consumer Examples](docs/examples/single-consumer-examples.md)** - Comprehensive guide for single message consumers
  - [Basic Single Consumer](docs/examples/single-consumer-examples.md#basic-single-consumer) - Simple message processing
  - [Consumer with Backpressure](docs/examples/single-consumer-examples.md#consumer-with-backpressure) - Rate limiting and flow control
  - [Pipeline Consumer](docs/examples/single-consumer-examples.md#pipeline-consumer) - Middleware-based processing
  - [Retryable Consumer](docs/examples/single-consumer-examples.md#retryable-consumer) - Advanced error handling and retry
  - [Custom Middleware](docs/examples/single-consumer-examples.md#custom-middleware) - Creating reusable middleware

#### Batch Message Processing

- **[Batch Consumer Examples](docs/examples/batch-consumer-examples.md)** - Comprehensive guide for batch message consumers
  - [Basic Batch Consumer](docs/examples/batch-consumer-examples.md#basic-batch-consumer) - Simple batch processing
  - [Pipelined Batch Consumer](docs/examples/batch-consumer-examples.md#pipelined-batch-consumer) - Middleware for batches
  - [Advanced Pipelined Batch Consumer](docs/examples/batch-consumer-examples.md#advanced-pipelined-batch-consumer) - Complex workflows
  - [Batch with Single Message Pipeline](docs/examples/batch-consumer-examples.md#batch-with-single-message-pipeline) - Hybrid approach
  - [Advanced Batch with Single Message Pipeline](docs/examples/batch-consumer-examples.md#advanced-batch-with-single-message-pipeline) - Context sharing
  - [Parallel Batch Processing](docs/examples/batch-consumer-examples.md#parallel-batch-processing) - Concurrent processing
  - [Flexible Batch Processing](docs/examples/batch-consumer-examples.md#flexible-batch-processing) - Configurable modes
  - [Concurrent with Attributes](docs/examples/batch-consumer-examples.md#concurrent-with-attributes) - Parallel + shared state
  - [Custom Batch Middleware](docs/examples/batch-consumer-examples.md#custom-batch-middleware) - Reusable components

### 📤 Producer Examples

- **[Producer Examples](docs/examples/producer-examples.md)** - Comprehensive guide for producers
  - [Basic Producer](docs/examples/producer-examples.md#basic-producer) - Simple message publishing
  - [Batch Producer](docs/examples/producer-examples.md#batch-producer) - Bulk message operations
  - [JSON Producer with Extensions](docs/examples/producer-examples.md#json-producer-with-extensions) - Type-safe JSON serialization
  - [Producer with Error Handling](docs/examples/producer-examples.md#producer-with-error-handling) - Robust error handling and DLQ
  - [Producer with Custom Headers](docs/examples/producer-examples.md#producer-with-custom-headers) - Metadata and tracing
  - [Best Practices](docs/examples/producer-examples.md#best-practices) - Production-ready patterns

### ⚙️ Advanced Features

#### Backpressure Control

```kotlin
import kotlin.time.Duration.Companion.minutes

val consumer = QuafkaConsumerBuilder<String, String>(props)
    .withDeserializer(StringDeserializer(), StringDeserializer())
    .subscribe("example-topic") {
        withSingleMessageHandler { msg, ctx ->
            // Slow processing work
            processMessage(msg)
            msg.ack()
        }.withBackpressure(
            backpressureBufferSize = 10_000,
            backpressureReleaseTimeout = 1.minutes
        )
    }
    .build()
```

#### Batch Publishing

```kotlin
val messages = (1..100).map { index ->
    OutgoingMessage.create(
        topic = "test-topic",
        key = "key-$index",
        value = "value-$index"
    )
}

val results: Collection<DeliveryResult> = producer.sendAll(messages)
```

## Quafka Extensions

Quafka-Extensions is a companion module that provides advanced, high-level features:

### 🔄 Serialization / Deserialization

- Built-in support for JSON and common formats
- Extensible design for custom serialization strategies
- Type-safe serialization leveraging Kotlin's type system

### ⏰ Message Delayer

- Schedule message processing with fine-grained delay control
- Fixed and dynamic delay strategies
- Perfect for delayed retries and scheduled workflows

### 🔁 Advanced Retry Mechanism

- **In-Memory Retry**: Lightweight retry for transient failures
- **Multi-Topic Retry**: Forward to dedicated retry topics with configurable backoff
- **Flexible Policies**: Exponential backoff, max attempts, custom retry logic

### 🔗 Messaging Pipeline

Middleware-style architecture for composable message processing:

```kotlin
import com.trendyol.quafka.extensions.consumer.single.pipelines.PipelineMessageHandler.Companion.usePipelineMessageHandler

val consumer = QuafkaConsumerBuilder<String, String>(props)
    .withDeserializer(StringDeserializer(), StringDeserializer())
    .subscribe("example-topic") {
        usePipelineMessageHandler {
            // Logging middleware
            use { envelope, next ->
                logger.info("Processing: ${envelope.message.offset}")
                next()
            }
            
            // Validation middleware
            use { envelope, next ->
                if (isValid(envelope.message.value)) {
                    next()
                } else {
                    logger.warn("Invalid message")
                }
            }
            
            // Business logic
            use { envelope, next ->
                processMessage(envelope.message.value)
                envelope.message.ack()
                next()
            }
        }
    }
    .build()
```

**Available Pipeline Steps:**
- **TracingPipelineStep**: Distributed tracing integration
- **LoggingPipelineStep**: Detailed logging for each stage
- **ErrorHandlerPipelineStep**: Centralized error management
- **DeserializationPipelineStep**: Type-safe deserialization
- **Custom Middleware**: Create your own reusable steps

## Documentation

### 📚 Core Documentation

- **[Fundamentals](docs/fundamentals.md)** - Core concepts and architecture
- **[Message Processing](docs/processing.md)** - How messages are processed internally

### 📖 Example Guides

- **[Single Consumer Examples](docs/examples/single-consumer-examples.md)** - Detailed single message patterns
- **[Batch Consumer Examples](docs/examples/batch-consumer-examples.md)** - Detailed batch processing patterns
- **[Producer Examples](docs/examples/producer-examples.md)** - Producer patterns and best practices

### 🔧 Extensions Documentation

- **[Pipeline Architecture](lib/quafka-extensions/src/main/kotlin/com/trendyol/quafka/extensions/consumer/batch/pipelines/README.md)** - Batch pipeline middleware guide

### 🎮 Interactive Demo

- **[Console Demo Application](examples/console/README.md)** - Interactive CLI for testing all Quafka patterns
  - Producer examples (Basic, Batch, JSON, Headers)
  - Single consumer examples (Basic, Backpressure, Pipeline, Retry)
  - Batch consumer examples (8 different patterns)
  - Easy-to-use menu-driven interface

## Status

> [!WARNING]
> While Quafka is **production-ready** and extensively used at Trendyol, the API is not yet fully stabilized. Breaking changes may occur in minor releases, but migration guides will always be provided.

> [!NOTE]
> Report any issue or bug in the [GitHub repository](https://github.com/Trendyol/quafka/issues).

## Contributing

Contributions are welcome! We appreciate:

* 🐛 **[Bug reports](https://github.com/Trendyol/quafka/issues)** - Help us identify and fix issues
* 💡 **[Feature requests](https://github.com/Trendyol/quafka/issues)** - Share your ideas for improvements
* 📖 **Documentation improvements** - Help make our docs better
* 🚀 **Code contributions** - Submit pull requests

Please read our [Contributing Guidelines](CONTRIBUTING.md) before submitting a pull request.

## License

Quafka is licensed under the Apache License 2.0. See the [LICENSE](LICENSE) file for details.

---

<p align="center">
  Made with ❤️ by <a href="https://github.com/Trendyol">Trendyol</a>
</p>
