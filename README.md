
<h1 align="center">Quafka</h1>
<div align="center">
<p >
  Quafka is a non-blocking Kafka client library built on top of the Apache Kafka Java client, designed for Kotlin with first-class coroutine support 
</p>
<img alt="Quafka" src="./docs/assets/logo.jpeg" width="400px">
</div>


[![codecov](https://codecov.io/gh/Trendyol/quafka/graph/badge.svg?token=HcKBT3chO7)](https://codecov.io/gh/Trendyol/quafka)

## What is Quafka?

Quafka is a non-blocking Kafka client library built on top of the Apache Kafka Java client, designed for Kotlin with first-class coroutine support. It separates polling from processing, preserves per-partition ordering, and provides flexible single and batch message handling, backpressure control, and a fluent, type-safe configuration DSL.

Quafka also ships with an extensions module that adds higher-level capabilities like retry orchestration, delayed processing, and a middleware pipeline for consumer logic.

## Features

- **Non-Blocking**
  - Fully supports Kotlin coroutines, enabling highly efficient, asynchronous operations. This ensures that your Kafka client remains responsive and scalable under heavy loads without the complexity of thread management.

- **Separated Polling and Processing Model**
  - Quafka employs a decoupled threading model where polling and processing happen on independent threads. This prevents Kafka consumers from being affected by processing delays, reducing the risk of consumer group rebalancing and ensuring consistent throughput even for long-running message processing tasks.

- **Worker per Topic and Partition**
  - Dedicated workers are created for each topic-partition pair, ensuring that messages are processed sequentially within a partition while maximizing parallelism across partitions. This approach guarantees message order preservation per partition and optimizes resource utilization across the system.

- **Batch and Single Message Handling**
  - Quafka provides seamless support for both batch and individual message processing, giving developers the flexibility to choose the most efficient approach based on their application’s requirements. Built-in callbacks simplify integration and reduce boilerplate code.

- **Backpressure Management**
  - Quafka includes advanced backpressure controls that dynamically adjust message fetching based on consumer performance. This prevents memory overload and ensures graceful degradation during high load. Configure backpressure using message count, timeouts, or both, tailored to your specific needs.

- **Fluent and Safe Configuration**
  - The library offers a developer-friendly configuration DSL, guiding you step-by-step through setting up consumers and producers. Type-safe defaults and validations help prevent configuration errors, making it simple to get started while ensuring production readiness.

- **Batch Publishing**
  - Efficiently publish multiple messages in a single operation, reducing network overhead and improving performance. Ideal for use cases where high throughput and low latency are critical.

- **Error Handling**
  - Robust error handling mechanisms provide resilience in processing and publishing. Options include automatic retries, error callbacks, and configurable failure strategies, ensuring that your system can recover gracefully from unexpected issues.

## Why choose Quafka over Apache Kafka client or Spring Kafka?

### Compared to the raw Apache Kafka Java client
- **Coroutine-first**: Natural Kotlin coroutine APIs for backpressure-friendly, non-blocking code.
- **Less boilerplate**: Builders for consumers and producers; built-in single/batch strategies, ack/commit helpers.
- **Operational safety**: Decoupled poll/processing model reduces rebalance surprises from slow handlers.
- **Backpressure**: Simple, configurable buffering and release timeouts.
- **Extensions**: Retry orchestration, delayed processing, and pipelines without custom plumbing.

### Compared to Spring Kafka
- **Lightweight Kotlin API**: Minimal abstractions, idiomatic Kotlin, coroutine-based instead of blocking listeners.
- **Explicit control**: Per-topic subscription DSL, per-partition workers, clear ack vs commit semantics.
- **Pluggable extensions**: Opt-in retry and pipeline middleware without framework coupling.
- **Focus**: Streamlined feature set for Kotlin services that prefer coroutines over Spring threading models.

Choose Quafka when you want a coroutine-first, lean, and highly controllable Kafka client with strong defaults and optional high-level extensions.

## Quafka Extensions

Quafka-Extensions is a companion module designed to extend Quafka with advanced, high-level features that further simplify and enhance Kafka integrations. Here are the key features:

### Serialization / Deserialization
- Built-in support for common data format like JSON, eliminating the need to implement custom serializers for routine tasks.
- Extensible design allows you to plug in custom serialization strategies, adapting to complex or domain-specific data structures.
- Provides seamless type safety, leveraging Kotlin’s powerful type system to reduce runtime errors and improve developer productivity.

### Message Delayer
- Schedule message processing with fine-grained control over delays. This feature is perfect for implementing delayed retries, scheduled workflows, or event-driven architectures.
- Supports both fixed and dynamic delay strategies, enabling flexible and context-aware scheduling.
- Built to integrate smoothly with your existing pipelines, ensuring delayed messages are processed in order and without additional complexity.

### Advanced Retry Mechanism
- **In-Memory Retry**: Lightweight retry mechanism that retries transient failures directly in memory. Ideal for high-performance scenarios where persistence overhead is unnecessary.
- **Multi-Topic Retry**: For more complex use cases, failed messages can be forwarded to designated retry topics with configurable backoff strategies. This ensures separation of transient and critical failures, allowing for targeted handling and monitoring.
- Flexible retry policies, including exponential backoff, maximum retry attempts, and custom retry logic, provide fine-grained control over failure recovery.

### Messaging Pipeline
- Quafka-Extensions introduces a middleware-style architecture for processing messages, enabling the composition of reusable, modular pipeline steps. This ensures a clean separation of concerns and simplifies debugging and testing.
- Example pipeline steps:
  - **TracingPipelineStep**: Automatically integrates distributed tracing, allowing you to monitor and visualize the flow of messages across services.
  - **LoggingPipelineStep**: Captures detailed logs for each processing stage, making it easy to trace issues or understand system behavior.
  - **ErrorHandlerPipelineStep**: Centralized error management for catching and handling exceptions, ensuring that failures are logged and acted upon consistently.
  - **DeserializationPipelineStep**: Converts raw message payloads into strongly-typed Kotlin objects, preparing them for business logic processing.
  - **MessageProcessorPipelineStep**: Executes the core business logic, keeping it isolated from ancillary concerns like tracing or error handling.
- Pipelines are fully customizable and extensible, empowering developers to create sophisticated message processing flows without sacrificing simplicity or clarity.


## Getting Started

### Installation
Add the following dependency to your `build.gradle.kts`:
```kotlin
dependencies {
    implementation("com.trendyol:quafka:0.1.0")
}
```

### Basic Usage

#### Consumer
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

- [More Consumer Examples](docs/examples/consumers.md)


#### Producer
```kotlin
import com.trendyol.quafka.producer.*
import com.trendyol.quafka.producer.configuration.QuafkaProducerBuilder
import org.apache.kafka.common.serialization.StringSerializer

val props = HashMap<String, Any>()
props[org.apache.kafka.clients.producer.ProducerConfig.BOOTSTRAP_SERVERS_CONFIG] = "localhost:9092"

val producer = QuafkaProducerBuilder<String?, String?>(props)
  .withSerializer(StringSerializer(), StringSerializer())
  .build()

val result: DeliveryResult = producer.send(
  OutgoingMessage.create(
    topic = "test-topic",
    key = "key",
    value = "value",
    headers = emptyList()
  )
)
```

- [More Producer Examples](docs/examples/producers.md)

### Advanced Features

#### Handling Backpressure
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

- [More examples](examples/console/src/main/kotlin/com/trendyol/quafka/examples/console/consumers/)

## [Fundamentals](docs/fundamentals.md)
## [How messages are processed](docs/processing.md)

## Status

> [!WARNING]
> While Quafka is production-ready and extensively used, the API is not yet fully stabilized. Breaking changes may
> occur in minor releases, but migration guides will always be provided.

> Report any issue or bug [in the GitHub repository.](https://github.com/Trendyol/quafka/issues)
> 
## Contributions 
Contributions are welcome! Whether it's:

* 🐛 [Bug reports](https://github.com/Trendyol/quafka/issues)
* 💡 [Feature requests](https://github.com/Trendyol/quafka/issues)
* 📖 Documentation improvements
* 🚀 Code contributions


## License
Quafka is licensed under the MIT License. See the [LICENSE](LICENSE) file for details.



