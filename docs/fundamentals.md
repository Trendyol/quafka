## Fundamentals and Core Concepts

### Architecture overview
- **Builders**: `QuafkaConsumerBuilder` and `QuafkaProducerBuilder` provide fluent, type-safe configuration.
- **Subscriptions**: For each topic you subscribe, you pick exactly one strategy: single or batch.
- **Per-partition workers**: Each assigned topic-partition has a dedicated worker to preserve order.
- **Separated poll and process**: Polling runs independently from processing.
- **Acknowledgment vs commit**:
  - `ack()` marks a message as acknowledged in Quafka’s offset manager.
  - `commit()` writes the offset to Kafka and returns a `Deferred<Unit>`.
  - `autoAckAfterProcess(true)` acknowledges automatically on success.
- **Backpressure**: Configure `withBackpressure(backpressureBufferSize, backpressureReleaseTimeout)` per subscription to pause/resume fetching when the buffer is full.
- **Fallback error handling**: Single/Batch strategies wrap your handler and delegate failures to a fallback error handler; with extensions you can enable in-memory and Kafka-based retries.

### Detailed consumer configuration
- Deserializers: `withDeserializer(key, value)` or provide classes via properties.
- Group/client IDs: `withGroupId(..)`, `withAutoClientId()` or `withClientId(..)`.
- Commit policy: `withCommitOptions(CommitOptions(..))` for periodic/safe commits.
- Poll cadence: `withPollDuration(..)` controls fetch loop cadence.
- Lifecycle: `withStartupOptions(blockOnStart)`, `withGracefulShutdownTimeout(..)`.
- Execution: `withDispatcher(..)`, `withCoroutineExceptionHandler(..)`.
- Observability: `withMessageFormatter(..)`, `withEventBus(..)`.

### Message handling strategies
- `withSingleMessageHandler { msg, ctx -> ... }`
  - Auto-ack optional; manual `msg.ack()` or `msg.commit()` available.
- `withBatchMessageHandler(batchSize, batchTimeout) { messages, ctx -> ... }`
  - Helpers: `messages.ackAll()` and `messages.commitAll()`.
- Per-subscription options (after choosing a handler):
  - `autoAckAfterProcess(Boolean)`
  - `withBackpressure(bufferSize, releaseTimeout)`
  - `withFallbackErrorHandler(handler)`

### Producer essentials
- Serializers: `withSerializer(key, value)`.
- Client ID: `withAutoClientId()` or `withClientId(..)`.
- Send APIs: `send(OutgoingMessage)` or `sendAll(Collection<OutgoingMessage>)` returning `DeliveryResult`.
- Error behavior: `withErrorOptions(ProducingOptions(..))`.
- Message composition: `OutgoingMessage.create(topic, key, value, headers)`; extensions provide `OutgoingMessageBuilder` with JSON serde.

### Extensions (optional, high-level)
- Retry orchestration: `TopicConfiguration` + `subscribeWithErrorHandling(..)` and `RetryPolicy` (NoRetry, InMemoryOnly, NonBlockingOnly, FullRetry).
- Delayed processing: header-based delays and forwarding via `MessageDelayer`.
- Pipeline middleware: `usePipelineMessageHandler { use { envelope, next -> ... } }` to compose reusable steps (logging, tracing, error handling, etc.).

### Rebalancing model (high level)
- Quafka tracks the current Kafka assignment; on change it:
  - Revokes lost partitions: stops workers, flushes offsets, closes resources.
  - Assigns new partitions: creates workers, sets starting offsets, resumes.
  - Updates unchanged partitions: refreshes offsets and resumes if needed.
- Backpressure is considered during pause/resume: partitions may pause fetch when buffered work exceeds thresholds and resume when drained or timeout elapses.

### Learn more
- See processing flow and diagram in [How messages are processed](processing.md).
- See code in [Consumer Examples](examples/consumers.md) and [Producer Examples](examples/producers.md).
