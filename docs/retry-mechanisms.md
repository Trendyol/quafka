# Retry Mechanisms in Quafka

This guide explains how Quafka handles message failures and retries, covering everything from basic concepts to advanced configuration patterns.

## Table of Contents
- [Introduction](#introduction)
- [Core Concepts](#core-concepts)
- [Retry Strategies](#retry-strategies)
- [Policy Configuration](#policy-configuration)
- [Configuration Reference](#configuration-reference)
- [Real-World Scenarios](#real-world-scenarios)
- [Best Practices](#best-practices)
- [Advanced Topics](#advanced-topics)
- [Extensibility](#extensibility)

---

## Introduction

When processing Kafka messages, failures are inevitable. Network issues, database timeouts, rate limits, and transient errors require robust retry mechanisms. Quafka provides a comprehensive retry system that combines:

- **In-Memory Retries**: Fast, synchronous retries within the same consumer instance
- **Non-Blocking Retries**: Asynchronous, Kafka-based retries using retry topics with configurable delays
- **Dead Letter Topics (DLT)**: Final destination for messages that cannot be processed after all retry attempts
- **Policy-Based Configuration**: Fine-grained control over retry behavior based on exception types

### When to Use Retries

| Scenario | Recommended Approach |
|----------|---------------------|
| Transient network errors | In-memory retry (fast, immediate) |
| Database connection issues | Full retry (in-memory + non-blocking) |
| Rate limiting / throttling | Non-blocking with exponential backoff |
| Business validation errors | No retry (direct to DLT) |
| External API timeouts | Non-blocking with custom delays |

---

## Core Concepts

### In-Memory Retry

In-memory retries execute immediately within the consumer process, blocking the partition worker until all attempts are exhausted or the message succeeds.

**Characteristics:**
- ⚡ **Fast**: No Kafka round-trip, immediate retry
- 🔒 **Blocking**: Holds the partition worker (maintains order)
- 💾 **Stateless**: No external state tracking
- ⏱️ **Short-lived**: Best for quick retries (milliseconds to seconds)
- 🎯 **Use for**: Transient errors, connection blips, optimistic locking

**Example:**
```kotlin
RetryPolicy.InMemory(
    identifier = PolicyIdentifier("QuickRetry"),
    config = InMemoryRetryConfig.basic(
        maxAttempts = 3,
        initialDelay = 100.milliseconds
    )
)
```

**Flow:**
```
Message fails → Wait 100ms → Retry #1 → Wait 100ms → Retry #2 → Wait 100ms → Retry #3 → Success or DLT
```

### Non-Blocking Retry

Non-blocking retries republish the message to a Kafka retry topic with a delay header. The consumer continues processing other messages while the delayed message waits.

**Characteristics:**
- 🔓 **Non-Blocking**: Consumer continues processing other messages
- 📊 **Observable**: Retry messages visible in Kafka
- ⏳ **Flexible Delays**: Supports exponential backoff, custom delays
- 🔄 **Persistent**: Survives consumer restarts
- 🎯 **Use for**: Long delays, rate limits, circuit breaker patterns

**Example:**
```kotlin
NonBlockingRetryConfig.exponentialRandomBackoff(
    maxAttempts = 10,
    initialDelay = 1.seconds,
    maxDelay = 5.minutes
)
```

**Flow:**
```
Message fails → Publish to retry topic with delay header → 
Continue processing other messages → 
Delay expires → Consume from retry topic → 
Retry processing
```

### Full Retry (Combined Approach)

Full retry combines both in-memory and non-blocking retries for maximum resilience:

1. **First**: Try in-memory retries (fast recovery for transient errors)
2. **If still failing**: Switch to non-blocking retries (long-term recovery)
3. **If all exhausted**: Send to dead letter topic

**Example:**
```kotlin
RetryPolicy.FullRetry(
    identifier = PolicyIdentifier("ResilientRetry"),
    inMemoryConfig = InMemoryRetryConfig.basic(
        maxAttempts = 3,
        initialDelay = 100.milliseconds
    ),
    nonBlockingConfig = NonBlockingRetryConfig.exponentialRandomBackoff(
        maxAttempts = 10,
        initialDelay = 1.seconds,
        maxDelay = 50.seconds
    )
)
```

**Flow:**
```
Message fails →
In-memory: 3 quick attempts (100ms each) →
Still failing? →
Non-blocking: 10 attempts with exponential backoff (1s → 2s → 4s → ... → 50s) →
Still failing? →
Dead Letter Topic
```

---

## Retry Strategies

Quafka provides four retry strategies that define how retry topics are organized and managed.

### 1. SingleTopicRetry

The simplest strategy: one retry topic for all retry attempts.

**Use Case:**
- Simple applications with uniform retry requirements
- When delay granularity is not important
- Quick setup for development/testing

**Configuration:**
```kotlin
TopicConfiguration(
    topic = "orders",
    retry = TopicRetryStrategy.SingleTopicRetry(
        retryTopic = "orders.retry",
        maxOverallAttempts = 5
    ),
    deadLetterTopic = "orders.dlt"
)
```

**Topic Flow:**
```
orders → orders.retry → orders.dlt
```

**Key Points:**
- All retry messages go to the same topic
- Retry count tracked in message headers
- Simple to set up and monitor

### 2. ExponentialBackoffMultiTopicRetry

Multiple retry topics with increasing delays. Each retry attempt uses a different topic with a progressively longer delay.

**Use Case:**
- Rate limiting scenarios
- Exponential backoff requirements
- Different monitoring/alerting per retry stage

**Configuration:**
```kotlin
TopicConfiguration(
    topic = "payments",
    retry = TopicRetryStrategy.ExponentialBackoffMultiTopicRetry(
        maxOverallAttempts = 4,
        retryTopics = listOf(
            DelayTopicConfiguration("payments.retry.10s", 10.seconds),
            DelayTopicConfiguration("payments.retry.30s", 30.seconds),
            DelayTopicConfiguration("payments.retry.1m", 1.minutes),
            DelayTopicConfiguration("payments.retry.5m", 5.minutes)
        )
    ),
    deadLetterTopic = "payments.dlt"
)
```

**Topic Flow:**
```
payments → 
payments.retry.10s (10s delay) → 
payments.retry.30s (30s delay) → 
payments.retry.1m (1m delay) → 
payments.retry.5m (5m delay) → 
payments.dlt
```

**Key Points:**
- Each retry attempt uses a different topic
- Delays configured per topic
- Better observability (can monitor each retry stage separately)

### 3. ExponentialBackoffToSingleTopicRetry

Delay topics for managing backoff, but all actual retries consume from a single retry topic.

**Use Case:**
- Centralized retry processing with controlled delays
- When you want separation between delay management and retry processing
- Complex retry workflows

**Configuration:**
```kotlin
TopicConfiguration(
    topic = "notifications",
    retry = TopicRetryStrategy.ExponentialBackoffToSingleTopicRetry(
        maxOverallAttempts = 4,
        retryTopic = "notifications.retry",
        delayTopics = listOf(
            DelayTopicConfiguration("notifications.delay.10s", 10.seconds),
            DelayTopicConfiguration("notifications.delay.30s", 30.seconds),
            DelayTopicConfiguration("notifications.delay.1m", 1.minutes),
            DelayTopicConfiguration("notifications.delay.5m", 5.minutes)
        )
    ),
    deadLetterTopic = "notifications.dlt"
)
```

**Topic Flow:**
```
notifications → 
notifications.delay.10s (10s) → notifications.retry → 
notifications.delay.30s (30s) → notifications.retry → 
notifications.delay.1m (1m) → notifications.retry → 
notifications.delay.5m (5m) → notifications.retry → 
notifications.dlt
```

**Key Points:**
- Delay topics handle timing
- Single retry topic for actual processing
- Consumer only needs to subscribe to retry topic (not delay topics)
- Useful when delay topics are managed by a separate delay service

### 4. NoneStrategy

No non-blocking retry topics. Only in-memory retries are available (if configured via policies).

**Use Case:**
- When Kafka-based retries are not desired
- Quick failure detection
- Stateless processing

**Configuration:**
```kotlin
TopicConfiguration(
    topic = "events",
    retry = TopicRetryStrategy.NoneStrategy,
    deadLetterTopic = "events.dlt"
)
```

**Topic Flow:**
```
events → (in-memory retries only) → events.dlt
```

**Key Points:**
- No retry topics created
- Only in-memory retries via policy configuration
- Fastest failure handling

---

## Policy Configuration

Policies define **how** to handle specific exceptions. Quafka supports two approaches for configuring retry policies.

### Approach 1: Global Policy Provider

Define retry policies centrally using `withPolicyProvider`. This approach inspects the exception and returns the appropriate policy.

**When to Use:**
- Policies apply across multiple topics uniformly
- Centralized exception handling logic
- Dynamic policy selection based on runtime conditions

**Example:**
```kotlin
QuafkaConsumerBuilder<String, String>(properties)
    .withDeserializer(StringDeserializer())
    .subscribeWithErrorHandling(topics, danglingTopic, producer) {
        withRecoverable {
            withPolicyProvider { message, context, exceptionDetails, topicConfig ->
                when (exceptionDetails.exception) {
                    is DatabaseConcurrencyException -> RetryPolicy.FullRetry(
                        identifier = PolicyIdentifier("DbConcurrency"),
                        inMemoryConfig = InMemoryRetryConfig.basic(3, 100.milliseconds),
                        nonBlockingConfig = NonBlockingRetryConfig.exponentialRandomBackoff(
                            maxAttempts = 10,
                            initialDelay = 1.seconds,
                            maxDelay = 50.seconds
                        )
                    )
                    
                    is RateLimitException -> RetryPolicy.NonBlockingOnly(
                        identifier = PolicyIdentifier("RateLimit"),
                        config = NonBlockingRetryConfig.exponentialRandomBackoff(
                            maxAttempts = 20,
                            initialDelay = 60.seconds,
                            maxDelay = 30.minutes
                        )
                    )
                    
                    is ValidationException -> RetryPolicy.NoRetry
                    
                    else -> RetryPolicy.InMemory(
                        identifier = PolicyIdentifiers.forException(exceptionDetails.exception),
                        config = InMemoryRetryConfig.basic(3, 100.milliseconds)
                    )
                }
            }
        }
        .withSingleMessageHandler { message, context ->
            // Your processing logic
        }
    }
```

**Policy Resolution Order:**
1. Check topic's `policyBuilder` (if configured)
2. Check global `withPolicyProvider`
3. Use `DefaultPolicy` (3 in-memory retries)

### Approach 2: Topic-Level Policy Builder

> **⚠️ Alpha Feature Warning**
>
> The `policyBuilder` parameter and related methods (`onException`, `onCondition`) are in alpha state.
> Their API may change in future releases.

Define policies directly in the topic configuration using `policyBuilder`. This provides a DSL for exception-specific retry behavior.

**When to Use:**
- Policies vary per topic
- Reusable exception handling patterns
- Cleaner separation of concerns
- Dynamic policy identifiers

**Example:**
```kotlin
// Define reusable policy patterns
fun TopicRetryStrategy<RetryPolicyBuilder.Full>.retryOnCommonErrors() {
    // Specific exception with full retry
    onException<DatabaseConcurrencyException>(
        PolicyIdentifiers.forException<DatabaseConcurrencyException>()
    ) {
        inMemoryConfig = InMemoryRetryConfig.basic(
            maxAttempts = 3,
            initialDelay = 100.milliseconds
        )
        nonBlockingConfig = NonBlockingRetryConfig.exponentialRandomBackoff(
            maxAttempts = 10,
            initialDelay = 1.seconds,
            maxDelay = 50.seconds
        )
    }
    
    // Business exception - no retry
    .onException<ValidationException> {
        dontRetry()  // Will go directly to DLT
    }
    
    // Catch-all with dynamic identifier
    .onException<Throwable>(
        identifierFactory = { ex -> PolicyIdentifiers.forException(ex) }
    ) {
        inMemoryConfig = InMemoryRetryConfig.basic(
            maxAttempts = 3,
            initialDelay = 100.milliseconds
        )
    }
}

// Use in topic configuration
val ordersTopic = TopicConfiguration(
    topic = "orders",
    retry = TopicRetryStrategy.SingleTopicRetry(
        retryTopic = "orders.retry",
        maxOverallAttempts = 5
    ),
    deadLetterTopic = "orders.dlt",
    policyBuilder = {
        retryOnCommonErrors()  // Apply reusable policies
        
        // Add topic-specific policies
        onException<PaymentException> {
            nonBlockingConfig = NonBlockingRetryConfig.exponentialRandomBackoff(
                maxAttempts = 15,
                initialDelay = 5.seconds,
                maxDelay = 10.minutes
            )
        }
    }
)
```

**Policy Identifiers:**

Identifiers uniquely identify retry sessions for tracking and observability.

```kotlin
// Static identifier (known exception type)
PolicyIdentifiers.forException<DatabaseException>()
// → PolicyIdentifier("DatabaseException")

// Dynamic identifier (runtime exception type)
identifierFactory = { ex -> PolicyIdentifiers.forException(ex) }
// For IOException → PolicyIdentifier("IOException")
// For SQLException → PolicyIdentifier("SQLException")
```

**Dynamic identifiers** are powerful for catch-all handlers because each exception type gets unique tracking.

**Condition-Based Policies:**

For complex matching beyond exception types:

```kotlin
policyBuilder = {
    onCondition(
        identifierFactory = { PolicyIdentifier("RateLimited") },
        predicate = { message, context, exceptionDetails ->
            exceptionDetails.exception is HttpException && 
            exceptionDetails.exception.statusCode == 429
        }
    ) {
        nonBlockingConfig = NonBlockingRetryConfig.exponentialRandomBackoff(
            maxAttempts = 20,
            initialDelay = 60.seconds,
            maxDelay = 30.minutes
        )
    }
}
```

---

## Configuration Reference

### InMemoryRetryConfig

Configures synchronous, in-memory retry behavior.

**Basic Configuration:**
```kotlin
InMemoryRetryConfig.basic(
    maxAttempts = 3,
    initialDelay = 100.milliseconds
)
```

**Custom Configuration:**
```kotlin
InMemoryRetryConfig(
    maxAttempts = 5,
    delayCalculator = { attempt ->
        (100.milliseconds.inWholeMilliseconds * (attempt + 1)).milliseconds
    }
)
```

**Delay Strategies:**
- **Fixed**: Same delay between attempts
- **Linear**: Increasing delay (100ms, 200ms, 300ms, ...)
- **Custom**: Implement your own `delayCalculator`

### NonBlockingRetryConfig

Configures Kafka-based retry with delay topics.

**Exponential Random Backoff:**
```kotlin
NonBlockingRetryConfig.exponentialRandomBackoff(
    maxAttempts = 10,
    initialDelay = 1.seconds,
    maxDelay = 5.minutes
)
```

**Formula:** `min(initialDelay * 2^attempt + random(0, initialDelay), maxDelay)`

**Example Delays:**
- Attempt 1: 1s + random(0, 1s) ≈ 1-2s
- Attempt 2: 2s + random(0, 1s) ≈ 2-3s
- Attempt 3: 4s + random(0, 1s) ≈ 4-5s
- Attempt 4: 8s + random(0, 1s) ≈ 8-9s
- ...
- Attempt N: capped at maxDelay

**Custom Delay Function:**
```kotlin
NonBlockingRetryConfig.custom(
    maxAttempts = 15,
    delayFn = { attempt, exceptionDetails ->
        when {
            attempt < 3 -> 10.seconds
            attempt < 7 -> 1.minutes
            else -> 5.minutes
        }
    }
)
```

### RetryPolicy Types

**NoRetry:**
```kotlin
RetryPolicy.NoRetry
```
Immediately sends message to DLT without any retries.

**InMemory:**
```kotlin
RetryPolicy.InMemory(
    identifier = PolicyIdentifier("QuickRetry"),
    config = InMemoryRetryConfig.basic(3, 100.milliseconds)
)
```
Only in-memory retries. If all attempts fail, sends to DLT.

**NonBlockingOnly:**
```kotlin
RetryPolicy.NonBlockingOnly(
    identifier = PolicyIdentifier("DelayedRetry"),
    config = NonBlockingRetryConfig.exponentialRandomBackoff(
        maxAttempts = 10,
        initialDelay = 1.seconds,
        maxDelay = 5.minutes
    )
)
```
Only Kafka-based retries with delays. If all attempts fail, sends to DLT.

**FullRetry:**
```kotlin
RetryPolicy.FullRetry(
    identifier = PolicyIdentifier("ResilientRetry"),
    inMemoryConfig = InMemoryRetryConfig.basic(3, 100.milliseconds),
    nonBlockingConfig = NonBlockingRetryConfig.exponentialRandomBackoff(
        maxAttempts = 10,
        initialDelay = 1.seconds,
        maxDelay = 50.seconds
    )
)
```
First tries in-memory, then non-blocking if still failing.

---

## Real-World Scenarios

### Scenario 1: E-Commerce Order Processing

**Requirements:**
- Fast retry for optimistic locking conflicts
- Long retry for payment gateway timeouts
- No retry for invalid orders

**Solution:**
```kotlin
val ordersTopic = TopicConfiguration(
    topic = "orders",
    retry = TopicRetryStrategy.ExponentialBackoffMultiTopicRetry(
        maxOverallAttempts = 5,
        retryTopics = listOf(
            DelayTopicConfiguration("orders.retry.10s", 10.seconds),
            DelayTopicConfiguration("orders.retry.1m", 1.minutes),
            DelayTopicConfiguration("orders.retry.5m", 5.minutes),
            DelayTopicConfiguration("orders.retry.15m", 15.minutes),
            DelayTopicConfiguration("orders.retry.30m", 30.minutes)
        )
    ),
    deadLetterTopic = "orders.dlt",
    policyBuilder = {
        // Database locking: quick in-memory retry
        onException<OptimisticLockException>(PolicyIdentifier("OptimisticLock")) {
            inMemoryConfig = InMemoryRetryConfig.basic(5, 50.milliseconds)
        }
        
        // Payment gateway timeout: full retry with backoff
        onException<PaymentTimeoutException>(PolicyIdentifier("PaymentTimeout")) {
            inMemoryConfig = InMemoryRetryConfig.basic(2, 100.milliseconds)
            nonBlockingConfig = NonBlockingRetryConfig.exponentialRandomBackoff(
                maxAttempts = 10,
                initialDelay = 5.seconds,
                maxDelay = 30.minutes
            )
        }
        
        // Invalid order: don't retry
        onException<OrderValidationException> {
            dontRetry()
        }
    }
)
```

### Scenario 2: External API Integration with Rate Limiting

**Requirements:**
- Respect API rate limits (429 responses)
- Long delays between retries
- Different handling for client errors vs server errors

**Solution:**
```kotlin
val apiEventsTopic = TopicConfiguration(
    topic = "api-events",
    retry = TopicRetryStrategy.SingleTopicRetry(
        retryTopic = "api-events.retry",
        maxOverallAttempts = 20
    ),
    deadLetterTopic = "api-events.dlt",
    policyBuilder = {
        // Rate limiting: long exponential backoff
        onCondition(
            identifierFactory = { PolicyIdentifier("RateLimit") },
            predicate = { message, context, exceptionDetails ->
                exceptionDetails.exception is ApiException &&
                exceptionDetails.exception.statusCode == 429
            }
        ) {
            nonBlockingConfig = NonBlockingRetryConfig.exponentialRandomBackoff(
                maxAttempts = 20,
                initialDelay = 60.seconds,
                maxDelay = 30.minutes
            )
        }
        
        // Server errors (5xx): retry with backoff
        onCondition(
            identifierFactory = { ex -> PolicyIdentifier("ServerError-${(ex as? ApiException)?.statusCode}") },
            predicate = { _, _, exceptionDetails ->
                exceptionDetails.exception is ApiException &&
                exceptionDetails.exception.statusCode in 500..599
            }
        ) {
            inMemoryConfig = InMemoryRetryConfig.basic(3, 200.milliseconds)
            nonBlockingConfig = NonBlockingRetryConfig.exponentialRandomBackoff(
                maxAttempts = 15,
                initialDelay = 10.seconds,
                maxDelay = 10.minutes
            )
        }
        
        // Client errors (4xx): don't retry
        onCondition(
            identifierFactory = { PolicyIdentifier("ClientError") },
            predicate = { _, _, exceptionDetails ->
                exceptionDetails.exception is ApiException &&
                exceptionDetails.exception.statusCode in 400..499
            }
        ) {
            dontRetry()
        }
    }
)
```

### Scenario 3: Database Synchronization

**Requirements:**
- Quick retry for connection issues
- No retry for constraint violations
- Exponential backoff for deadlocks

**Solution:**
```kotlin
val syncTopic = TopicConfiguration(
    topic = "db-sync",
    retry = TopicRetryStrategy.ExponentialBackoffToSingleTopicRetry(
        maxOverallAttempts = 10,
        retryTopic = "db-sync.retry",
        delayTopics = listOf(
            DelayTopicConfiguration("db-sync.delay.1s", 1.seconds),
            DelayTopicConfiguration("db-sync.delay.5s", 5.seconds),
            DelayTopicConfiguration("db-sync.delay.15s", 15.seconds),
            DelayTopicConfiguration("db-sync.delay.1m", 1.minutes),
            DelayTopicConfiguration("db-sync.delay.5m", 5.minutes)
        )
    ),
    deadLetterTopic = "db-sync.dlt",
    policyBuilder = {
        // Connection issues: quick in-memory retry
        onException<SQLException>(
            identifierFactory = { ex -> 
                val sqlEx = ex as SQLException
                when (sqlEx.sqlState) {
                    "08000", "08001", "08003", "08004", "08006" -> 
                        PolicyIdentifier("ConnectionError")
                    else -> 
                        PolicyIdentifier("SQLException-${sqlEx.errorCode}")
                }
            }
        ) {
            inMemoryConfig = InMemoryRetryConfig.basic(5, 200.milliseconds)
            nonBlockingConfig = NonBlockingRetryConfig.custom(
                maxAttempts = 10,
                delayFn = { attempt, _ ->
                    when (attempt) {
                        0, 1 -> 1.seconds
                        2, 3 -> 5.seconds
                        4, 5 -> 15.seconds
                        else -> 1.minutes
                    }
                }
            )
        }
        
        // Constraint violations: don't retry
        onCondition(
            identifierFactory = { PolicyIdentifier("ConstraintViolation") },
            predicate = { _, _, exceptionDetails ->
                exceptionDetails.exception is SQLException &&
                (exceptionDetails.exception as SQLException).sqlState?.startsWith("23") == true
            }
        ) {
            dontRetry()
        }
    }
)
```

### Scenario 4: Notification Service

**Requirements:**
- Multiple notification channels (email, SMS, push)
- Channel-specific retry strategies
- Fallback to alternative channels

**Solution:**
```kotlin
val notificationsTopic = TopicConfiguration(
    topic = "notifications",
    retry = TopicRetryStrategy.ExponentialBackoffMultiTopicRetry(
        maxOverallAttempts = 8,
        retryTopics = listOf(
            DelayTopicConfiguration("notifications.retry.10s", 10.seconds),
            DelayTopicConfiguration("notifications.retry.30s", 30.seconds),
            DelayTopicConfiguration("notifications.retry.1m", 1.minutes),
            DelayTopicConfiguration("notifications.retry.5m", 5.minutes),
            DelayTopicConfiguration("notifications.retry.15m", 15.minutes),
            DelayTopicConfiguration("notifications.retry.30m", 30.minutes),
            DelayTopicConfiguration("notifications.retry.1h", 1.hours),
            DelayTopicConfiguration("notifications.retry.2h", 2.hours)
        )
    ),
    deadLetterTopic = "notifications.dlt",
    policyBuilder = {
        // Email service: moderate retry
        onException<EmailServiceException>(PolicyIdentifier("EmailService")) {
            inMemoryConfig = InMemoryRetryConfig.basic(3, 100.milliseconds)
            nonBlockingConfig = NonBlockingRetryConfig.exponentialRandomBackoff(
                maxAttempts = 8,
                initialDelay = 10.seconds,
                maxDelay = 2.hours
            )
        }
        
        // SMS service: quick retry (expensive)
        onException<SmsServiceException>(PolicyIdentifier("SmsService")) {
            inMemoryConfig = InMemoryRetryConfig.basic(2, 50.milliseconds)
            nonBlockingConfig = NonBlockingRetryConfig.custom(
                maxAttempts = 5,
                delayFn = { attempt, _ ->
                    when (attempt) {
                        0 -> 30.seconds
                        1 -> 2.minutes
                        2 -> 5.minutes
                        3 -> 15.minutes
                        else -> 30.minutes
                    }
                }
            )
        }
        
        // Push notification: aggressive retry (cheap)
        onException<PushServiceException>(PolicyIdentifier("PushService")) {
            inMemoryConfig = InMemoryRetryConfig.basic(5, 200.milliseconds)
            nonBlockingConfig = NonBlockingRetryConfig.exponentialRandomBackoff(
                maxAttempts = 10,
                initialDelay = 5.seconds,
                maxDelay = 1.hours
            )
        }
    }
)
```

---

## Best Practices

### 1. Choose the Right Retry Type

- **In-Memory**: Transient errors, fast recovery needed, order must be maintained
- **Non-Blocking**: Long delays, rate limits, circuit breaker patterns
- **Full**: Critical operations that need maximum resilience
- **None**: Business validation errors, permanent failures

### 2. Set Appropriate Timeouts

```kotlin
// ❌ Bad: Too aggressive, will waste resources
InMemoryRetryConfig.basic(maxAttempts = 20, initialDelay = 10.milliseconds)

// ✅ Good: Reasonable attempts with appropriate delays
InMemoryRetryConfig.basic(maxAttempts = 3, initialDelay = 100.milliseconds)

// ❌ Bad: Delays too short for rate limiting
NonBlockingRetryConfig.exponentialRandomBackoff(
    maxAttempts = 10,
    initialDelay = 100.milliseconds,
    maxDelay = 1.seconds
)

// ✅ Good: Appropriate delays for rate limits
NonBlockingRetryConfig.exponentialRandomBackoff(
    maxAttempts = 20,
    initialDelay = 60.seconds,
    maxDelay = 30.minutes
)
```

### 3. Use Meaningful Policy Identifiers

```kotlin
// ❌ Bad: Generic, not descriptive
PolicyIdentifier("retry1")

// ✅ Good: Clear and specific
PolicyIdentifier("DatabaseConcurrencyException")
PolicyIdentifiers.forException<PaymentTimeoutException>()

// ✅ Good: Dynamic with context
identifierFactory = { ex -> 
    PolicyIdentifier("ApiError-${(ex as? ApiException)?.statusCode ?: "unknown"}")
}
```

### 4. Don't Retry Business Errors

```kotlin
// ❌ Bad: Retrying validation errors
onException<ValidationException> {
    inMemoryConfig = InMemoryRetryConfig.basic(3, 100.milliseconds)
}

// ✅ Good: Send validation errors directly to DLT
onException<ValidationException> {
    dontRetry()
}
```

### 5. Set maxOverallAttempts Appropriately

Understanding the relationship between **overall attempts** (topic strategy level) and **policy attempts** (exception policy level) is crucial for correct retry configuration.

#### Overall Attempts vs Policy Attempts

**Overall Attempts** (`maxOverallAttempts` in `TopicRetryStrategy`):
- Controls the **maximum number of times** a message can be sent to retry topics
- Acts as a **safety limit** across all retry policies for that topic
- Prevents infinite retry loops regardless of policy configuration
- Applies **per message**, not per policy

**Policy Attempts** (`maxAttempts` in retry configs):
- Controls retry behavior **for a specific exception type**
- Can vary per exception (some exceptions retry more, some less)
- Works **within** the overall attempts boundary

**Key Rule:** `maxOverallAttempts` should be >= the highest `maxAttempts` in any policy for that topic.

#### Why PolicyIdentifier is Needed

`PolicyIdentifier` serves several critical purposes:

1. **Policy-Specific Retry Tracking**: Each policy tracks its own retry count based on the identifier. When the identifier changes (different exception), the policy-specific retry count **resets to zero**, allowing the new policy's retry logic to apply fresh.

2. **Preventing Infinite Loops with Total Retry Tracking**: While policy-specific counts reset on identifier change, Quafka maintains a **total retry count** that never resets and is capped by `maxOverallAttempts`. This prevents infinite retry loops when exceptions keep changing.

3. **Observability**: Identifiers appear in monitoring, logs, and headers, making it easy to trace which retry policy is currently handling a message and detect when policies switch.

4. **Header Management**: Identifiers are stored in message headers to preserve retry state across Kafka topics and consumer restarts.

#### How Identifier Changes Affect Retry Counting

Consider this scenario where exception types change during retries:

```kotlin
TopicConfiguration(
    topic = "orders",
    retry = TopicRetryStrategy.SingleTopicRetry(
        retryTopic = "orders.retry",
        maxOverallAttempts = 10  // Total safety limit
    ),
    policyBuilder = {
        onException<SimpleException>(PolicyIdentifier("Simple")) {
            nonBlockingConfig = NonBlockingRetryConfig.basic(maxAttempts = 2)
        }
        
        onException<ConcurrencyException>(PolicyIdentifier("Concurrency")) {
            nonBlockingConfig = NonBlockingRetryConfig.basic(maxAttempts = 3)
        }
    }
)

// Execution flow when exceptions change:
// 
// Attempt 1: Fails with SimpleException
//   - Identifier: "Simple"
//   - Policy retry count: 0
//   - Total retry count: 1
//   - Action: Publish to retry topic
//
// Attempt 2: Fails with ConcurrencyException (identifier changed!)
//   - Identifier: "Concurrency" (new identifier)
//   - Policy retry count: 0 (RESET because identifier changed)
//   - Total retry count: 2 (keeps incrementing)
//   - Action: Publish to retry topic
//
// Attempt 3: Fails with ConcurrencyException
//   - Identifier: "Concurrency"
//   - Policy retry count: 1
//   - Total retry count: 3
//   - Action: Publish to retry topic
//
// Attempt 4: Fails with SimpleException (identifier changed back!)
//   - Identifier: "Simple" (switched back)
//   - Policy retry count: 0 (RESET again)
//   - Total retry count: 4
//   - Action: Publish to retry topic
//
// ... continues until total retry count reaches maxOverallAttempts (10)
// Then: Send to DLT regardless of policy-specific retry counts
```

**Key Insight:** Without `PolicyIdentifier`:
- We couldn't detect when exception types change
- All exceptions would share one counter
- We couldn't apply different retry strategies per exception type

**With `PolicyIdentifier`:**
- Each exception type can have its own retry strategy
- Policy-specific counter resets when exception changes (allowing new policy to apply)
- Total retry counter prevents infinite loops (even if exceptions keep changing)
- `maxOverallAttempts` provides the ultimate safety net

**Why This Design Matters:**

Without this two-level counting (policy-specific + total), you could have infinite retry loops:

```kotlin
// Dangerous scenario without total retry counting:
// 1. SimpleException fails → retry 2 times → exhausted
// 2. Exception changes to ConcurrencyException → reset counter → retry 3 times
// 3. Exception changes back to SimpleException → reset counter → retry 2 times
// 4. Back to ConcurrencyException → reset counter → retry 3 times
// 5. Infinite loop! 🔄
//
// Solution: maxOverallAttempts caps the total, preventing infinite loops
// After 10 total attempts → DLT (regardless of identifier changes)
```

#### Example: How Overall Attempts Caps Policy Attempts

```kotlin
// ❌ Bad: Mismatch between policy and strategy
TopicConfiguration(
    topic = "orders",
    retry = TopicRetryStrategy.SingleTopicRetry(
        retryTopic = "orders.retry",
        maxOverallAttempts = 3  // Topic-level limit
    ),
    policyBuilder = {
        onException<DatabaseException>(PolicyIdentifier("DbException")) {
            // Policy wants 10 attempts
            nonBlockingConfig = NonBlockingRetryConfig.exponentialRandomBackoff(
                maxAttempts = 10  // ⚠️ Will be capped at 3 by maxOverallAttempts!
            )
        }
    }
)

// What happens:
// - Message fails with DatabaseException
// - Policy wants to retry 10 times
// - Topic strategy only allows 3 overall attempts
// - After 3 retry publishes → sent to DLT (not 10!)
```

**Why the cap exists:** It prevents a single policy from overwhelming retry topics. If you have multiple exception types with different policies, the overall attempts ensure total retry publishes stay bounded.

```kotlin
// ✅ Good: Strategy max >= policy max
TopicConfiguration(
    topic = "orders",
    retry = TopicRetryStrategy.SingleTopicRetry(
        retryTopic = "orders.retry",
        maxOverallAttempts = 15  // Higher than any policy
    ),
    policyBuilder = {
        onException<DatabaseException>(PolicyIdentifier("DbException")) {
            nonBlockingConfig = NonBlockingRetryConfig.exponentialRandomBackoff(
                maxAttempts = 10  // ✅ Has room to retry
            )
        }
        
        onException<ApiException>(PolicyIdentifier("ApiException")) {
            nonBlockingConfig = NonBlockingRetryConfig.exponentialRandomBackoff(
                maxAttempts = 15  // ✅ Can use full allowance
            )
        }
    }
)
```

#### How In-Memory and Non-Blocking Attempts Interact

In-memory retries **do not count** toward `maxOverallAttempts`. Only non-blocking retry publishes do.

```kotlin
TopicConfiguration(
    topic = "payments",
    retry = TopicRetryStrategy.SingleTopicRetry(
        retryTopic = "payments.retry",
        maxOverallAttempts = 5
    ),
    policyBuilder = {
        onException<PaymentException>(PolicyIdentifier("Payment")) {
            inMemoryConfig = InMemoryRetryConfig.basic(
                maxAttempts = 3  // These don't count toward maxOverallAttempts
            )
            nonBlockingConfig = NonBlockingRetryConfig.exponentialRandomBackoff(
                maxAttempts = 5  // These DO count toward maxOverallAttempts
            )
        }
    }
)

// Actual flow:
// 1. Message fails
// 2. In-memory retry: 3 attempts (immediate, within consumer)
// 3. Still failing → Non-blocking retry begins
// 4. Non-blocking retry: 5 attempts (published to retry topic)
//    - Each publish counted toward maxOverallAttempts
// 5. After 5 non-blocking attempts → DLT
//
// Total processing attempts: 1 (original) + 3 (in-memory) + 5 (non-blocking) = 9 attempts
// But maxOverallAttempts only tracks non-blocking: 5
```

#### Best Practice: Set Overall Attempts Generously

```kotlin
// ✅ Recommended pattern
val maxPolicyAttempts = 20  // Highest policy attempt count

TopicConfiguration(
    topic = "critical-service",
    retry = TopicRetryStrategy.ExponentialBackoffMultiTopicRetry(
        maxOverallAttempts = maxPolicyAttempts + 5,  // Add buffer
        retryTopics = /* ... */
    ),
    policyBuilder = {
        onException<TransientException>(PolicyIdentifier("Transient")) {
            nonBlockingConfig = NonBlockingRetryConfig.exponentialRandomBackoff(
                maxAttempts = 10
            )
        }
        
        onException<RateLimitException>(PolicyIdentifier("RateLimit")) {
            nonBlockingConfig = NonBlockingRetryConfig.exponentialRandomBackoff(
                maxAttempts = 20  // Highest policy
            )
        }
    }
)
```

### 6. Use Reusable Policy Functions

```kotlin
// ✅ Good: Reusable, testable, maintainable
fun TopicRetryStrategy<RetryPolicyBuilder.Full>.standardRetryPolicy() {
    onException<DatabaseException>(PolicyIdentifier("Database")) {
        inMemoryConfig = InMemoryRetryConfig.basic(3, 100.milliseconds)
        nonBlockingConfig = NonBlockingRetryConfig.exponentialRandomBackoff(
            maxAttempts = 10,
            initialDelay = 1.seconds,
            maxDelay = 5.minutes
        )
    }
    .onException<ValidationException> {
        dontRetry()
    }
}

// Apply to multiple topics
val topic1 = TopicConfiguration(
    topic = "orders",
    retry = /* ... */,
    deadLetterTopic = "orders.dlt",
    policyBuilder = { standardRetryPolicy() }
)

val topic2 = TopicConfiguration(
    topic = "payments",
    retry = /* ... */,
    deadLetterTopic = "payments.dlt",
    policyBuilder = { 
        standardRetryPolicy()
        // Add topic-specific policies
        onException<PaymentGatewayException> { /* ... */ }
    }
)
```

---

## Advanced Topics

### Message Modification on Retry

Modify messages before sending to retry topics (e.g., add tracking headers):

```kotlin
withRecoverable {
    withOutgoingMessageModifier { message, context, exceptionDetails ->
        val newHeaders = headers.toMutableList()
        newHeaders.add(header("X-Original-Topic", message.topic))
        newHeaders.add(header("X-Exception-Type", exceptionDetails.exception::class.simpleName!!))
        newHeaders.add(header("X-Retry-Timestamp", System.currentTimeMillis().toString()))
        copy(headers = newHeaders)
    }
}
```

### Custom Exception Details

Provide rich exception information for debugging:

```kotlin
withRecoverable {
    withExceptionDetailsProvider { throwable ->
        ExceptionDetails(
            exception = throwable,
            details = buildString {
                appendLine("Exception: ${throwable::class.simpleName}")
                appendLine("Message: ${throwable.message}")
                appendLine("Stack trace:")
                appendLine(throwable.stackTraceToString())
                
                // Add custom context
                appendLine("Environment: ${System.getenv("ENV")}")
                appendLine("Host: ${InetAddress.getLocalHost().hostName}")
            }
        )
    }
}
```

### Custom Delay Functions

Implement sophisticated delay strategies:

```kotlin
// Business hours aware delays
val businessHoursDelayFn: DelayFn = { attempt, exceptionDetails ->
    val now = LocalDateTime.now()
    val hour = now.hour
    
    val baseDelay = when (attempt) {
        0 -> 1.minutes
        1 -> 5.minutes
        2 -> 15.minutes
        else -> 30.minutes
    }
    
    // Add extra delay if outside business hours
    if (hour < 9 || hour > 17) {
        baseDelay * 2
    } else {
        baseDelay
    }
}

NonBlockingRetryConfig.custom(
    maxAttempts = 10,
    delayFn = businessHoursDelayFn
)
```

### Dangling Topic Handling

Messages that don't match any configured topic go to the dangling topic:

```kotlin
subscribeWithErrorHandling(
    topics = listOf(topic1, topic2),
    danglingTopic = "unconfigured.messages",
    producer = producer
) {
    // ...
}
```

**Use Cases:**
- Catch misconfigured topics early
- Monitor unexpected traffic
- Debug routing issues

### Combining Policy Provider and Policy Builder

Both can be used together. Resolution order:

1. Topic's `policyBuilder` (checked first)
2. Global `withPolicyProvider` (fallback)
3. `DefaultPolicy` (3 in-memory retries)

```kotlin
subscribeWithErrorHandling(topics, danglingTopic, producer) {
    withRecoverable {
        // Global fallback for all topics
        withPolicyProvider { message, context, exceptionDetails, topicConfig ->
            when (exceptionDetails.exception) {
                is CriticalException -> RetryPolicy.NoRetry
                else -> null  // Let topic policyBuilder handle it
            }
        }
    }
    .withSingleMessageHandler { message, context ->
        // ...
    }
}

// Topic-specific policies take precedence
TopicConfiguration(
    topic = "orders",
    retry = /* ... */,
    deadLetterTopic = "orders.dlt",
    policyBuilder = {
        onException<OrderException> {
            // This will be checked before global withPolicyProvider
        }
    }
)
```

---

## Extensibility

### Custom Message Delayer

The `MessageDelayer` component manages message delays. You can implement custom delay strategies:

```kotlin
class CustomMessageDelayer : MessageDelayer {
    override suspend fun calculateDelay(
        message: IncomingMessage<*, *>,
        context: ConsumerContext
    ): Duration? {
        // Custom delay calculation logic
        val customHeader = message.headers.find { it.key == "X-Custom-Delay" }
        return customHeader?.value?.toString()?.toLongOrNull()?.milliseconds
    }
    
    override suspend fun delayMessage(
        message: OutgoingMessage<*, *>,
        delay: Duration,
        targetTopic: String
    ): OutgoingMessage<*, *> {
        // Custom delay header injection
        val headers = message.headers + header("X-Delayed-Until", (System.currentTimeMillis() + delay.inWholeMilliseconds).toString())
        return message.copy(headers = headers)
    }
}

// Use custom delayer
withRecoverable {
    withMessageDelayer(CustomMessageDelayer())
}
```

### Custom Retry Policy Provider

Implement complex policy resolution logic:

```kotlin
class DynamicPolicyProvider(
    private val configService: ConfigService
) {
    suspend fun provide(
        message: IncomingMessage<*, *>,
        context: ConsumerContext,
        exceptionDetails: ExceptionDetails,
        topicConfig: TopicConfiguration<*>
    ): RetryPolicy {
        // Fetch retry configuration from external service
        val config = configService.getRetryConfig(
            topic = message.topic,
            exceptionType = exceptionDetails.exception::class.simpleName!!
        )
        
        return when (config.strategy) {
            "aggressive" -> RetryPolicy.FullRetry(
                identifier = PolicyIdentifier(config.id),
                inMemoryConfig = InMemoryRetryConfig.basic(config.inMemoryAttempts, config.inMemoryDelay),
                nonBlockingConfig = NonBlockingRetryConfig.exponentialRandomBackoff(
                    maxAttempts = config.nonBlockingAttempts,
                    initialDelay = config.initialDelay,
                    maxDelay = config.maxDelay
                )
            )
            "passive" -> RetryPolicy.InMemory(
                identifier = PolicyIdentifier(config.id),
                config = InMemoryRetryConfig.basic(config.inMemoryAttempts, config.inMemoryDelay)
            )
            "none" -> RetryPolicy.NoRetry
            else -> DefaultPolicy
        }
    }
}

// Use custom provider
val policyProvider = DynamicPolicyProvider(configService)

withRecoverable {
    withPolicyProvider { message, context, exceptionDetails, topicConfig ->
        policyProvider.provide(message, context, exceptionDetails, topicConfig)
    }
}
```

### Middleware Integration

Combine retry mechanisms with custom middleware:

```kotlin
usePipelineMessageHandler {
    // Custom logging middleware
    use { envelope, next ->
        logger.info("Processing: ${envelope.message.key}")
        val startTime = System.currentTimeMillis()
        
        try {
            next(envelope)
            logger.info("Success in ${System.currentTimeMillis() - startTime}ms")
        } catch (e: Exception) {
            logger.error("Failed in ${System.currentTimeMillis() - startTime}ms", e)
            throw e
        }
    }
    
    // Retry middleware (from subscribeWithErrorHandling)
    useRetryMiddleware()
    
    // Custom circuit breaker middleware
    use { envelope, next ->
        if (circuitBreaker.isOpen) {
            throw CircuitBreakerOpenException()
        }
        
        try {
            next(envelope)
            circuitBreaker.recordSuccess()
        } catch (e: Exception) {
            circuitBreaker.recordFailure()
            throw e
        }
    }
    
    // Actual message processing
    use { envelope, _ ->
        processMessage(envelope.message)
    }
}
```

### Event-Driven Monitoring

Subscribe to retry events for monitoring and alerting:

```kotlin
withEventBus { event ->
    when (event) {
        is RecovererEvent.RetryMessagePublished -> {
            metrics.increment("retry.published", mapOf(
                "topic" to event.message.topic,
                "retry_count" to event.attemptCount.toString()
            ))
        }
        
        is RecovererEvent.ErrorMessagePublished -> {
            metrics.increment("dlt.published", mapOf(
                "topic" to event.message.topic
            ))
            
            // Alert on critical DLT messages
            if (event.message.topic.startsWith("critical")) {
                alertingService.sendAlert(
                    "Critical message sent to DLT",
                    "Topic: ${event.message.topic}, Key: ${event.message.key}"
                )
            }
        }
        
        is RecovererEvent.DelayMessagePublished -> {
            metrics.histogram("retry.delay", event.delay.inWholeSeconds.toDouble())
        }
    }
}
```

---

## Summary

Quafka's retry mechanisms provide:

✅ **Flexible Strategies**: Four built-in retry strategies for different use cases  
✅ **Policy-Based Configuration**: Fine-grained control per exception type  
✅ **In-Memory + Non-Blocking**: Best of both worlds for resilient processing  
✅ **Developer-Friendly DSL**: Intuitive configuration with Kotlin builders  
✅ **Observable**: Full visibility through events and headers  
✅ **Extensible**: Customize every aspect of retry behavior  

Choose the right combination of strategies and policies for your specific requirements, and leverage the extensibility points for advanced use cases.

For more examples, see:
- [Single Consumer Examples](examples/single-consumer-examples.md)
- [Batch Consumer Examples](examples/batch-consumer-examples.md)

