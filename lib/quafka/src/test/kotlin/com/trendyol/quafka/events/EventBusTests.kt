package com.trendyol.quafka.events

import io.kotest.assertions.nondeterministic.*
import io.kotest.core.spec.style.FunSpec
import io.kotest.matchers.shouldBe
import kotlinx.coroutines.*
import kotlin.time.Duration.Companion.milliseconds

class EventBusTests :
    FunSpec({

        data class EventProcessed(
            val offset: Int
        ) : QuafkaEvent

        data class EventReceived(
            val offset: Int
        ) : QuafkaEvent

        test("should publish multiple events and subscribe to events") {
            val eventBus = EventBus()
            val capturedEvents = mutableListOf<QuafkaEvent>()
            val subscriptionInitialized = CompletableDeferred<Unit>()

            // Subscribe
            val subscriptionJob = launch {
                subscriptionInitialized.complete(Unit) // Mark the subscription as initialized
                eventBus.subscribe {
                    capturedEvents.add(it)
                }
            }

            // Wait for the subscription to initialize
            subscriptionInitialized.await()

            // Publish events
            eventBus.publishSuspendable(EventReceived(1))
            eventBus.publishSuspendable(EventProcessed(1))

            // Wait until all events are captured
            until(100.milliseconds) {
                capturedEvents.size == 2
            }

            // Assert
            capturedEvents.first() shouldBe EventReceived(1)
            capturedEvents.last() shouldBe EventProcessed(1)

            // Cleanup
            eventBus.close()
            subscriptionJob.cancel()
        }

        test("should publish event and subscribe multiple times") {
            val eventBus = EventBus()
            val capturedEvents = mutableListOf<QuafkaEvent>()
            val subscription1Initialized = CompletableDeferred<Unit>()
            val subscription2Initialized = CompletableDeferred<Unit>()

            // Subscribe
            val subscriptionJob = launch {
                subscription1Initialized.complete(Unit) // Mark the subscription as initialized
                eventBus.subscribe {
                    capturedEvents.add(it)
                }
            }

            val subscriptionJob2 = launch {
                subscription2Initialized.complete(Unit)
                eventBus.subscribe {
                    capturedEvents.add(it)
                }
            }

            // Wait for the subscription to initialize
            subscription1Initialized.await()
            subscription2Initialized.await()

            // Publish events
            eventBus.publishSuspendable(EventReceived(1))
            eventBus.publishSuspendable(EventProcessed(1))

            // Wait until all events are captured
            until(500.milliseconds) {
                capturedEvents.size == 4
            }

            // Assert
            capturedEvents.size shouldBe 4

            // Cleanup
            eventBus.close()
            subscriptionJob.cancel()
            subscriptionJob2.cancel()
        }

        test("should publish multiple events but subscribe to a specific type of event") {
            val eventBus = EventBus()
            val capturedEvents = mutableListOf<QuafkaEvent>()
            val subscriptionInitialized = CompletableDeferred<Unit>()

            // Subscribe
            val subscriptionJob = launch {
                subscriptionInitialized.complete(Unit) // Mark the subscription as initialized
                eventBus.subscribe<EventProcessed> {
                    capturedEvents.add(it)
                }
            }

            // Wait for the subscription to initialize
            subscriptionInitialized.await()

            // Publish events
            eventBus.publishSuspendable(EventReceived(1))
            eventBus.publishSuspendable(EventReceived(2))
            eventBus.publishSuspendable(EventReceived(3))
            eventBus.publishSuspendable(EventProcessed(3))
            eventBus.publishSuspendable(EventProcessed(1))

            // Wait until the specific events are captured
            until(200.milliseconds) {
                capturedEvents.size == 2
            }

            // Assert
            capturedEvents.first() shouldBe EventProcessed(3)
            capturedEvents.last() shouldBe EventProcessed(1)

            // Cleanup
            eventBus.close()
            subscriptionJob.cancel()
        }
    })
