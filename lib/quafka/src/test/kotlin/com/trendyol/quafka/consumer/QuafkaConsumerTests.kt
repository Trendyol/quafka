package com.trendyol.quafka.consumer

import com.trendyol.quafka.consumer.configuration.QuafkaConsumerOptions
import com.trendyol.quafka.consumer.internals.*
import io.github.resilience4j.retry.*
import io.kotest.core.spec.style.FunSpec
import io.kotest.matchers.shouldBe
import io.mockk.*
import kotlinx.coroutines.*

class QuafkaConsumerTests :
    FunSpec({

        lateinit var quafkaConsumerOptions: QuafkaConsumerOptions<String, String>
        lateinit var pollingConsumerFactory: PollingConsumerFactory<String, String>
        lateinit var sut: QuafkaConsumer<String, String>
        lateinit var mockConsumer: PollingConsumer<String, String>
        val scope = CoroutineScope(Dispatchers.Default)
        val retryConfig = RetryConfig.ofDefaults()

        beforeEach {
            quafkaConsumerOptions = mockk(relaxed = true)
            pollingConsumerFactory = mockk(relaxed = true)
            mockConsumer = mockk(relaxed = true)

            every { quafkaConsumerOptions.dispatcher } returns Dispatchers.Unconfined
            every { quafkaConsumerOptions.coroutineExceptionHandler } returns CoroutineExceptionHandler { _, _ -> }
            every { quafkaConsumerOptions.connectionRetryPolicy } returns Retry.of("basic", retryConfig)
            every { quafkaConsumerOptions.subscriptions } returns listOf(
                mockk(relaxed = true) {}
            )

            sut = QuafkaConsumer(
                quafkaConsumerOptions = quafkaConsumerOptions,
                pollingConsumerFactory = pollingConsumerFactory
            )
        }

        afterEach {
            scope.coroutineContext.cancelChildren()
        }

        test("start transitions from Ready to Running and creates consumer") {
            // arrange
            every { quafkaConsumerOptions.blockOnStart } returns false
            every { pollingConsumerFactory.create(quafkaConsumerOptions, any(), null, any()) } returns mockConsumer
            every { mockConsumer.start() } just runs
            every { quafkaConsumerOptions.hasSubscription() } returns true

            // act
            sut.start()

            // assert
            sut.status shouldBe QuafkaConsumer.ConsumerStatus.Running
            verify(exactly = 1) { pollingConsumerFactory.create(quafkaConsumerOptions, any(), null, any()) }
            verify(exactly = 1) { mockConsumer.start() }
        }

        test("start does nothing if status is already Running") {
            // arrange
            // consumer is already started
            every { quafkaConsumerOptions.blockOnStart } returns false
            every { pollingConsumerFactory.create(quafkaConsumerOptions, any(), null, any()) } returns mockConsumer
            every { mockConsumer.start() } just runs
            every { quafkaConsumerOptions.hasSubscription() } returns true

            sut.start()

            // act
            sut.start() // second time

            // assert
            verify(exactly = 1) { pollingConsumerFactory.create(quafkaConsumerOptions, any(), null, any()) }
            verify(exactly = 1) { mockConsumer.start() }
            sut.status shouldBe QuafkaConsumer.ConsumerStatus.Running
        }

        test("should wait after started the consumer if blockOnStart is true") {
            // arrange
            every { quafkaConsumerOptions.blockOnStart } returns true
            coEvery { pollingConsumerFactory.create(quafkaConsumerOptions, any(), any(), any()) } returns mockConsumer
            every { quafkaConsumerOptions.hasSubscription() } returns true

            // act
            val job = scope.launch {
                sut.start()
            }
            delay(100)

            // assert
            job.isActive shouldBe true
            sut.status shouldBe QuafkaConsumer.ConsumerStatus.Running
            verify { mockConsumer.start() }

            // clear
            job.cancel()
        }

        test("stop transitions from Running to Ready") {
            // arrange
            every { quafkaConsumerOptions.blockOnStart } returns false
            every { pollingConsumerFactory.create(quafkaConsumerOptions, any(), null, any()) } returns mockConsumer
            every { mockConsumer.start() } just runs
            sut.start()

            // act
            sut.stop()

            // assert
            sut.status shouldBe QuafkaConsumer.ConsumerStatus.Ready
        }

        test("stop does nothing if status is not Running") {
            // act
            sut.status shouldBe QuafkaConsumer.ConsumerStatus.Ready

            // arrange
            sut.stop()

            // assert
            sut.status shouldBe QuafkaConsumer.ConsumerStatus.Ready
        }
    })
