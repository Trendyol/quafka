package com.trendyol.quafka.consumer.internals

import com.trendyol.quafka.*
import com.trendyol.quafka.consumer.*
import com.trendyol.quafka.consumer.configuration.*
import com.trendyol.quafka.consumer.messageHandlers.*
import io.kotest.assertions.nondeterministic.eventually
import io.kotest.core.spec.style.FunSpec
import io.kotest.matchers.shouldBe
import io.kotest.matchers.shouldNotBe
import io.mockk.*
import kotlinx.coroutines.*
import org.apache.kafka.common.TopicPartition
import kotlin.time.Duration.Companion.ZERO
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds

class TopicPartitionWorkerTests :
    FunSpec({
        val topicPartition = TopicPartition("topic", 1)
        val groupId = "groupId"
        val clientId = "clientId"
        val instantProvider = FakeTimeProvider.default()
        val subscriptionOptions = TopicSubscriptionOptions<String, String>(
            topicPartition.topic(),
            false,
            1,
            ZERO,
            mockk(relaxed = true)
        )

        val quafkaConsumerOptions: QuafkaConsumerOptions<String, String> = mockk(relaxed = true) {
            val opt = this
            every { opt.getGroupId() } returns groupId
            every { opt.getClientId() } returns clientId
            every { opt.timeProvider } returns instantProvider
        }

        fun createTopicPartitionWorker(subscriptionOptions: TopicSubscriptionOptions<String, String>, job: Job = Job()): TopicPartitionWorker<String, String> = TopicPartitionWorker(
            topicPartition = topicPartition,
            quafkaConsumerOptions = quafkaConsumerOptions,
            scope = CoroutineScope(Dispatchers.IO + job),
            subscriptionOptions = subscriptionOptions
        )

        test("should return false when worker is stopped cancelled") {
            // arrange
            val job = Job()
            val sut = createTopicPartitionWorker(subscriptionOptions.copy(backpressureBufferSize = 1), job)
            job.cancel()
            val topicPartitionBasedMessageBuilder = TopicPartitionBasedMessageBuilder<String, String>(
                topicPartition = topicPartition
            )
            val message1 = topicPartitionBasedMessageBuilder.new("key1", "value1").build()

            // act
            val result = sut.enqueue(
                message1
            )

            // assert
            result shouldBe false
        }

        test("should return false when stop") {
            // arrange
            val sut = createTopicPartitionWorker(subscriptionOptions = subscriptionOptions.copy(backpressureBufferSize = 1))
            sut.stop()
            val topicPartitionBasedMessageBuilder = TopicPartitionBasedMessageBuilder<String, String>(
                topicPartition = topicPartition
            )
            val message1 = topicPartitionBasedMessageBuilder.new("key1", "value1").build()

            // act
            val result = sut.enqueue(
                message1
            )

            // assert
            result shouldBe false
        }

        test("should return when message send successfully") {
            // arrange
            val sut = createTopicPartitionWorker(subscriptionOptions = subscriptionOptions.copy(backpressureBufferSize = 1))
            val topicPartitionBasedMessageBuilder = TopicPartitionBasedMessageBuilder<String, String>(
                topicPartition = topicPartition
            )
            val message1 = topicPartitionBasedMessageBuilder.new("key1", "value1").build()

            // act
            val result = sut.enqueue(
                message1
            )

            // assert
            result shouldBe true
        }

        test("should handle cancellation when worker cancelled") {
            var exception: Throwable? = null
            val job = Job()
            // arrange
            val sut = createTopicPartitionWorker(
                subscriptionOptions.copy(
                    backpressureBufferSize = 2,
                    messageHandlingStrategy = SingleMessageHandlingStrategy<String, String>(
                        { incomingMessage, consumerContext ->
                            try {
                                delay(2.seconds)
                            } catch (ex: CancellationException) {
                                exception = ex
                                throw ex
                            }
                        },
                        true
                    )
                ),
                job
            )

            // act
            val topicPartitionBasedMessageBuilder = TopicPartitionBasedMessageBuilder<String, String>(
                topicPartition = topicPartition
            )
            val message1 = topicPartitionBasedMessageBuilder.new("key1", "value1").build()
            sut.enqueue(
                message1
            )
            delay(100.milliseconds)
            job.cancel()

            // assert
            eventually(5.seconds) {
                exception as? CancellationException shouldNotBe null
            }
        }

        test("should process message") {
            val job = Job()
            val waitGroup = WaitGroup(1)
            var processedMessage: IncomingMessage<String, String>? = null
            var consumerContext: ConsumerContext? = null
            // arrange
            val sut = createTopicPartitionWorker(
                subscriptionOptions.copy(
                    backpressureBufferSize = 2,
                    messageHandlingStrategy = SingleMessageHandlingStrategy<String, String>(
                        { im, cx ->
                            processedMessage = im
                            consumerContext = cx
                            waitGroup.done()
                        },
                        true
                    )
                ),
                job
            )

            // act
            val topicPartitionBasedMessageBuilder = TopicPartitionBasedMessageBuilder<String, String>(
                topicPartition = topicPartition
            )
            val message1 = topicPartitionBasedMessageBuilder.new("key1", "value1").build()
            sut.enqueue(
                message1
            )

            // assert
            waitGroup.wait()
            processedMessage shouldBe message1
            consumerContext!!.topicPartition shouldBe topicPartition
            consumerContext.consumerOptions.getGroupId() shouldBe groupId
            consumerContext.consumerOptions.getClientId() shouldBe clientId
            consumerContext.consumerOptions.timeProvider shouldBe instantProvider
            consumerContext.isActive shouldBe true
        }

        test("should process and sends ack to processed message") {
            // arrange
            val waitGroup = WaitGroup(1)
            var ackedMessage: IncomingMessage<*, *>? = null
            val sut = createTopicPartitionWorker(
                subscriptionOptions.copy(
                    backpressureBufferSize = 2,
                    messageHandlingStrategy = SingleMessageHandlingStrategy<String, String>(
                        { im, cx ->
                        },
                        true
                    )
                )
            )
            val topicPartitionBasedMessageBuilder = TopicPartitionBasedMessageBuilder<String, String>(
                topicPartition = topicPartition
            )
            val message1 = topicPartitionBasedMessageBuilder
                .new("key1", "value1")
                .copy(ackCallback = {
                    ackedMessage = it
                    launch { waitGroup.done() }
                })
                .build()

            // act
            sut.enqueue(
                message1
            )

            // assert
            waitGroup.wait()
            ackedMessage shouldBe message1
        }

        test("should process batch messages when batch size is ready") {
            val waitGroup = WaitGroup(1)
            val processedMessage = mutableListOf<IncomingMessage<String, String>>()

            // arrange
            val sut = createTopicPartitionWorker(
                subscriptionOptions.copy(
                    backpressureBufferSize = 5,
                    messageHandlingStrategy = BatchMessageHandlingStrategy<String, String>(
                        { incomingMessages, consumerContext: ConsumerContext ->
                            processedMessage += incomingMessages
                            waitGroup.done()
                        },
                        3,
                        ZERO,
                        true
                    )
                )
            )

            // act
            val topicPartitionBasedMessageBuilder = TopicPartitionBasedMessageBuilder<String, String>(
                topicPartition = topicPartition
            )
            val message1 = topicPartitionBasedMessageBuilder.new("key1", "value1").build()
            val message2 = topicPartitionBasedMessageBuilder.new("key2", "value2").build()
            val message3 = topicPartitionBasedMessageBuilder.new("key3", "value3").build()
            sut.enqueue(
                message1
            )
            sut.enqueue(
                message2
            )
            sut.enqueue(
                message3
            )

            // assert
            waitGroup.wait()
            processedMessage.size shouldBe 3
        }

        test("should process batch messages when batch timeout is ready") {
            val waitGroup = WaitGroup(1)
            val processedMessage = mutableListOf<IncomingMessage<String, String>>()

            // arrange
            val sut = createTopicPartitionWorker(
                subscriptionOptions.copy(
                    backpressureBufferSize = 5,
                    messageHandlingStrategy = BatchMessageHandlingStrategy<String, String>(
                        { incomingMessages, consumerContext: ConsumerContext ->
                            processedMessage += incomingMessages
                            waitGroup.done()
                        },
                        3,
                        200.milliseconds,
                        true
                    )
                )
            )

            // act
            val topicPartitionBasedMessageBuilder = TopicPartitionBasedMessageBuilder<String, String>(
                topicPartition = topicPartition
            )
            val message1 = topicPartitionBasedMessageBuilder.new("key1", "value1").build()
            val message2 = topicPartitionBasedMessageBuilder.new("key2", "value2").build()
            sut.enqueue(
                message1
            )
            sut.enqueue(
                message2
            )

            // assert
            waitGroup.wait()
            processedMessage.size shouldBe 2
        }
    })
