package com.trendyol.quafka.extensions.errorHandling.recoverer

import com.trendyol.quafka.*
import com.trendyol.quafka.consumer.IncomingMessage
import com.trendyol.quafka.extensions.delaying.DelayHeaders.hasRetryHeaders
import com.trendyol.quafka.producer.*
import io.kotest.assertions.throwables.shouldThrow
import io.kotest.core.spec.style.FunSpec
import io.kotest.matchers.shouldBe
import io.mockk.*
import org.apache.kafka.common.header.Header
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.minutes
import kotlin.time.Duration.Companion.seconds

class AdvancedErrorHandlerTests :
    FunSpec({

        lateinit var producer: QuafkaProducer<String, String>
        lateinit var sut: FailedMessageRouter<String, String>
        lateinit var retryPolicyProvider: RetryPolicyProvider
        val outgoingMessageSlot = slot<OutgoingMessage<String, String>>()

        beforeEach {
            producer = mockk(relaxed = true)
            retryPolicyProvider = mockk(relaxed = true) {
                coEvery { this@mockk(any(), any(), any(), any()) } returns RetryPolicy.NonBlocking(
                    identifier = PolicyIdentifier("test"),
                    config = NonBlockingRetryConfig.exponentialRandomBackoff(3, 300.milliseconds, 1.minutes)
                )
            }
            coEvery { producer.send(capture(outgoingMessageSlot)) } returns mockk(relaxed = true)
        }

        fun createSut(topicConfigurations: List<TopicConfiguration<*>>): FailedMessageRouter<String, String> =
            FailedMessageRouter(
                exceptionDetailsProvider = { throwable -> ExceptionDetails(throwable, "") },
                topicResolver = TopicResolver(topicConfigurations),
                danglingTopic = "dangling-topic",
                quafkaProducer = producer,
                policyProvider = retryPolicyProvider,
                outgoingMessageModifier = { _, _, _ -> this }
            )

        fun createMessage(topic: String = "product.create", attempt: Int = 0, overallAttempt: Int = 0, policyIdentifier: PolicyIdentifier = PolicyIdentifier.none()): IncomingMessage<String, String> {
            val headers = if (attempt > 0 || overallAttempt > 0) {
                mutableListOf<Header>().addRetryAttempt(attempt, overallAttempt, policyIdentifier)
            } else {
                mutableListOf()
            }

            return TopicPartitionBasedMessageBuilder<String, String>(topic, 0)
                .new("key", "value")
                .copy(headers = headers)
                .build()
        }

        suspend fun recover(message: IncomingMessage<String, String>, exception: Throwable = Exception("test")) {
            sut.handle(
                incomingMessage = message,
                consumerContext = message.buildConsumerContext(FakeTimeProvider.default()),
                exception = exception
            )
        }

        test("when a fatal error occurs, then it should be re-thrown") {
            // Arrange
            sut = createSut(listOf()) // Config doesn't matter here
            val fatalError = OutOfMemoryError("Fatal Error")

            // Act & Assert
            val exception = shouldThrow<OutOfMemoryError> {
                recover(createMessage(), exception = fatalError)
            }
            exception.message shouldBe "Fatal Error"
            coVerify(exactly = 0) { producer.send(any()) }
        }

        test("when the retry identifier changes, then the attempt count should reset") {
            // Arrange
            sut = createSut(
                listOf(
                    TopicConfiguration(
                        topic = "product.create",
                        retry = TopicRetryStrategy.SingleTopicRetry(
                            retryTopic = "product.create.retry",
                            maxOverallAttempts = 5
                        ),
                        deadLetterTopic = "product.create.error"
                    )
                )
            )
            // Message was last retried for a different reason ("old-identifier")
            val message = createMessage(attempt = 2, policyIdentifier = PolicyIdentifier("old-identifier"))

            coEvery { retryPolicyProvider(any(), any(), any(), any()) } returns RetryPolicy.NonBlocking(
                identifier = PolicyIdentifier("new-identifier"),
                config = NonBlockingRetryConfig.exponentialRandomBackoff(3, 1.seconds, 1.seconds)
            )

            // Act: Recover for a new reason ("new-identifier")
            recover(message)

            // Assert
            outgoingMessageSlot.captured.topic shouldBe "product.create.retry"
            // The per-retry attempt for the *new* identifier should be 1
            outgoingMessageSlot.captured.headers.getRetryAttemptOrDefault() shouldBe 1
        }

        context("given an exponential backoff multi-topic retry strategy") {
            beforeEach {
                sut = createSut(
                    listOf(
                        TopicConfiguration(
                            topic = "product.create",
                            retry = TopicRetryStrategy.ExponentialBackoffMultiTopicRetry(
                                maxOverallAttempts = 5,
                                retryTopics = listOf(
                                    TopicRetryStrategy.DelayTopicConfiguration(
                                        "product.create.retry.100ms",
                                        500.milliseconds
                                    ),
                                    TopicRetryStrategy.DelayTopicConfiguration(
                                        "product.create.retry.1s",
                                        1.seconds
                                    )
                                )
                            ),
                            deadLetterTopic = "product.create.error"
                        )
                    )
                )
            }

            test("when a message fails, then it should be sent to the correct retry bucket topic") {
                // Arrange
                val message = createMessage() // First failure (overall attempt will be 1)

                // Act
                recover(message)

                // Assert
                val capturedMessage = outgoingMessageSlot.captured
                // The first calculated delay is 50ms, which falls into the 100ms bucket
                capturedMessage.topic shouldBe "product.create.retry.100ms"
                capturedMessage.headers.getOverallRetryAttemptOrDefault() shouldBe 1
                // Check that delay headers are added
                capturedMessage.headers.hasRetryHeaders() shouldBe true
            }
        }

        context("given an exponential backoff to single-topic retry strategy") {
            beforeEach {
                sut = createSut(
                    listOf(
                        TopicConfiguration(
                            topic = "product.create",
                            retry = TopicRetryStrategy.ExponentialBackoffToSingleTopicRetry(
                                delayTopics = listOf(
                                    TopicRetryStrategy.DelayTopicConfiguration(
                                        "product.create.delay.100ms",
                                        100.milliseconds
                                    ),
                                    TopicRetryStrategy.DelayTopicConfiguration(
                                        "product.create.delay.200ms",
                                        250.milliseconds
                                    ),
                                    TopicRetryStrategy.DelayTopicConfiguration(
                                        "product.create.delay.1s",
                                        1.seconds
                                    )
                                ),
                                retryTopic = "product.create.retry", // All messages eventually go here
                                maxOverallAttempts = 5
                            ),
                            deadLetterTopic = "product.create.error"
                        )
                    )
                )
            }

            test("when a message fails, then it should be forwarded to the correct delay bucket topic") {
                // Arrange
                coEvery { retryPolicyProvider(any(), any(), any(), any()) } returns RetryPolicy.NonBlocking(
                    identifier = PolicyIdentifier("test"),
                    config = NonBlockingRetryConfig.exponentialRandomBackoff(3, 50.milliseconds, 1.minutes)
                )
                val message = createMessage() // First failure

                // Act
                recover(message)

                // Assert
                val capturedMessage = outgoingMessageSlot.captured
                // 1. It is sent to the DELAY topic first
                capturedMessage.topic shouldBe "product.create.delay.100ms"
                // 2. The header specifies the final destination RETRY topic
                capturedMessage.headers.getForwardingTopic() shouldBe "product.create.retry"
                capturedMessage.headers.getOverallRetryAttemptOrDefault() shouldBe 1
            }
        }

        context("given a single topic retry strategy") {
            beforeEach {
                sut = createSut(
                    listOf(
                        TopicConfiguration(
                            topic = "product.create",
                            retry = TopicRetryStrategy.SingleTopicRetry(
                                retryTopic = "product.create.retry",
                                maxOverallAttempts = 3
                            ),
                            deadLetterTopic = "product.create.error"
                        )
                    )
                )
            }

            test("when a message fails and can be retried, then it should be sent to the retry topic") {
                // Arrange
                val message = createMessage()

                // Act
                recover(message)

                // Assert
                outgoingMessageSlot.captured.topic shouldBe "product.create.retry"
            }

            test("when retry attempts are maxed out, then it should be sent to the dead-letter topic") {
                // Arrange
                val message = createMessage(attempt = 3, policyIdentifier = PolicyIdentifier("test"))

                // Act
                recover(message)

                // Assert
                outgoingMessageSlot.captured.topic shouldBe "product.create.error"
            }

            test("when overall attempts are maxed out, then it should be sent to the dead-letter topic") {
                // Arrange
                val message = createMessage(overallAttempt = 3)

                // Act
                recover(message)

                // Assert
                outgoingMessageSlot.captured.topic shouldBe "product.create.error"
            }

            test("when the message is not retryable, then it should be sent to the dead-letter topic") {
                // Arrange
                coEvery { retryPolicyProvider(any(), any(), any(), any()) } returns RetryPolicy.NoRetry
                val message = createMessage(attempt = 1)

                // Act
                recover(message)

                // Assert
                outgoingMessageSlot.captured.topic shouldBe "product.create.error"
            }
        }

        context("given a non-existent topic configuration") {
            beforeEach {
                sut = createSut(listOf())
            }

            test("when a message fails, then it should be sent to the dangling topic") {
                // Arrange
                val message = createMessage(topic = "unconfigured.topic")

                // Act
                recover(message)

                // Assert
                outgoingMessageSlot.captured.topic shouldBe "dangling-topic"
            }
        }

        context("given no retry strategy") {
            beforeEach {
                sut = createSut(
                    listOf(
                        TopicConfiguration(
                            topic = "product.create",
                            retry = TopicRetryStrategy.NoneStrategy,
                            deadLetterTopic = "product.create.error"
                        )
                    )
                )
            }

            test("when a message fails, then it should be sent to the dead-letter topic") {
                // Arrange
                val message = createMessage()

                // Act
                recover(message)

                // Assert
                outgoingMessageSlot.captured.topic shouldBe "product.create.error"
            }
        }
    })
