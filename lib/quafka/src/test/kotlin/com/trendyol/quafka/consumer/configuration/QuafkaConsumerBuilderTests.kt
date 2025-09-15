package com.trendyol.quafka.consumer.configuration

import com.trendyol.quafka.FakeTimeProvider
import com.trendyol.quafka.consumer.*
import com.trendyol.quafka.consumer.messageHandlers.*
import io.github.resilience4j.retry.*
import io.kotest.core.spec.style.FunSpec
import io.kotest.matchers.*
import kotlinx.coroutines.*
import org.apache.kafka.common.serialization.StringDeserializer
import kotlin.time.Duration.Companion.seconds

class QuafkaConsumerBuilderTests :
    FunSpec({
        test("should create") {
            val singleMessageHandler = object : SingleMessageHandler<String, String> {
                override suspend fun invoke(
                    incomingMessage: IncomingMessage<String, String>,
                    consumerContext: ConsumerContext
                ) = Unit
            }

            val batchMessageHandler = object : BatchMessageHandler<String, String> {
                override suspend fun invoke(
                    incomingMessages: Collection<IncomingMessage<String, String>>,
                    consumerContext: ConsumerContext
                ) = Unit
            }

            val deserializer = StringDeserializer()
            val exceptionHandler = CoroutineExceptionHandler { coroutineContext, throwable -> }
            val properties = mapOf<String, Any>()
            val builder: QuafkaConsumer<String, String> = QuafkaConsumerBuilder<String, String>(properties)
                .withClientId("client_id")
                .withDeserializer(deserializer, deserializer)
                .withGroupId("group_id")
                .withDispatcher(Dispatchers.IO)
                .withCoroutineExceptionHandler(exceptionHandler)
                .withConnectionRetryPolicy(Retry.of("retry-config", RetryConfig.ofDefaults()))
                .withCommitOptions(CommitOptions(10.seconds))
                .withMessageFormatter(DummyMessageLogFormatter())
                .withPollDuration(1.seconds)
                .withTimeProvider(FakeTimeProvider.default())
                .withStartupOptions(blockOnStart = true)
                .subscribe("topic1", "topic2") {
                    withSingleMessageHandler(handler = singleMessageHandler)
                        .withBackpressure(10, 5.seconds)
                        .autoAckAfterProcess(true)
                }.subscribe("topic3") {
                    withBatchMessageHandler(handler = batchMessageHandler, batchSize = 8, batchTimeout = 7.seconds)
                        .autoAckAfterProcess(false)
                }.build()

            val options = builder.quafkaConsumerOptions

            options.keyDeserializer shouldBe deserializer
            options.valueDeserializer shouldBe deserializer
            options.timeProvider.javaClass shouldBe FakeTimeProvider::class.java
            options.coroutineExceptionHandler shouldBe exceptionHandler
            options.connectionRetryPolicy.name shouldBe "retry-config"
            options.incomingMessageStringFormatter.javaClass shouldBe DummyMessageLogFormatter::class.java
            options.blockOnStart shouldBe true
            options.pollDuration shouldBe 1.seconds
            options.commitOptions.duration shouldBe 10.seconds
            options.dispatcher shouldBe Dispatchers.IO
            options.subscribedTopics() shouldBe setOf("topic1", "topic2", "topic3")
            options.getGroupId() shouldBe "group_id"
            options.getClientId() shouldBe "client_id"

            val topic1Subscription = options.getSubscriptionOptionsByTopicName("topic1")!!
            topic1Subscription.backpressureBufferSize shouldBe 10
            topic1Subscription.backpressureReleaseTimeout shouldBe 5.seconds
            topic1Subscription.autoAck shouldBe true
            val topic1SubscriptionHandler = topic1Subscription.messageHandlingStrategy as? SingleMessageHandlingStrategy<String, String>
            topic1SubscriptionHandler shouldNotBe null

            val topic2Subscription = options.getSubscriptionOptionsByTopicName("topic2")!!
            topic2Subscription.backpressureBufferSize shouldBe 10
            topic2Subscription.backpressureReleaseTimeout shouldBe 5.seconds
            topic2Subscription.autoAck shouldBe true
            val topic2SubscriptionHandler = topic2Subscription.messageHandlingStrategy as? SingleMessageHandlingStrategy<String, String>
            topic2SubscriptionHandler shouldNotBe null

            val topic3Subscription = options.getSubscriptionOptionsByTopicName("topic3")!!
            topic3Subscription.backpressureBufferSize shouldBe DEFAULT_BACKPRESSURE_BUFFER_SIZE
            topic3Subscription.backpressureReleaseTimeout shouldBe DEFAULT_BACKPRESSURE_RELEASE_TIMEOUT
            topic3Subscription.autoAck shouldBe false
            val topic3SubscriptionHandler = topic3Subscription.messageHandlingStrategy as BatchMessageHandlingStrategy<String, String>
            topic3SubscriptionHandler.batchSize shouldBe 8
            topic3SubscriptionHandler.batchTimeout shouldBe 7.seconds
        }
    })

private class DummyMessageLogFormatter : IncomingMessageStringFormatter()
