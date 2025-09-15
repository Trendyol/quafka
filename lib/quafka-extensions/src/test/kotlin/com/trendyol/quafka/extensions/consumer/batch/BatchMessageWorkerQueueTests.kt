package com.trendyol.quafka.extensions.consumer.batch

import com.trendyol.quafka.*
import com.trendyol.quafka.common.*
import com.trendyol.quafka.common.HeaderParsers.asString
import com.trendyol.quafka.consumer.configuration.*
import io.kotest.core.spec.style.FunSpec
import io.kotest.matchers.shouldBe
import io.mockk.mockk
import org.apache.kafka.common.TopicPartition
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger

class BatchMessageWorkerQueueTests :
    FunSpec({

        test("should group and process groups concurrently") {
            // arrange
            val processedMessages = ConcurrentHashMap<String, AtomicInteger>()
            val handler = GroupedBatchedMessageHandler<String, String>(
                GroupedBatchedMessageHandler.GroupedBatchedMessageHandlerOptions(
                    singleMessageHandler = { incomingMessage, consumerContext ->
                        val atomicCounter = processedMessages.computeIfAbsent(
                            incomingMessage.headers.get("X-Group")?.asString() ?: ""
                        ) { AtomicInteger(0) }
                        atomicCounter.incrementAndGet()
                    },
                    concurrency = 2,
                    groupingFn = { incomingMessage ->
                        incomingMessage.headers.get("X-Group")?.asString() ?: ""
                    }
                )
            )

            val topicPartitionBasedMessageBuilder = TopicPartitionBasedMessageBuilder<String, String>(
                topicPartition = TopicPartition("topic1", 1)
            )

            val messages = listOf("group1", "group2")
                .map { group ->
                    (1..20).map { index ->
                        val header = header("X-Group", group)
                        topicPartitionBasedMessageBuilder
                            .new("key$index", "value1$index")
                            .copy(headers = mutableListOf(header))
                            .build()
                    }
                }.flatten()

            // act
            handler.invoke(
                messages,
                mockk()
            )

            // asset
            processedMessages["group1"]?.get() shouldBe 20
            processedMessages["group2"]?.get() shouldBe 20
        }
    })
