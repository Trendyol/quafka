package com.trendyol.quafka.consumer

import com.trendyol.quafka.IncomingMessageBuilder
import io.kotest.core.spec.style.FunSpec
import io.kotest.matchers.shouldBe

class AcknowledgmentTests :
    FunSpec({
        test("should ack all") {

            val ackedOffsets = mutableListOf<Long>()

            val topic1Message1 = IncomingMessageBuilder(
                topic = "topic1",
                partition = 1,
                offset = 7,
                key = "key",
                value = "value",
                ackCallback = {
                    ackedOffsets.add(it.offset)
                }
            ).build()
            val topic1Message2 = IncomingMessageBuilder(
                topic = "topic1",
                partition = 1,
                offset = 8,
                key = "key",
                value = "value",
                ackCallback = {
                    ackedOffsets.add(it.offset)
                }
            ).build()

            val messages = listOf(topic1Message1, topic1Message2)
            messages.ackAll()

            ackedOffsets.size shouldBe 2
            ackedOffsets[0] shouldBe 7L
            ackedOffsets[1] shouldBe 8L
        }

        test("should commit all") {

            val committedOffsets = mutableListOf<Long>()

            val topic1Message1 = IncomingMessageBuilder(
                topic = "topic1",
                partition = 1,
                offset = 7,
                key = "key",
                value = "value",
                commitCallback = {
                    committedOffsets.add(it.offset)
                }
            ).build()
            val topic1Message2 = IncomingMessageBuilder(
                topic = "topic1",
                partition = 1,
                offset = 8,
                key = "key",
                value = "value",
                commitCallback = {
                    committedOffsets.add(it.offset)
                }
            ).build()

            val messages = listOf(topic1Message1, topic1Message2)
            messages.commitAll()

            committedOffsets.size shouldBe 2
            committedOffsets[0] shouldBe 7L
            committedOffsets[1] shouldBe 8L
        }
    })
