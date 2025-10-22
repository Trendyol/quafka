package com.trendyol.quafka.consumer

import com.trendyol.quafka.common.header
import io.kotest.core.spec.style.FunSpec
import io.kotest.matchers.shouldBe
import kotlinx.coroutines.*
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.header.internals.*
import org.apache.kafka.common.record.TimestampType
import java.time.Instant
import java.util.Optional

class IncomingMessageTests :
    FunSpec({

        test("should create with acknowledgement") {
            // arrange
            val now = Instant.now().toEpochMilli()
            val consumerRecord = ConsumerRecord(
                "topic",
                8,
                3,
                now,
                TimestampType.NO_TIMESTAMP_TYPE,
                2,
                3,
                "key",
                "value",
                RecordHeaders(
                    listOf(
                        RecordHeader("key", "value".toByteArray(Charsets.UTF_8))
                    )
                ),
                Optional.of(2)
            )

            // act
            var acked = false
            var committed = false
            val incomingMessage = IncomingMessage.create(
                consumerRecord,
                IncomingMessageStringFormatter(),
                "groupId",
                "clientId",
                object : Acknowledgment {
                    override fun acknowledge(incomingMessage: IncomingMessage<*, *>) {
                        acked = true
                    }

                    override fun commit(incomingMessage: IncomingMessage<*, *>): Deferred<Unit> {
                        committed = true
                        return CompletableDeferred(Unit)
                    }
                }
            )

            incomingMessage.ack()
            incomingMessage.commit()

            // assert
            acked shouldBe true
            committed shouldBe true
        }

        test("should create with empty key") {
            // arrange
            val now = Instant.now().toEpochMilli()
            val consumerRecord = ConsumerRecord(
                "topic",
                8,
                3,
                now,
                TimestampType.NO_TIMESTAMP_TYPE,
                2,
                3,
                null,
                "value",
                RecordHeaders(
                    listOf(
                        RecordHeader("key", "value".toByteArray(Charsets.UTF_8))
                    )
                ),
                Optional.of(2)
            )

            // act

            val incomingMessage = IncomingMessage.create(
                consumerRecord,
                IncomingMessageStringFormatter(),
                "groupId",
                "clientId"
            )

            // assert
            incomingMessage.topic shouldBe consumerRecord.topic()
            incomingMessage.partition shouldBe consumerRecord.partition()
            incomingMessage.topicPartition shouldBe TopicPartition(consumerRecord.topic(), consumerRecord.partition())
            incomingMessage.offset shouldBe consumerRecord.offset()
            incomingMessage.timestamp shouldBe consumerRecord.timestamp()
            incomingMessage.timestampType shouldBe consumerRecord.timestampType()
            incomingMessage.serializedKeySize shouldBe consumerRecord.serializedKeySize()
            incomingMessage.serializedValueSize shouldBe consumerRecord.serializedValueSize()
            incomingMessage.key shouldBe null
            incomingMessage.value shouldBe "value"
            incomingMessage.headers shouldBe listOf(header("key", "value"))
            incomingMessage.leaderEpoch shouldBe consumerRecord.leaderEpoch().get()
            incomingMessage.getTimestampAsInstant() shouldBe Instant.ofEpochMilli(now)
            incomingMessage.isTombStonedMessage() shouldBe false
            incomingMessage.isKeyNull() shouldBe true
            incomingMessage.groupId shouldBe "groupId"
            incomingMessage.clientId shouldBe "clientId"
        }

        test("should create with empty value") {
            // arrange
            val now = Instant.now().toEpochMilli()
            val consumerRecord = ConsumerRecord(
                "topic",
                8,
                3,
                now,
                TimestampType.NO_TIMESTAMP_TYPE,
                2,
                3,
                "key",
                null,
                RecordHeaders(
                    listOf(
                        RecordHeader("key", "value".toByteArray(Charsets.UTF_8))
                    )
                ),
                Optional.of(2)
            )

            // act

            val incomingMessage = IncomingMessage.create(
                consumerRecord,
                IncomingMessageStringFormatter(),
                "groupId",
                "clientId"
            )

            // assert
            incomingMessage.topic shouldBe consumerRecord.topic()
            incomingMessage.partition shouldBe consumerRecord.partition()
            incomingMessage.topicPartition shouldBe TopicPartition(consumerRecord.topic(), consumerRecord.partition())
            incomingMessage.offset shouldBe consumerRecord.offset()
            incomingMessage.timestamp shouldBe consumerRecord.timestamp()
            incomingMessage.timestampType shouldBe consumerRecord.timestampType()
            incomingMessage.serializedKeySize shouldBe consumerRecord.serializedKeySize()
            incomingMessage.serializedValueSize shouldBe consumerRecord.serializedValueSize()
            incomingMessage.key shouldBe consumerRecord.key()
            incomingMessage.value shouldBe null
            incomingMessage.headers shouldBe listOf(header("key", "value"))
            incomingMessage.leaderEpoch shouldBe consumerRecord.leaderEpoch().get()
            incomingMessage.getTimestampAsInstant() shouldBe Instant.ofEpochMilli(now)
            incomingMessage.isTombStonedMessage() shouldBe true
            incomingMessage.isKeyNull() shouldBe false
            incomingMessage.groupId shouldBe "groupId"
            incomingMessage.clientId shouldBe "clientId"
        }

        test("should create with second timestamp") {
            // arrange
            val now = Instant.now().epochSecond
            val consumerRecord = ConsumerRecord(
                "topic",
                8,
                3,
                now,
                TimestampType.NO_TIMESTAMP_TYPE,
                2,
                3,
                "key",
                "value",
                RecordHeaders(
                    listOf(
                        RecordHeader("key", "value".toByteArray(Charsets.UTF_8))
                    )
                ),
                Optional.of(2)
            )

            // act

            val incomingMessage = IncomingMessage.create(
                consumerRecord,
                IncomingMessageStringFormatter(),
                "groupId",
                "clientId"
            )

            // assert
            incomingMessage.topic shouldBe consumerRecord.topic()
            incomingMessage.partition shouldBe consumerRecord.partition()
            incomingMessage.topicPartition shouldBe TopicPartition(consumerRecord.topic(), consumerRecord.partition())
            incomingMessage.offset shouldBe consumerRecord.offset()
            incomingMessage.timestamp shouldBe consumerRecord.timestamp()
            incomingMessage.timestampType shouldBe consumerRecord.timestampType()
            incomingMessage.serializedKeySize shouldBe consumerRecord.serializedKeySize()
            incomingMessage.serializedValueSize shouldBe consumerRecord.serializedValueSize()
            incomingMessage.key shouldBe consumerRecord.key()
            incomingMessage.headers shouldBe listOf(header("key", "value"))
            incomingMessage.leaderEpoch shouldBe consumerRecord.leaderEpoch().get()
            incomingMessage.getTimestampAsInstant() shouldBe Instant.ofEpochSecond(now)
            incomingMessage.isKeyNull() shouldBe false
            incomingMessage.groupId shouldBe "groupId"
            incomingMessage.clientId shouldBe "clientId"
        }

        test("should create") {
            // arrange
            val now = Instant.now().toEpochMilli()
            val consumerRecord = ConsumerRecord(
                // topic =
                "topic",
                // partition =
                8,
                // offset =
                3,
                // timestamp =
                now,
                // timestampType =
                TimestampType.NO_TIMESTAMP_TYPE,
                // serializedKeySize =
                2,
                // serializedValueSize =
                3,
                // key =
                "key",
                // value =
                "value",
                // headers =
                RecordHeaders(
                    listOf(
                        RecordHeader("key", "value".toByteArray(Charsets.UTF_8))
                    )
                ),
                // leaderEpoch =
                Optional.of(2)
            )

            // act
            val incomingMessage = IncomingMessage.create(
                consumerRecord,
                IncomingMessageStringFormatter(),
                "groupId",
                "clientId"
            )

            // assert
            incomingMessage.topic shouldBe consumerRecord.topic()
            incomingMessage.partition shouldBe consumerRecord.partition()
            incomingMessage.topicPartition shouldBe TopicPartition(consumerRecord.topic(), consumerRecord.partition())
            incomingMessage.offset shouldBe consumerRecord.offset()
            incomingMessage.timestamp shouldBe consumerRecord.timestamp()
            incomingMessage.timestampType shouldBe consumerRecord.timestampType()
            incomingMessage.serializedKeySize shouldBe consumerRecord.serializedKeySize()
            incomingMessage.serializedValueSize shouldBe consumerRecord.serializedValueSize()
            incomingMessage.key shouldBe consumerRecord.key()
            incomingMessage.value shouldBe consumerRecord.value()
            incomingMessage.headers shouldBe listOf(header("key", "value"))
            incomingMessage.leaderEpoch shouldBe consumerRecord.leaderEpoch().get()
            incomingMessage.getTimestampAsInstant() shouldBe Instant.ofEpochMilli(now)
            incomingMessage.isTombStonedMessage() shouldBe false
            incomingMessage.isKeyNull() shouldBe false
            incomingMessage.groupId shouldBe "groupId"
            incomingMessage.clientId shouldBe "clientId"
        }

        test("should equal") {
            // arrange
            val consumerRecord1 = ConsumerRecord(
                "topic",
                8,
                3,
                "key",
                "value"
            )

            val consumerRecord2 = ConsumerRecord(
                "topic",
                8,
                3,
                "key",
                "value"
            )

            // act
            val incomingMessage1 = IncomingMessage.create(
                consumerRecord1,
                IncomingMessageStringFormatter(),
                "groupId",
                "clientId"
            )

            val incomingMessage2 = IncomingMessage.create(
                consumerRecord2,
                IncomingMessageStringFormatter(),
                "groupId",
                "clientId"
            )

            // assert
            incomingMessage1 shouldBe incomingMessage2
            incomingMessage1.hashCode() shouldBe incomingMessage2.hashCode()
        }
    })
