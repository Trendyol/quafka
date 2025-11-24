package com.trendyol.quafka.producer

import com.trendyol.quafka.*
import com.trendyol.quafka.common.header
import com.trendyol.quafka.common.toQuafkaHeaders
import com.trendyol.quafka.producer.configuration.*
import io.kotest.assertions.nondeterministic.eventually
import io.kotest.assertions.throwables.shouldThrow
import io.kotest.core.spec.style.FunSpec
import io.kotest.matchers.*
import org.apache.kafka.common.serialization.ByteArraySerializer
import java.util.UUID
import kotlin.time.Duration.Companion.seconds
import kotlin.time.toJavaDuration

class ProducerTests :
    FunSpec({
        val kafka = EmbeddedKafka.instance

        fun createKafkaProducer(
            configure: (
                QuafkaProducerBuilder<ByteArray?, ByteArray?>
            ) -> QuafkaProducerBuilder<ByteArray?, ByteArray?> = {
                it
            }
        ): QuafkaProducer<ByteArray?, ByteArray?> {
            val byteArraySerializer = ByteArraySerializer()

            val quafkaProducer = QuafkaProducerBuilder<ByteArray?, ByteArray?>(kafka.createProducerProperties())
                .withSerializer(byteArraySerializer, byteArraySerializer)

            return configure(quafkaProducer).build()
        }

        test("should not send message to kafka when producer closed") {
            val producer = createKafkaProducer()
                .use { producer ->
                    producer.send(
                        OutgoingMessage.create(
                            "topic8",
                            key = "key1".toByteArray(),
                            value = "value1".toByteArray(),
                            correlationMetadata = UUID.randomUUID().toString(),
                            headers =
                            listOf(
                                header("X-Header", "header value1")
                            )
                        )
                    )
                    producer
                }

            val ex =
                shouldThrow<Throwable> {
                    producer.send(
                        OutgoingMessage.create(
                            "topic8",
                            key = "key2".toByteArray(),
                            value = "value2".toByteArray(),
                            correlationMetadata = UUID.randomUUID().toString(),
                            headers =
                            listOf(
                                header("X-Header", "header value2")
                            )
                        )
                    )
                }
            ex shouldNotBe null
        }

        test("should return result with the request that receives an error when stop on error is disabled") {
            val topic = kafka.getRandomTopicName()
            val outgoingMessage: OutgoingMessage<ByteArray?, ByteArray?> =
                OutgoingMessage.create(
                    topic,
                    key = "key1".toByteArray(),
                    value = "value1".toByteArray(),
                    correlationMetadata = UUID.randomUUID().toString(),
                    headers =
                    listOf(
                        header("X-Header", "header value1")
                    )
                )
            val outgoingMessage2: OutgoingMessage<ByteArray?, ByteArray?> =
                OutgoingMessage.create(
                    "",
                    key = "key2".toByteArray(),
                    value = "value2".toByteArray(),
                    correlationMetadata = UUID.randomUUID().toString(),
                    headers =
                    listOf(
                        header("X-Header", "header value2")
                    )
                )
            createKafkaProducer()
                .use { producer ->
                    val response =
                        producer.sendAll(
                            listOf(outgoingMessage, outgoingMessage2),
                            ProducingOptions(false, false)
                        )

                    response.size shouldBe 2
                    val success = response.first { !it.failed() }
                    val failed = response.first { it.failed() }
                    success.recordMetadata.offset() shouldBe 0
                    failed.exception shouldNotBe null
                }

            kafka.createBytesBytesConsumer(topic).use { consumer ->
                eventually(10.seconds) {
                    val producedMessages = consumer.poll(1.seconds.toJavaDuration())
                    producedMessages.count() shouldBe 1
                    val firstMessage = producedMessages.first()
                    firstMessage.key().toStringWithCharset() shouldBe "key1"
                    firstMessage.value().toStringWithCharset() shouldBe "value1"
                    firstMessage.headers().toQuafkaHeaders() shouldBe listOf(header("X-Header", "header value1"))
                }
            }
        }

        test("should throw exception when an error happens while sending message to kafka") {
            val outgoingMessage: OutgoingMessage<ByteArray?, ByteArray?> =
                OutgoingMessage.create(
                    "",
                    key = "key".toByteArray(),
                    value = "value".toByteArray(),
                    correlationMetadata = UUID.randomUUID().toString(),
                    headers =
                    listOf(
                        header("X-Header", "header value")
                    )
                )

            createKafkaProducer()
                .use { producer ->
                    val exception =
                        shouldThrow<Exception> {
                            producer.send(
                                outgoingMessage
                            )
                        }

                    exception.message shouldNotBe null
                }
        }

        test("should send message to kafka") {
            val topic = kafka.getRandomTopicName()
            val outgoingMessage: OutgoingMessage<ByteArray?, ByteArray?> =
                OutgoingMessage.create(
                    topic,
                    key = "key".toByteArray(),
                    value = "value".toByteArray(),
                    correlationMetadata = UUID.randomUUID().toString(),
                    headers =
                    listOf(
                        header("X-Header", "header value")
                    )
                )
            createKafkaProducer().use { producer ->
                val response = producer.send(
                    outgoingMessage
                )
                response.recordMetadata.offset() shouldBe 0
                response.correlationMetadata shouldBe outgoingMessage.correlationMetadata
                response.exception shouldBe null
            }

            kafka
                .createBytesBytesConsumer(topic)
                .use { consumer ->
                    eventually(10.seconds) {
                        val producedMessage = consumer.poll(1.seconds.toJavaDuration()).first()
                        producedMessage.key().toStringWithCharset() shouldBe "key"
                        producedMessage.value().toStringWithCharset() shouldBe "value"
                        producedMessage.headers().toQuafkaHeaders() shouldBe listOf(header("X-Header", "header value"))
                    }
                }
        }
    })
