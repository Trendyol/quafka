package com.trendyol.quafka.producer
import org.apache.kafka.clients.producer.*
import kotlin.coroutines.*

/**
 * Extension function to send a Kafka message asynchronously using coroutines.
 *
 * This function provides a suspending wrapper around the `KafkaProducer.send` method, enabling
 * seamless integration with coroutine-based code. The result of the send operation is returned
 * as a [RecordMetadata] object. If an exception occurs during the send operation, it will
 * be thrown as part of the coroutine's execution flow.
 *
 * @param K The type of the key for the Kafka message.
 * @param V The type of the value for the Kafka message.
 * @param record The [ProducerRecord] containing the message to be sent.
 * @return A [RecordMetadata] object containing metadata about the successfully sent record.
 * @throws Exception If the send operation fails, the exception is thrown within the coroutine.
 */
suspend fun <K, V> KafkaProducer<K, V>.sendAsync(record: ProducerRecord<K, V>) =
    suspendCoroutine<RecordMetadata> {
        this.send(record) { metadata, exception ->
            if (exception != null) {
                it.resumeWithException(exception)
            } else {
                it.resume(metadata)
            }
        }
    }
