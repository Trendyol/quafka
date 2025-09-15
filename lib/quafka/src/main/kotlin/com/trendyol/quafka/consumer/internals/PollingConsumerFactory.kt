package com.trendyol.quafka.consumer.internals

import com.trendyol.quafka.common.Waiter
import com.trendyol.quafka.consumer.configuration.*
import kotlinx.coroutines.CoroutineScope
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.slf4j.*

/**
 * Factory class responsible for creating and configuring a [PollingConsumer] instance.
 *
 * This factory encapsulates the initialization of a Kafka consumer along with its supporting components,
 * such as the logger, offset manager, and partition assignment manager. The resulting [PollingConsumer]
 * is configured to poll and process messages based on the provided consumer options.
 *
 * @param TKey The type of the key used in the Kafka messages.
 * @param TValue The type of the value used in the Kafka messages.
 */
internal class PollingConsumerFactory<TKey, TValue> {
    /**
     * Creates a [PollingConsumer] configured with the provided consumer options and coroutine scope.
     *
     * This method performs the following steps:
     * 1. Instantiates a [KafkaConsumer] using the properties and deserializers from [quafkaConsumerOptions].
     * 2. Initializes an [OffsetManager] to track and commit offsets.
     * 3. Sets up a [PartitionAssignmentManager] to manage partition assignments and rebalances.
     * 4. Constructs and returns a [PollingConsumer] which ties together the above components.
     *
     * @param quafkaConsumerOptions The configuration options for the consumer, including properties, deserializers,
     *                              client and group IDs, and other settings.
     * @param scope The [CoroutineScope] in which the consumer's asynchronous operations will execute.
     * @param waiter An optional [Waiter] used to signal when the consumer has fully stopped.
     * @param logger The [Logger]
     * @return A fully configured [PollingConsumer] instance ready for consumption.
     */
    internal fun create(
        quafkaConsumerOptions: QuafkaConsumerOptions<TKey, TValue>,
        scope: CoroutineScope,
        waiter: Waiter?,
        logger: Logger
    ): PollingConsumer<TKey, TValue> {
        val consumer = KafkaConsumer(
            quafkaConsumerOptions.properties,
            quafkaConsumerOptions.keyDeserializer,
            quafkaConsumerOptions.valueDeserializer
        )

        val offsetManager = OffsetManager(
            kafkaConsumer = consumer,
            quafkaConsumerOptions = quafkaConsumerOptions,
            logger = logger
        )

        val partitionAssignmentManager = PartitionAssignmentManager(
            kafkaConsumer = consumer,
            logger = logger,
            parentScope = scope,
            quafkaConsumerOptions = quafkaConsumerOptions,
            offsetManager = offsetManager
        )

        return PollingConsumer(
            quafkaConsumerOptions = quafkaConsumerOptions,
            consumer = consumer,
            partitionAssignmentManager = partitionAssignmentManager,
            offsetManager = offsetManager,
            scope = scope,
            logger = logger,
            waiter = waiter
        )
    }
}
