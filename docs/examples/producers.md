## Producer Examples

### Basic producer and single message
```kotlin
import com.trendyol.quafka.producer.*
import com.trendyol.quafka.producer.configuration.QuafkaProducerBuilder
import org.apache.kafka.common.serialization.StringSerializer

val props = HashMap<String, Any>()
props[org.apache.kafka.clients.producer.ProducerConfig.BOOTSTRAP_SERVERS_CONFIG] = "localhost:9092"

val producer = QuafkaProducerBuilder<String?, String?>(props)
    .withSerializer(StringSerializer(), StringSerializer())
    .build()

val result: DeliveryResult = producer.send(
    OutgoingMessage.create(
        topic = "test-topic",
        key = "key",
        value = "value",
        headers = emptyList()
    )
)
```

### Batch publishing
```kotlin
val messages = (1..100).map { index ->
    OutgoingMessage.create(
        topic = "test-topic",
        key = "key-$index",
        value = "value-$index"
    )
}
val results: Collection<DeliveryResult> = producer.sendAll(messages)
```

### JSON payloads with extensions builder
```kotlin
import com.fasterxml.jackson.databind.ObjectMapper
import com.trendyol.quafka.extensions.producer.OutgoingMessageBuilder
import com.trendyol.quafka.extensions.serialization.json.ByteArrayJsonMessageSerde
import com.trendyol.quafka.extensions.serialization.json.AutoPackageNameBasedTypeResolver
import com.trendyol.quafka.extensions.serialization.json.HeaderAwareTypeNameExtractor

val serde = ByteArrayJsonMessageSerde(ObjectMapper(), AutoPackageNameBasedTypeResolver(HeaderAwareTypeNameExtractor()))
val outgoingBuilder = OutgoingMessageBuilder(serde)

data class Command(val id: Int)

val jsonMessage = outgoingBuilder
    .newMessageWithTypeInfo("test-topic", Command(42), key = "42")
    .build()

producer.send(jsonMessage)
```

### Producing options
```kotlin
import com.trendyol.quafka.producer.configuration.QuafkaProducerBuilder

val strictProducer = QuafkaProducerBuilder<String?, String?>(props)
    .withSerializer(StringSerializer(), StringSerializer())
    .withErrorOptions(ProducingOptions(stopOnFirstError = true))
    .build()
```
