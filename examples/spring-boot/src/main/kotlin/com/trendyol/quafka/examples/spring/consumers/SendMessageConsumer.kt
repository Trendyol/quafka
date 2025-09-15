package com.trendyol.quafka.examples.spring.consumers

import com.trendyol.quafka.consumer.*
import com.trendyol.quafka.logging.LoggerHelper
import org.slf4j.Logger
import org.springframework.stereotype.Component

@Component
class JobConsume2r {
    private val logger: Logger = LoggerHelper.createLogger(this.javaClass)

    suspend fun consume(incomingMessage: IncomingMessage<ByteArray, ByteArray>, consumerContext: ConsumerContext) {
        logger.info(
            "incoming message: {}",
            incomingMessage.toString(addValue = true, addHeaders = true)
        )
        incomingMessage.ack()
    }

    suspend fun consume(incomingMessages: Collection<IncomingMessage<ByteArray, ByteArray>>, consumerContext: ConsumerContext) {
        incomingMessages.forEach {
            consume(it, consumerContext)
        }
    }
}
