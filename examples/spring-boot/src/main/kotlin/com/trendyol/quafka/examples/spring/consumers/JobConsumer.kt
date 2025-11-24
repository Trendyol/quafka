package com.trendyol.quafka.examples.spring.consumers

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.readValue
import com.trendyol.quafka.common.HeaderParsers.asString
import com.trendyol.quafka.common.HeaderParsers.key
import com.trendyol.quafka.consumer.configuration.*
import com.trendyol.quafka.examples.spring.configuration.KafkaConfig
import com.trendyol.quafka.examples.spring.controllers.Requests
import com.trendyol.quafka.examples.spring.models.Job
import com.trendyol.quafka.examples.spring.persistence.JobRepository
import com.trendyol.quafka.logging.LoggerHelper
import org.slf4j.Logger
import org.slf4j.event.Level
import org.springframework.stereotype.Component

@Component
class JobConsumer(private val jobRepository: JobRepository, private val objectMapper: ObjectMapper) : Consumer<ByteArray, ByteArray> {
    private val logger: Logger = LoggerHelper.createLogger(this.javaClass)

    override fun configure(
        builder: QuafkaConsumerBuilder<ByteArray, ByteArray>
    ): SubscriptionBuildStep<ByteArray, ByteArray> = builder.subscribe(KafkaConfig.Topics.job.name()) {
        withSingleMessageHandler { incomingMessage, consumerContext ->

            val request = objectMapper.readValue<Requests.JobRequest>(incomingMessage.value)

            jobRepository.save(
                Job(
                    id = request.id,
                    payload = request.payload,
                    offset = incomingMessage.offset,
                    topic = incomingMessage.topic,
                    partition = incomingMessage.partition,
                    headers = incomingMessage.headers.associate { it.key to it.asString() }
                )
            )
            logger.info(
                "incoming message: {}",
                incomingMessage.toString(logLevel = Level.INFO, addValue = true, addHeaders = true)
            )
            incomingMessage.ack()
        }
    }
}
