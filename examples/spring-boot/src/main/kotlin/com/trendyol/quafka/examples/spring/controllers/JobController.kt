package com.trendyol.quafka.examples.spring.controllers

import com.trendyol.quafka.common.header
import com.trendyol.quafka.examples.spring.configuration.KafkaConfig
import com.trendyol.quafka.examples.spring.models.Job
import com.trendyol.quafka.examples.spring.persistence.JobRepository
import com.trendyol.quafka.extensions.producer.OutgoingMessageBuilder
import com.trendyol.quafka.extensions.serialization.json.typeResolvers.newMessageWithTypeInfo
import com.trendyol.quafka.producer.QuafkaProducer
import org.springframework.web.bind.annotation.*
import org.springframework.web.server.ServerWebExchange
import java.time.Instant

@RestController
@RequestMapping("/jobs")
class JobController(
    private val outgoingMessageBuilder: OutgoingMessageBuilder<ByteArray?, ByteArray?>,
    private val producer: QuafkaProducer<ByteArray?, ByteArray?>,
    private val jobRepository: JobRepository
) {
    @PostMapping()
    suspend fun send(
        @RequestBody request: Requests.CreateJobRequest,
        exchange: ServerWebExchange
    ): Requests.JobRequestAccepted {
        val httpRequestId = exchange.request.id
        val newRequest = Requests.JobRequest(id = httpRequestId, request.payload)

        val message = outgoingMessageBuilder
            .newMessageWithTypeInfo(KafkaConfig.Topics.job.name(), null, newRequest)
            .withHeader(header("X-RequestId", httpRequestId))
            .withHeader(header("X-PublishedAt", Instant.now().toString()))
            .build()

        producer.send(message)
        return Requests.JobRequestAccepted(newRequest.id, message.correlationMetadata)
    }

    @GetMapping("/:id")
    suspend fun get(
        @RequestParam id: String,
        exchange: ServerWebExchange
    ): Job? = jobRepository.getById(id)

    @GetMapping()
    suspend fun getAll(
        exchange: ServerWebExchange
    ): Collection<Job> = jobRepository.getAll()
}
