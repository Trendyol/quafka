package com.trendyol.quafka.examples.spring.persistence

import com.trendyol.quafka.examples.spring.models.Job
import org.springframework.stereotype.Component
import java.util.concurrent.ConcurrentHashMap

@Component
class JobRepository {
    private val jobs = ConcurrentHashMap<String, Job>()

    fun save(job: Job) {
        jobs[job.id] = job
    }

    fun getAll() = jobs.values

    fun getById(id: String) = jobs[id]
}
