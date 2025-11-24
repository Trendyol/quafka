package com.trendyol.quafka.examples.spring.controllers

object Requests {
    data class CreateJobRequest(val payload: String)

    data class JobRequest(val id: String, val payload: String)

    data class JobRequestAccepted(val id: String, val correlationMetadata: String)
}
