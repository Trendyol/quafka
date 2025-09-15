package com.trendyol.quafka.examples.spring.configuration

import org.apache.kafka.clients.admin.NewTopic

object KafkaConfig {
    object Topics {
        val job = NewTopic("quafka.spring.job.0", 3, 1)
        val alltopics = listOf(
            job
        )
    }

    val clientId = "quafka.spring.client-id"
    val groupId = "quafka.spring"
}
