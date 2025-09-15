package com.trendyol.quafka

import io.kotest.core.annotation.AutoScan
import io.kotest.core.config.AbstractProjectConfig
import java.time.Instant

@AutoScan
object EmbeddedKafka : AbstractProjectConfig() {
    val instance: KafkaExtension by lazy {
        KafkaExtension().apply { start() }
    }

    override suspend fun beforeProject() {
        println("FakeKafkaExtensions starting, ${Instant.now()}")
        println(instance.getBootstrapServers())
        println("FakeKafkaExtensions started, ${Instant.now()}")
    }

    override suspend fun afterProject() {
        instance.stop()
    }
}
