package com.trendyol.quafka.extensions

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule

object Defaults {
    val objectMapper = ObjectMapper().registerKotlinModule()
}
