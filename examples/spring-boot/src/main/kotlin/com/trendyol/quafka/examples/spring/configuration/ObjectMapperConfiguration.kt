package com.trendyol.quafka.examples.spring.configuration

import com.fasterxml.jackson.databind.*
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import org.springframework.context.annotation.*

@Configuration
class ObjectMapperConfiguration {
    @Bean
    fun customObjectMapper(): ObjectMapper {
        val objectMapper = ObjectMapper()
            .enable(SerializationFeature.INDENT_OUTPUT)
            .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
            .disable(SerializationFeature.FAIL_ON_EMPTY_BEANS)
            .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
            .registerKotlinModule()
        return objectMapper
    }
}
