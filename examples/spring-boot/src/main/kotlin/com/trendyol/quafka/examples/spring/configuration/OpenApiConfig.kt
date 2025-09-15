package com.trendyol.quafka.examples.spring.configuration

import io.swagger.v3.oas.models.OpenAPI
import io.swagger.v3.oas.models.info.Info
import org.springframework.context.annotation.*

@Configuration
class OpenApiConfig {
    @Bean
    fun customOpenAPI(): OpenAPI = OpenAPI()
        .info(
            Info()
                .title("Spring Boot 3 with Quafka")
                .description("Sample application demonstrating Quafka with Spring Boot 3")
                .version("1.0.0")
        )
}
