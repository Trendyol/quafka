package com.trendyol.quafka.examples.spring.controllers

import org.springframework.http.HttpStatus
import org.springframework.web.bind.annotation.*
import org.springframework.web.server.ServerWebExchange
import reactor.core.publisher.Mono
import java.net.URI

@RestController
class RootRedirectController {
    @GetMapping("/")
    fun redirectToSwaggerUi(exchange: ServerWebExchange): Mono<Void> {
        exchange.response.setStatusCode(HttpStatus.PERMANENT_REDIRECT)
        exchange.response.headers.location = URI.create("/swagger-ui")
        return exchange.response.setComplete()
    }
}
