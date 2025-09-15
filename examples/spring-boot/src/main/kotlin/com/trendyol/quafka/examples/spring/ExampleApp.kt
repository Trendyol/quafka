package com.trendyol.quafka.examples.spring

import org.springframework.boot.Banner
import org.springframework.boot.SpringApplication
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication
import org.springframework.context.ConfigurableApplicationContext

@SpringBootApplication
class ExampleApp

fun main(args: Array<String>) {
    run(args)
}

fun run(
    args: Array<String>,
    configureApp: (SpringApplication) -> Unit = {}
): ConfigurableApplicationContext =
    runApplication<ExampleApp>(
        *args,
        init = {
            setBannerMode(Banner.Mode.OFF)
            configureApp(this)
        }
    )
