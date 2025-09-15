
plugins {
    alias(libs.plugins.spring.plugin)
    alias(libs.plugins.spring.dependencyManagement)
    alias(libs.plugins.spring.boot)
    idea
    application
}

configurations {
    compileOnly {
        extendsFrom(configurations.annotationProcessor.get())
    }
}

dependencies {
    api(project(":lib:quafka"))
    api(project(":lib:quafka-extensions"))
    implementation(libs.spring.boot.webflux)
    implementation(libs.springdoc.openapi)
    implementation(libs.kotlinx.core)
    implementation(libs.kotlinx.slf4j)
    implementation(libs.kotlin.reflect)
    implementation(libs.jackson.kotlin)
    implementation(libs.kotlin.stdlib.jdk8)
    implementation(libs.embedded.kafka)
    annotationProcessor(libs.spring.boot.configuration.processor)
}

application { mainClass.set("com.trendyol.quafka.examples.spring.ExampleAppkt") }
