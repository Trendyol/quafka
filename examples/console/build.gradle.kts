@file:Suppress("VulnerableLibrariesLocal")

plugins {
    idea
    application
}

dependencies {
    api(project(":lib:quafka"))
    api(project(":lib:quafka-extensions"))
    api(libs.kotlinx.slf4j)
    implementation(libs.logback.classic)
}

application { mainClass.set("com.trendyol.quafka.examples.console.Appkt") }
