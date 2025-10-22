
dependencies {
    api(libs.slf4j.api)
    api(libs.resillience4j.kotlin)
    api(libs.resillience4j.retry)
    api(libs.kafka)
    api(libs.kotlinx.core)
    implementation(libs.kotlinx.jdk8)
    api(libs.kotlinx.slf4j)
    api(libs.kotlin.stdlib.jdk8)

    testImplementation(libs.logback.classic)
    testFixturesApi(libs.kotest.runner.junit5)
    testFixturesApi(libs.kotest.framework.api.jvm)
    testFixturesApi(libs.kotest.property.jvm)
    testFixturesApi(libs.embedded.kafka)
    testFixturesApi(libs.kotest.assertion)
    testFixturesApi(libs.kotest.datatests)
    testFixturesApi(libs.mockk)
}
