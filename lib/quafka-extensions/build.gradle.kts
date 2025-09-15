dependencies {
    api(project(":lib:quafka"))
    api(libs.jackson.core)
    api(libs.jackson.databind)
    api(libs.jackson.kotlin)

    testImplementation(libs.logback.classic)
    testImplementation(testFixtures(project(":lib:quafka")))
}
