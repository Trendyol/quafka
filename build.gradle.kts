import org.gradle.kotlin.dsl.libs
import org.gradle.plugins.ide.idea.model.IdeaModel
import org.jetbrains.kotlin.gradle.dsl.JvmTarget

plugins {
    kotlin("jvm").version(libs.versions.kotlin)
    alias(libs.plugins.spotless)
    alias(libs.plugins.testLogger)
    alias(libs.plugins.kover)
    alias(libs.plugins.maven.publish)
    idea
    java
}
group = "com.trendyol"
version = CI.version(project)

allprojects {
    extra.set("dokka.outputDirectory", rootDir.resolve("docs"))
}

kover {
    reports {
        filters {
            excludes {
                classes(
                    "com.trendyol.quafka.examples.*"
                )
            }
        }
    }
}
val koverProjects = subprojects.of("lib")
dependencies {
    koverProjects.forEach {
        kover(it)
    }
}

subprojects.of("lib", "examples") {
    apply {
        plugin("kotlin")
        plugin(
            rootProject.libs.plugins.spotless
                .get()
                .pluginId
        )
        plugin(
            rootProject.libs.plugins.testLogger
                .get()
                .pluginId
        )
        plugin(
            rootProject.libs.plugins.kover
                .get()
                .pluginId
        )

        plugin("idea")
        plugin("java-test-fixtures")
    }

    val testImplementation by configurations
    val libs = rootProject.libs

    dependencies {
        testImplementation(kotlin("test"))
        testImplementation(libs.kotest.runner.junit5)
        testImplementation(libs.kotest.framework.api)
        testImplementation(libs.kotest.property)
    }

    spotless {
        kotlin {
            ktlint(libs.versions.ktlint.get()).setEditorConfigPath(rootProject.layout.projectDirectory.file(".editorconfig"))
            targetExclude("build/", "generated/", "out/")
            targetExcludeIfContentContains("generated")
            targetExcludeIfContentContainsRegex("generated.*")
        }
    }

    the<IdeaModel>().apply {
        module {
            isDownloadSources = true
            isDownloadJavadoc = true
        }
    }

    tasks {
        compileKotlin {
            incremental = true
        }
        test {
            maxParallelForks = 1
            useJUnitPlatform()
            testlogger {
                setTheme("mocha")
                showStandardStreams = !runningOnCI
                showExceptions = true
                showCauses = true
            }
            reports {
                junitXml.required.set(true)
                junitXml.outputLocation = file("reports/test/xml")
            }
            jvmArgs("--add-opens", "java.base/java.util=ALL-UNNAMED -XX:+EnableDynamicAgentLoading")
        }
        kotlin {
            jvmToolchain(21)
            compilerOptions {
                jvmTarget.set(JvmTarget.JVM_21)
                allWarningsAsErrors = false
                freeCompilerArgs.addAll(
                    "-Xjsr305=strict",
                    "-Xcontext-receivers"
                )
            }
        }
    }
}

val publishedProjects = listOf(
    "quafka",
    "quafka-extensions"
)

subprojects.of("lib", filter = { p -> publishedProjects.contains(p.name) }) {
    apply {
        plugin("java")
        plugin(rootProject.libs.plugins.maven.publish.pluginId)
    }

    mavenPublishing {
        coordinates(groupId = rootProject.group.toString(), artifactId = project.name, version = rootProject.version.toString())
        publishToMavenCentral()
        pom {
            name.set(project.name)
            description.set(project.properties["projectDescription"].toString())
            url.set(project.properties["projectUrl"].toString())
            licenses {
                license {
                    name.set(project.properties["licence"].toString())
                    url.set(project.properties["licenceUrl"].toString())
                }
            }
            developers {
                developer {
                    id.set("oguzhaneren")
                    name.set("Oğuzhan Eren")
                }
            }
            scm {
                connection.set("scm:git@github.com:Trendyol/quafka.git")
                developerConnection.set("scm:git:ssh://github.com:Trendyol/quafka.git")
                url.set(project.properties["projectUrl"].toString())
            }
        }
        signAllPublications()
    }

    java {
        withSourcesJar()
        // withJavadocJar()
    }
}
