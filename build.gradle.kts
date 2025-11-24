import com.vanniktech.maven.publish.JavadocJar
import com.vanniktech.maven.publish.KotlinJvm
import org.gradle.plugins.ide.idea.model.IdeaModel
import org.jetbrains.kotlin.gradle.dsl.JvmTarget

plugins {
    kotlin("jvm").version(libs.versions.kotlin)
    alias(libs.plugins.spotless)
    alias(libs.plugins.testLogger)
    alias(libs.plugins.kover)
    alias(libs.plugins.maven.publish)
    alias(libs.plugins.dokka)
    alias(libs.plugins.javadoc)
    idea
    java
}
group = "com.trendyol"
version = CI.version(project)

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

val publishedProjects = subprojects.of("lib")
dependencies {
    publishedProjects.forEach {
        kover(it)
        dokka(it)
    }
}

subprojects.of("lib", "examples") {
    val p = this
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

publishedProjects.forEvery {
    val p = this

    apply {
        plugin("java")
        plugin(rootProject.libs.plugins.maven.publish.pluginId)
        plugin(rootProject.libs.plugins.dokka.pluginId)
        plugin(rootProject.libs.plugins.javadoc.pluginId)
    }

    dokka {
        dokkaPublications.html {
            suppressInheritedMembers.set(true)
            failOnWarning.set(false)
        }
        dokkaSourceSets.main {
            skipDeprecated = false
            sourceLink {
                localDirectory = project.projectDir
                remoteUrl("https://github.com/Trendyol/quafka/tree/master/lib/${p.name}/src")
                remoteLineSuffix.set("#L")
            }
            samples.from("examples")
        }
        pluginsConfiguration.html {
            footerMessage.set("(c) Trendyol")
        }
    }

    configure<JavaPluginExtension> {
        withSourcesJar()
    }

    configure<PublishingExtension> {
        repositories {
            maven {
                name = "GitHubPackages"
                url = uri("https://maven.pkg.github.com/Trendyol/quafka")
                credentials {
                    username = System.getenv("GITHUB_ACTOR")
                    password = System.getenv("GITHUB_TOKEN")
                }
            }
        }
    }

    mavenPublishing {
        configure(
            KotlinJvm(
                javadocJar = JavadocJar.Dokka("dokkaGenerateJavadoc"),
                sourcesJar = true
            )
        )
        publishToMavenCentral()
        coordinates(groupId = rootProject.group.toString(), artifactId = project.name, version = rootProject.version.toString())

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
        if (isGithubActions) {
            signAllPublications()
        }
    }
}

dokka {
    dokkaPublications.html {
        outputDirectory.set(rootDir.resolve("_dokka"))

        pluginsConfiguration.html {
            customAssets.from("docs/assets/logo.jpeg")
            footerMessage.set("(c) Trendyol")
        }
        includes.from(
            project.layout.projectDirectory.file("README.md"),
            project.layout.projectDirectory.dir("docs/fundamentals.md")
        )
    }
}
