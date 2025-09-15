@file:Suppress("UnstableApiUsage")

import org.gradle.internal.impldep.org.eclipse.jgit.hooks.Hooks.preCommit

enableFeaturePreview("TYPESAFE_PROJECT_ACCESSORS")

rootProject.name = "quafka"
include(
    "lib",
    "lib:quafka",
    "lib:quafka-extensions"
)

include(
    "examples:spring-boot",
    "examples:console"
)

dependencyResolutionManagement {
    repositories {
        mavenCentral()
        maven {
            url = uri("https://packages.confluent.io/maven/")
        }
    }
}
plugins {
    id("org.danilopianini.gradle-pre-commit-git-hooks").version("2.0.30")
}
gitHooks {
    preCommit {
        from(rootDir.resolve("pre-commit.sh"))
    }
    createHooks(overwriteExisting = true)
}
