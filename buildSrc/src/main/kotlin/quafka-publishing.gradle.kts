plugins {
    `maven-publish`
    signing
    java
}

fun getProperty(
    projectKey: String,
    environmentKey: String
): String? = if (project.hasProperty(projectKey)) {
    project.property(projectKey) as? String?
} else {
    System.getenv(environmentKey)
}

publishing {
    publications {
        create<MavenPublication>("publish-${project.name}") {
            groupId = rootProject.group.toString()
            version = rootProject.version.toString()
            println("version to be published: ${rootProject.version}")
            artifactId = project.name
            from(components["java"])
            pom {
                name.set(project.name)
                description.set(project.properties["projectDescription"].toString())
                url.set(project.properties["projectUrl"].toString())
                packaging = "jar"
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
        }
    }

    repositories {
        maven {
            name = "GitHubPackages"
            url = uri("https://maven.pkg.github.com/trendyol/quafka")
            credentials {
                username = System.getenv("GITHUB_ACTOR")
                password = System.getenv("GITHUB_TOKEN")
            }
        }
        maven {
            val releasesRepoUrl = uri("https://oss.sonatype.org/service/local/staging/deploy/maven2/")
            val snapshotsRepoUrl = uri("https://oss.sonatype.org/content/repositories/snapshots/")
            url = if (rootProject.version.toString().endsWith("SNAPSHOT")) snapshotsRepoUrl else releasesRepoUrl
            credentials {
                username = getProperty("nexus_username", "nexus_username")
                password = getProperty("nexus_password", "nexus_password")
            }
        }
    }
}

val signingKey = getProperty(projectKey = "gpg.key", environmentKey = "gpg_private_key")
val passPhrase = getProperty(projectKey = "gpg.passphrase", environmentKey = "gpg_passphrase")
signing {
    if (passPhrase == null && runningOnCI) {
        logger.warn(
            "The passphrase for the signing key was not found. " +
                "Either provide it as env variable 'gpg_passphrase' or " +
                "as project property 'gpg_passphrase'. Otherwise the signing might fail!"
        )
    }
    useInMemoryPgpKeys(signingKey, passPhrase)
    sign(publishing.publications)
}
