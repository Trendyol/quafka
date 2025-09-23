import org.gradle.api.Project

object CI {
    private val isSnapshot: Boolean
        get() = System.getenv("SNAPSHOT") != null && System.getenv("SNAPSHOT") == "true"

    private val Project.snapshotBase: String
        get() = properties["snapshot"].toString()

    private val Project.releaseVersion: String
        get() = properties["version"].toString()

    private val buildNumber: String
        get() = System.getenv("BUILD_NUMBER") ?: "0"

    fun version(project: Project): String = when {
        isSnapshot -> "${project.snapshotBase}.$buildNumber-SNAPSHOT"
        else -> project.properties["version"].toString()
    }
}
