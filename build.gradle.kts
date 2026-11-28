// The modules' own builds are in akta-core/ and akta-exposed/.
plugins {
    alias(libs.plugins.kotlin.jvm) apply false
    alias(libs.plugins.kotlin.serialization) apply false
    alias(libs.plugins.ktlint) apply false
}

allprojects {
    group = "dev.octogene"
    version = "0.1.0"
}

// ktlint on every module: `./gradlew ktlintCheck` (part of `check`), `./gradlew ktlintFormat` to fix. Style in .editorconfig.
val ktlintPlugin = libs.plugins.ktlint.get().pluginId
val ktlintVersion = libs.versions.ktlint.cli.get()
subprojects {
    apply(plugin = ktlintPlugin)
    configure<org.jlleitschuh.gradle.ktlint.KtlintExtension> {
        version.set(ktlintVersion)
    }
}
