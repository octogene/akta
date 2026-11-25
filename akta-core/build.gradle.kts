plugins {
    alias(libs.plugins.kotlin.jvm)
    alias(libs.plugins.kotlin.serialization)       // for the tests' @Serializable data
    `java-library`
    `maven-publish`
}

kotlin {
    jvmToolchain(17)
    // A library: every public declaration says so, with its type. Nothing becomes public by accident.
    explicitApi()
    // The public API, recorded in api/: a change to it fails the build until the record is updated on purpose.
    @OptIn(org.jetbrains.kotlin.gradle.dsl.abi.ExperimentalAbiValidation::class)
    abiValidation()
}

java {
    withSourcesJar()
}

dependencies {
    api(libs.kotlinx.coroutines.core)
    // The format of saga inputs and step outputs (SagaSerializer).
    api(libs.kotlinx.serialization.json)

    testImplementation(kotlin("test"))
    testImplementation(platform(libs.junit.bom))
    testImplementation(libs.junit.jupiter)
    testRuntimeOnly(libs.junit.platform.launcher)
}

tasks.test {
    useJUnitPlatform()
    // Forward -Dakta.* options of DifferentialTest (e.g. -Dakta.scripts=50000, -Dakta.seed=42) to the test JVM.
    System.getProperties().filterKeys { (it as String).startsWith("akta.") }
        .forEach { (k, v) -> systemProperty(k as String, v) }
}

publishing {
    publications {
        create<MavenPublication>("maven") {
            artifactId = "akta-core"
            from(components["java"])
        }
    }
}
