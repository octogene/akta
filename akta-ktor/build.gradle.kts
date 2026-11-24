plugins {
    alias(libs.plugins.kotlin.jvm)
    alias(libs.plugins.kotlin.serialization)       // the API's JSON bodies
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
    api(project(":akta-core"))
    // The routes are added to the host application's Route: Ktor's types are part of this module's API.
    api(libs.ktor.server.core)

    testImplementation(kotlin("test"))
    testImplementation(platform(libs.junit.bom))
    testImplementation(libs.junit.jupiter)
    testRuntimeOnly(libs.junit.platform.launcher)
    testImplementation(libs.ktor.server.test.host)
}

tasks.test {
    useJUnitPlatform()
}

publishing {
    publications {
        create<MavenPublication>("maven") {
            artifactId = "akta-ktor"
            from(components["java"])
        }
    }
}
