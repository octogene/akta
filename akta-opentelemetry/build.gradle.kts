plugins {
    alias(libs.plugins.kotlin.jvm)
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
    // The application gives its OpenTelemetry (an SDK, or the Java agent's): only the API is needed here.
    api(libs.opentelemetry.api)
    // The span made current in the call's coroutine, so that the step's own spans (HTTP clients) are its children.
    implementation(libs.opentelemetry.extension.kotlin)

    testImplementation(kotlin("test"))
    testImplementation(platform(libs.junit.bom))
    testImplementation(libs.junit.jupiter)
    testRuntimeOnly(libs.junit.platform.launcher)
    testImplementation(libs.opentelemetry.sdk.testing)
}

tasks.test {
    useJUnitPlatform()
}

publishing {
    publications {
        create<MavenPublication>("maven") {
            artifactId = "akta-opentelemetry"
            from(components["java"])
        }
    }
}
