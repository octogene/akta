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
    // The routes are added to the host application's Route: Ktor's types are part of this module's API.
    api(libs.ktor.server.core)
    // The pages, built with kotlinx.html: type-safe, and every text escaped.
    implementation(libs.ktor.server.html.builder)

    testImplementation(kotlin("test"))
    testImplementation(platform(libs.junit.bom))
    testImplementation(libs.junit.jupiter)
    testRuntimeOnly(libs.junit.platform.launcher)
    testImplementation(libs.ktor.server.test.host)
    // The demo (./gradlew :akta-ktor-console:runDemo): a real server, for a browser.
    testImplementation(libs.ktor.server.cio)
}

tasks.test {
    useJUnitPlatform()
}

// A console on an in-memory engine with sagas in every state, at http://127.0.0.1:8089/admin/console/
tasks.register<JavaExec>("runDemo") {
    group = "application"
    description = "Runs the console on demo sagas, at http://127.0.0.1:8089/admin/console/"
    classpath = sourceSets["test"].runtimeClasspath
    mainClass.set("dev.octogene.akta.console.DemoKt")
}

publishing {
    publications {
        create<MavenPublication>("maven") {
            artifactId = "akta-ktor-console"
            from(components["java"])
        }
    }
}
