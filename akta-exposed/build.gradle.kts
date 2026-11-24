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
    api(project(":akta-core"))
    // ExposedSagaTransactions takes an Exposed Database, and the repository joins Exposed transactions.
    api(libs.exposed.core)
    api(libs.exposed.jdbc)
    implementation(libs.exposed.kotlin.datetime)

    testImplementation(kotlin("test"))
    testImplementation(platform(libs.junit.bom))
    testImplementation(libs.junit.jupiter)
    testRuntimeOnly(libs.junit.platform.launcher)
    testImplementation(libs.testcontainers.postgresql)
    testRuntimeOnly(libs.postgresql)
    testImplementation(libs.hikaricp)
}

tasks.test {
    useJUnitPlatform {
        excludeTags("benchmark", "stress")         // slow, Docker: run with ./gradlew benchmark / stressTest
    }
}

// Long-running checks against PostgreSQL (Testcontainers: Docker required). Never up to date: they measure.
fun registerTaggedTest(name: String, tag: String, description: String) = tasks.register<Test>(name) {
    this.description = description
    group = "verification"
    testClassesDirs = sourceSets.test.get().output.classesDirs
    classpath = sourceSets.test.get().runtimeClasspath
    useJUnitPlatform { includeTags(tag) }
    outputs.upToDateWhen { false }
    testLogging { showStandardStreams = true }
    // Forward -Dsaga.* options (e.g. -Dsaga.sagas=2000, -Dsaga.seed=42) to the test JVM.
    System.getProperties().filterKeys { (it as String).startsWith("saga.") }
        .forEach { (k, v) -> systemProperty(k as String, v) }
}

registerTaggedTest("benchmark", "benchmark", "Measures the cost of a saga against PostgreSQL.")
registerTaggedTest("stressTest", "stress", "Runs many sagas with random failures and checks the invariants.")

publishing {
    publications {
        create<MavenPublication>("maven") {
            artifactId = "akta-exposed"
            from(components["java"])
        }
    }
}
