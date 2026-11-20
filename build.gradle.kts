plugins {
    alias(libs.plugins.kotlin.jvm)
    `java-library`
    `maven-publish`
}

group = "com.example"
version = "0.1.0"

repositories {
    mavenCentral()
}

kotlin {
    jvmToolchain(17)
}

java {
    withSourcesJar()
}

dependencies {
    api(libs.kotlinx.coroutines.core)
    // Exposed is only used by the com.example.saga.exposed package.
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
            artifactId = "saga-engine"
            from(components["java"])
        }
    }
}
