rootProject.name = "akta"

dependencyResolutionManagement {
    repositories {
        mavenCentral()
    }
}

// akta-core: the engine, the DSL and an in-memory store for tests. No database dependency.
// akta-exposed: the PostgreSQL store, with Exposed, and its schema.
include("akta-core", "akta-exposed")
