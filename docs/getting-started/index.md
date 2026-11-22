# Installation

Three things to set up: the dependency, the four tables, and the engine itself. Then [write a first saga](first-saga.md).

## The dependency

The library targets **Kotlin 2.x on JVM 17+**, **Exposed 1.x** and **PostgreSQL 12+**. It has no repository yet: publish it to your local Maven repository first.

```bash
./gradlew publishToMavenLocal
```

Then, in the service that uses it:

```kotlin title="build.gradle.kts"
plugins {
    kotlin("plugin.serialization") version "2.4.21"   // saga inputs and outputs are @Serializable
}

repositories {
    mavenLocal()
    mavenCentral()
}

dependencies {
    implementation("com.example:saga-engine:0.1.0")
}
```

The library brings kotlinx.serialization JSON with it (an `api` dependency), since your saga data types use its annotations.

## The database schema

Copy [`schema.sql`](../reference/schema.md) into one of your Flyway migrations, for example `V5__saga.sql`. It creates four tables:

| Table | Content |
|---|---|
| `saga` | One row per saga: its state, current step, attempts, lock, last error, and its input |
| `saga_step_result` | The output of each completed step |
| `saga_signal` | Signals received for steps that wait for one, at most one per name and saga |
| `saga_log` | The history: every call, check, compensation, signal and manual action, with its trace id |

Inputs and outputs are stored as text (JSON), not JSONB: the engine never queries inside them.

## The engine

One engine per application, built once with every saga definition it runs:

```kotlin
val database = Database.connect(dataSource, databaseConfig = DatabaseConfig { defaultMaxAttempts = 1 })

val engine = SagaEngine(
    transactions = ExposedSagaTransactions(database),
    repository = ExposedSagaRepository(),
    definitions = listOf(itemSetup, itemExit),
    serializer = KotlinxJsonSagaSerializer(),
    traceId = { Span.current().spanContext.traceId.takeIf { it != TraceId.getInvalid() } },   // optional
)
```

| Parameter | Meaning |
|---|---|
| `transactions` | How the engine opens its own short transactions. `ExposedSagaTransactions` always uses READ COMMITTED, which the row locking relies on. Implement `SagaTransactions` to use your own helper. |
| `repository` | Where sagas are stored. `ExposedSagaRepository` works on the current Exposed transaction, so `create` can join yours. |
| `definitions` | Every saga type this engine runs. A pod only resumes the types it knows, which keeps rolling deployments safe. |
| `serializer` | The format of inputs and outputs. The engine checks at startup that it can serialize every declared type. |
| `config` | Delays, limits and attempts: see [Configuration](../operations/configuration.md). Defaults suit HTTP calls of up to 30 s. |
| `traceId` | The current OpenTelemetry trace id, written with each history entry. |
| `listeners` | Told about each history entry after its transaction commits, for metrics: see [Observability](../operations/observability.md). |

!!! warning "Turn off Exposed's automatic retries"
    `defaultMaxAttempts = 1` stops Exposed from silently rerunning a failed transaction (3 attempts by default). The engine decides itself what to do after a failure, and must see it.

Next: [your first saga](first-saga.md).
