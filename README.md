# akta

A small, generic saga engine for Kotlin services that call external systems **which do not accept idempotency keys**. State lives in PostgreSQL (through Exposed), so a saga survives restarts, timeouts and crashes, and every call leaves a trace you can investigate.

- A saga is an ordered list of steps. Each step calls an external system, and can undo itself.
- The engine never calls an external system inside a database transaction.
- When the outcome of a call is unknown (timeout, connection reset, `5xx`, crash), the engine **checks** whether the call was applied before it tries again. It never replays blindly. Steps whose service does accept idempotency keys are replayed with the same key instead.
- When it cannot decide, it stops and waits for a human, instead of risking a duplicate.
- A saga can carry a typed **input**, fixed at creation, and each step a typed **output**, read by the steps after it, stored as JSON through kotlinx.serialization.
- A step can also **wait for a signal**, an event from outside such as a payment confirmation, with a timeout.

**Documentation: <https://octogene.github.io/akta/>**

## A saga in short

```kotlin
@Serializable data class SetupRequest(val epc: String, val storeId: String)   // the input
@Serializable data class StockEntry(val id: String, val location: String)     // a step's output

val itemSetup = saga<SetupRequest>("ITEM_SETUP") {
    val stock = step<StockEntry>("stock") {
        execute { stockClient.moveToTestStock(input.epc, input.storeId) }
        compensate { own -> own?.let { stockClient.revertTestStockEntry(it.id) } }   // undo, safe to repeat
        checkByLookup { stockClient.findTestStockEntries(input.epc) }                // was I applied?
        logAs { it.id }                                                              // what the history shows
    }
    step<String>("booking") {
        execute { bookingClient.createResource(input.epc, stockId = requireResult(stock).id) }
        checkByLookup { bookingClient.findResources(input.epc) }
    }
    onCompleted { items.markAvailable(key) }                  // in the saga's last transaction
}
```

Create it in the business transaction, then run it after the commit:

```kotlin
val sagaId = db.tx {
    val item = items.insert(epc, storeId)
    engine.create(itemSetup, key = "item:${item.id}", input = SetupRequest(epc, storeId))
}
val saga = engine.run(sagaId, deadline = Clock.System.now() + 8.seconds)   // COMPLETED, FAILED, or still going
```

A recovery job calls `engine.resumeDue()` every 30 seconds to take on whatever is due: retries, outcomes to check, calls whose pod died.

## Modules

| Module | Content | Depends on |
|---|---|---|
| `akta-core` | The engine, the DSL, the model, and an in-memory store for tests | kotlinx.coroutines, kotlinx.serialization |
| `akta-exposed` | The PostgreSQL store (`ExposedSagaRepository`, `ExposedSagaTransactions`) and its schema | `akta-core`, Exposed |
| `akta-ktor` | An admin API for operators (`sagaAdminRead`, `sagaAdminActions`), mounted in your Ktor application | `akta-core`, Ktor server |
| `akta-opentelemetry` | A span per step call, and the engine's events and stats as metrics, with OpenTelemetry | `akta-core`, OpenTelemetry API |

It targets Kotlin 2.x on JVM 17+, Exposed 1.x and PostgreSQL 12+. There is no published release yet: `./gradlew publishToMavenLocal`, then depend on `dev.octogene:akta-exposed:0.1.0`, and copy [`schema.sql`](akta-exposed/src/main/resources/dev/octogene/akta/schema.sql) into a Flyway migration. [Installation](https://octogene.github.io/akta/getting-started/) has the details.

## Where to read next

| To… | Read |
|---|---|
| Install it and write a first saga | [Getting started](https://octogene.github.io/akta/getting-started/) |
| See it applied to real cases: two systems, a payment event, services with keys | [Use cases](https://octogene.github.io/akta/use-cases/) |
| Fit it into a Ktor service with clean architecture | [The guide](https://octogene.github.io/akta/guides/clean-architecture/) |
| Understand the step cycle, unknown outcomes, compensation, signals, recovery | [How it works](https://octogene.github.io/akta/how-it-works/) |
| Know exactly what is guaranteed, and the limits to design around | [Guarantees and limits](https://octogene.github.io/akta/how-it-works/guarantees/) |
| Run it in production: stuck sagas, metrics, alerts, settings | [Operations](https://octogene.github.io/akta/operations/) |
| Give operators an HTTP API to find and resolve sagas | [Admin API for Ktor](https://octogene.github.io/akta/operations/admin-api/) |
| Look up an event, a table or an index | [Reference](https://octogene.github.io/akta/reference/) |

## Building

```bash
./gradlew test             # unit tests, and the PostgreSQL tests (Testcontainers: Docker required)
./gradlew stressTest       # many sagas, random failures and crashes, invariants checked (about 10 s)
./gradlew benchmark        # the cost of a saga against PostgreSQL (about 40 s)
./gradlew checkKotlinAbi   # the public API matches api/*.api; after a change made on purpose: ./gradlew updateKotlinAbi
```

The documentation is built from `docs/` with Material for MkDocs, and published on every push to `master`:

```bash
pipx run --spec mkdocs-material==9.6.14 mkdocs serve            # http://127.0.0.1:8000, reloads on edit
pipx run --spec mkdocs-material==9.6.14 mkdocs build --strict   # static site in site/
```

## License

MIT: see [LICENSE](LICENSE).
