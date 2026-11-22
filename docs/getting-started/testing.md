# Testing your sagas

Every path of a saga can be tested without a database: the engine runs on an in-memory repository and a clock you move forward by hand.

```kotlin
class MutableClock(var now: Instant = Instant.parse("2026-11-22T10:00:00Z")) : Clock {
    override fun now() = now
    fun advance(d: Duration) { now += d }
}

val clock = MutableClock()
val repository = InMemorySagaRepository()
val itemSetup = itemSetupSaga(fakeStock, fakeBooking, fakeItems)
val engine = SagaEngine(
    InMemorySagaTransactions(repository), repository, listOf(itemSetup),
    KotlinxJsonSagaSerializer(), clock = clock,
)
```

`InMemorySagaTransactions(repository)` rolls back a "transaction" that throws, as PostgreSQL would: build it on the same repository.

## Test the paths that matter

=== "Happy path"

    ```kotlin
    val id = engine.create(itemSetup, "item-1", SetupRequest(epc = "E-1", storeId = "S-1"))

    val saga = engine.run(id, clock.now + 10.seconds)

    assertEquals(SagaState.COMPLETED, saga.state)
    ```

=== "A refusal compensates"

    ```kotlin
    fakeBooking.refuseNext = true                              // throws StepRejected
    val id = engine.create(itemSetup, "item-1", SetupRequest("E-1", "S-1"))

    val saga = engine.run(id, clock.now + 10.seconds)

    assertEquals(SagaState.FAILED, saga.state)
    assertTrue(fakeStock.entries.isEmpty())                    // the stock move was undone
    ```

=== "A timeout is checked"

    ```kotlin
    fakeStock.timeoutAfterApplying = true                      // applied, then throws IOException
    val id = engine.create(itemSetup, "item-1", SetupRequest("E-1", "S-1"))
    engine.run(id, clock.now + 10.seconds)                     // UNKNOWN: waiting for the settle delay

    clock.advance(2.minutes)
    engine.resumeDue()                                         // the check finds the entry

    assertEquals(SagaState.COMPLETED, engine.find(id)!!.state)
    assertEquals(1, fakeStock.entries.size)                    // not applied twice
    ```

`repository.log` holds the whole history, to assert on events; `repository.storedResults(id)` the stored outputs.

## The library's own tests

```bash
./gradlew test          # unit tests, and PostgreSQL integration tests (Docker, through Testcontainers)
./gradlew stressTest    # hundreds of sagas under random failures, every invariant checked (Docker)
./gradlew benchmark     # the engine's own cost (Docker); report in build/reports/saga-benchmark.md
```

The stress test simulates the external systems and records what they really applied. While it runs, it injects failures at random:

- requests lost or refused;
- calls that outlive their lock;
- checks that fail;
- compensations that fail;
- pods that die mid-call;
- payment events that arrive early, late, twice or never;
- customers who cancel.

At the end it checks that every saga ended, that nothing was applied twice, and that every completed or failed saga matches the external systems exactly. A run that fails prints its seed and the history of the sagas that broke an invariant.
