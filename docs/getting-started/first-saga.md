# Your first saga

A saga is declared once, with a small DSL, and registered in the engine. This page builds one for a common case: putting an item into the test stock of a store, which takes two calls to two systems that must both succeed or both be undone.

1. Move the item into the test stock, in the **stock system**.
2. Create a bookable resource for it, in the **booking service**.

## The data

A saga can carry an **input**, what it was asked to do, fixed at creation. Each step can return an **output**, what its call returned, read by the steps after it. Both are plain `@Serializable` classes:

```kotlin
@Serializable data class SetupRequest(val epc: String, val storeId: String)   // the input
@Serializable data class StockEntry(val id: String, val location: String)     // the stock step's output
```

Keep them to parameters and identifiers (16 KB at most each): anything large belongs in a business table, referenced by id.

## The saga

```kotlin
fun itemSetupSaga(stockClient: StockClient, bookingClient: BookingClient, items: Items) =
    saga<SetupRequest>("ITEM_SETUP") {                                    // (1)!

        val stock = step<StockEntry>("stock") {                           // (2)!
            execute { stockClient.moveToTestStock(input.epc, input.storeId) }
            compensate { own -> own?.let { stockClient.revertTestStockEntry(it.id) } }   // (3)!
            checkByLookup { stockClient.findTestStockEntries(input.epc) } // (4)!
            logAs { it.id }                                               // (5)!
        }

        step<String>("booking") {
            execute { bookingClient.createResource(input.epc, stockId = requireResult(stock).id) }   // (6)!
            checkByLookup { bookingClient.findResources(input.epc) }
            logAs { it }                                                  // a resource id: safe to log
        }

        onCompleted { items.markAvailable(key, location = requireResult(stock).location) }   // (7)!
        onFailed { items.markSetupFailed(key) }
    }
```

1. The saga **type**, stored with each saga. `saga<SetupRequest>` gives it an input; `saga("X") { … }` declares one without.
2. Steps run in declaration order. `step<StockEntry>` says what the call returns; it gives back a `StepRef<StockEntry>`, used to read that output later.
3. **Undo.** It receives the step's own output, and must be safe to run twice: "revert if it is still there".
4. **Was I applied?** Asked after a timeout. None found: not applied, the step runs again. One found: applied, and it becomes the output. Several: the engine cannot tell, and stops for a human.
5. What the history shows about the output: an identifier, never personal data.
6. `requireResult(stock)` is the stock step's output, typed as a `StockEntry`.
7. Business effects at the end, **in the same transaction** as the saga's last change. No external calls here.

Inside every block, `this` is the saga's context: `input`, `key` (the business key), `sagaId`, and the outputs of earlier steps.

## How a step reports what happened

A step's `execute` returns its output when the call worked. When it did not, **the exception it throws tells the engine what to do**:

| Your code | Meaning | What the engine does |
|---|---|---|
| returns normally | Applied | Stores the output, goes on to the next step |
| throws `StepRejected` | Refused, **nothing applied** (`400`, `422`, a business refusal) | Undoes the completed steps |
| throws `StepNotSent` | The request **never left** (connection refused, DNS, connect timeout, `429`) | Retries later, with backoff |
| throws anything else | **Unknown**: it may have been applied (read timeout, reset, `5xx`) | Checks, after a settle delay |

```kotlin
suspend fun moveToTestStock(epc: String, storeId: String): StockEntry {
    val response = try {
        http.post("/test-stock") { setBody(MoveRequest(epc, storeId)) }
    } catch (e: ConnectException) {
        throw StepNotSent("stock system unreachable", e)      // never left: safe to retry
    }                                                         // a read timeout propagates: unknown
    return when {
        response.status.isSuccess() -> response.body()
        response.status == HttpStatusCode.TooManyRequests -> throw StepNotSent("rate limited")
        response.status.value in 400..499 -> throw StepRejected("refused: ${response.status}")
        else -> error("stock system answered ${response.status}")   // 5xx: it may have been applied
    }
}
```

!!! danger "Classify errors carefully"
    `StepNotSent` for a request that may have reached the server skips the check, and can create a duplicate. When in doubt, let the exception through: **unknown is the safe default**.

## The DSL in one table

| DSL | Meaning |
|---|---|
| `saga<I>(type) { … }` | A saga type whose input is an `I`. `saga(type) { … }` has none. |
| `step<O>(name) { … }` | The next step, whose call returns an `O`. `step(name) { … }` returns nothing. |
| `execute { … }` | Required. The call. |
| `compensate { own -> … }` | Optional. Undo, given the step's own output. Safe to repeat. |
| `checkByLookup { … }` | Optional. The matches found in the external system: none, one or several. |
| `check { … }` | Optional. The same, in full: return `CheckResult.Done(output)`, `NotDone` or `CannotTell`. |
| `logAs { … }` | Optional. What the history shows about the output. Default: a `String` output itself, nothing otherwise. |
| `idempotent(keyRetention)` | Optional. The service accepts an idempotency key: see [Services with idempotency keys](../use-cases/idempotent-services.md). |
| `awaitSignal<S>(name, signal, timeout) { … }` | A step that waits for an event instead of calling: see [A reservation that waits for a payment](../use-cases/reservation-payment.md). |
| `onCompleted { … }` / `onFailed { … }` | Business effects at the end, in the saga's last transaction. |

The DSL builds plain `SagaDefinition` and `SagaStep` objects. A step can also be a class implementing `SagaStep<I, O>`, for example to share it between sagas or to inject its clients.

Next: [run it](running.md).
