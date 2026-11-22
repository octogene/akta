# saga-engine

**A small saga engine for Kotlin services that call external systems.** A saga runs an ordered list of steps, each one a call to another system. If a step is refused, the engine undoes the steps already done. Its state lives in PostgreSQL, so a saga survives timeouts, restarts and pods that die mid-call.

It is built for one hard case: systems that **do not accept idempotency keys**. After a timeout, nobody knows whether the call was applied, and calling again may create a duplicate. So the engine never replays blindly: it **checks** first, and stops for a human when it cannot tell.

```kotlin
val itemSetup = saga<SetupRequest>("ITEM_SETUP") {
    val stock = step<StockEntry>("stock") {
        execute { stockClient.moveToTestStock(input.epc, input.storeId) }
        compensate { own -> own?.let { stockClient.revert(it.id) } }
        checkByLookup { stockClient.findEntries(input.epc) }     // "was I applied?"
    }
    step<String>("booking") {
        execute { bookingClient.createResource(input.epc, stockId = requireResult(stock).id) }
        checkByLookup { bookingClient.findResources(input.epc) }
    }
}
```

<div class="grid cards" markdown>

-   **Getting started**

    ---

    Add the library and its four tables, write a first saga, start it from an HTTP handler, and test it without a database.

    [:octicons-arrow-right-24: Getting started](getting-started/index.md)

-   **Use cases**

    ---

    Three real flows: an item set up in two systems, a reservation that waits for a payment, and services with idempotency keys. And when not to use it.

    [:octicons-arrow-right-24: Use cases](use-cases/index.md)

-   **How it works**

    ---

    The lifecycle of a saga, the claim / call / record cycle, unknown outcomes, compensation, signals, recovery, and what is guaranteed.

    [:octicons-arrow-right-24: How it works](how-it-works/index.md)

-   **Operations**

    ---

    Stuck sagas and what to do for each reason, metrics, alerts, retention, and configuration.

    [:octicons-arrow-right-24: Operations](operations/index.md)

</div>

## What it gives you

| | |
|---|---|
| **Durable** | Each saga is one row in PostgreSQL. A crash, a restart or a deployment never loses where a saga stands. |
| **No call inside a transaction** | Each step is claimed in a short transaction, called, then recorded in another. No connection or lock is held during a call. |
| **Unknown outcomes handled** | A timeout is checked in the external system before anything is replayed. A step with an idempotency key is replayed with it instead. |
| **Compensation** | A refused step undoes the completed ones, last to first. |
| **A synchronous answer** | An HTTP request runs its saga inline within a time budget (201, 422 or 202); a recovery job finishes the rest. |
| **Waiting for events** | A step can wait for a signal (a payment confirmed on Kafka) without holding a thread. |
| **Typed data** | A saga's input and each step's output are Kotlin types, stored as JSON through kotlinx.serialization. |
| **A full history** | Every call, check, compensation and manual action is in `saga_log`, with its OpenTelemetry trace id. |
| **Operable** | Stuck sagas carry a reason that says what to do; reads, metrics and manual actions are built in. |

**Requirements:** Kotlin 2.x on JVM 17+, Exposed 1.x, PostgreSQL 12+, kotlinx.serialization.
