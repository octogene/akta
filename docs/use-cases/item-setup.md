# Setting up an item in two systems

A store employee scans an item to lend it to customers for a trial. Two systems must change, and neither accepts an idempotency key:

1. the **stock system** moves the item into the test stock;
2. the **booking service** creates a resource customers can book.

If the booking service refuses, the stock move must be undone. If the stock call times out, nobody knows whether the item moved: moving it again could create a second entry. The employee expects an answer at the counter.

## The saga

```kotlin
@Serializable data class SetupRequest(val epc: String, val storeId: String)
@Serializable data class StockEntry(val id: String, val location: String)

fun itemSetupSaga(stock: StockClient, booking: BookingClient, items: Items) =
    saga<SetupRequest>("ITEM_SETUP") {
        val entry = step<StockEntry>("stock") {
            execute { stock.moveToTestStock(input.epc, input.storeId) }
            compensate { own -> own?.let { stock.revertTestStockEntry(it.id) } }
            checkByLookup { stock.findTestStockEntries(input.epc) }
            logAs { it.id }
        }
        step<String>("booking") {
            execute { booking.createResource(input.epc, stockId = requireResult(entry).id) }
            checkByLookup { booking.findResources(input.epc) }
        }
        onCompleted { items.markAvailable(key, location = requireResult(entry).location) }
        onFailed { items.markSetupFailed(key) }
    }
```

The item's own status (`SETTING_UP`, then `AVAILABLE` or `SETUP_FAILED`) hides the intermediate state from the rest of the application. The hooks change it in the same transaction as the saga's end.

## What happens in each case

=== "Both calls work"

    The request runs the saga inline: two calls, about 4 short transactions in all. The employee gets a **201** within a second or two, and the item becomes available.

=== "The booking service refuses"

    `createResource` throws `StepRejected`. The engine compensates the stock step (`revertTestStockEntry`), the saga ends `FAILED`, `onFailed` marks the item, and the employee gets a **422** with the reason.

=== "The stock call times out"

    The exception is neither `StepRejected` nor `StepNotSent`, so the outcome is **unknown**. The employee gets a **202**: the setup continues in the background.

    Two minutes later (the settle delay, long enough for the stock system to finish whatever it was doing), the recovery job runs the check: `findTestStockEntries(epc)`.

    - One entry: the move was applied; it becomes the step's output, and the saga goes on to the booking.
    - None: it was not applied; the move runs again.
    - Several: the engine cannot tell which is ours, and the saga stops for a human.

=== "The pod dies during the booking call"

    The step stays `IN_PROGRESS` until its lock expires (35 s). Another pod's recovery job then treats the outcome as unknown, and checks it two minutes later with `findResources(epc)`.

=== "The stock system is down"

    `moveToTestStock` throws `StepNotSent` (connection refused). Nothing left, so the engine simply retries after 1, 2, 4… minutes. After 10 attempts, the saga stops for a human, who retries once the system is back.

## What it needs from the external systems

- **A lookup by a business identifier** (here the EPC) for every step: that is what turns an unknown outcome into a decision. Without it, each timeout means a human looks in the system.
- **Ideally, a refusal of duplicates** for the same EPC. It turns the rare late duplicate into a refusal.
- **A known maximum processing time**, to set the settle delay above it.

## Taking the item out again

Removing the item from the test stock (a sale, scrapping) is a second saga type, `ITEM_EXIT`, with the same business key. The engine allows one unfinished saga per key: an exit requested while the setup still runs is refused with `DuplicateSagaException`, which the handler turns into a 409.
