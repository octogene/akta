# A reservation that waits for a payment

A customer reserves items online. The items are held, a payment is created, and the reservation is confirmed only when the payment provider says it succeeded. The provider answers later, through an event on Kafka. The customer may cancel meanwhile, and a payment that never completes must release the items.

## The saga

```kotlin
@Serializable data class ReservationRequest(val customerId: String, val items: List<String>, val amount: Long)
@Serializable data class Hold(val id: String)
@Serializable data class Payment(val id: String, val url: String)
@Serializable data class PaymentResult(val paymentId: String, val succeeded: Boolean, val reason: String? = null)

val reservation = saga<ReservationRequest>("RESERVATION") {
    val hold = step<Hold>("hold-items") {
        execute { stock.hold(input.items, until = Clock.System.now() + 45.minutes) }
        compensate { own -> own?.let { stock.release(it.id) } }
        checkByLookup { stock.findHolds(reference = sagaId.toString()) }
    }
    val payment = step<Payment>("create-payment") {
        idempotent(keyRetention = 24.hours)                                   // the provider accepts keys
        execute { payments.create(reference = sagaId.toString(), amount = input.amount, idempotencyKey = idempotencyKey) }
        compensate { own -> own?.let { payments.cancelOrRefund(it.id) } }
        logAs { it.id }
    }
    val paid = awaitSignal<PaymentResult>("await-payment", signal = "payment", timeout = 30.minutes) {
        onSignal { if (!it.succeeded) throw StepRejected("payment failed: ${it.reason}") }
        check { payments.status(requireResult(payment).id) }                  // at the timeout: ask before giving up
        logAs { it.paymentId }
    }
    step("confirm") {
        execute { reservations.confirm(key, requireResult(hold).id, requireResult(paid).paymentId) }
    }
}
```

The payment's `reference` is **our saga id**: the provider sends it back in its events, and that is how an event finds its saga.

## The HTTP request

```kotlin
val saga = engine.run(sagaId, deadline = Clock.System.now() + 5.seconds)
when (saga.stepStatus) {
    StepStatus.WAITING -> {
        val payment = payments.findByReference(sagaId.toString())          // created by the saga, with our id
        respond(HttpStatusCode.Accepted, PaymentPage(url = payment.url))   // 202
    }
    else -> respond(statusOf(saga))
}
```

`run` stops when the saga starts waiting: the handler answers **202** with the payment page. Step outputs are read inside the saga (`requireResult`), not from outside it, so the handler finds the payment by its reference. While it waits, the saga is only a row that is not due. No thread, connection or lock is held, for minutes or hours.

## The Kafka listener

One listener for every saga type: it maps provider events to signal names. It commits its offset once the signal is stored, then runs the saga on in the background, without holding up the next event.

```kotlin
class PaymentListener(
    private val engine: SagaEngine,
    private val scope: CoroutineScope,              // the application's, with a SupervisorJob, cancelled at shutdown
) {
    private val permits = Semaphore(8)              // sagas run at once: well below the connection pool size

    suspend fun onPaymentEvent(event: PaymentEvent) {
        // Not one of our saga ids: throwing would block the partition on it.
        val id = event.reference?.let { runCatching { Uuid.parse(it) }.getOrNull() } ?: return deadLetter(event)

        val result = engine.signal(                 // throws (the database is down)? no commit: it is redelivered
            sagaId = id,
            name = "payment",
            value = PaymentResult(event.paymentId, succeeded = event is PaymentCompleted, reason = event.failureReason),
        )
        commitOffset(event)                         // the signal is stored: a redelivery changes nothing
        if (result is SignalResult.Delivered) runOn(id)
    }

    /** Finishes the saga now, if there is room: otherwise, or if it fails, the recovery job does. */
    private fun runOn(id: Uuid) {
        if (!permits.tryAcquire()) return
        scope.launch {
            try {
                engine.run(id, Clock.System.now() + 10.seconds, skipLocked = true)
            } catch (e: CancellationException) {
                throw e
            } catch (e: Exception) {
                log.warn("Saga $id is left to the recovery job", e)
            } finally {
                permits.release()
            }
        }
    }
}
```

- **The signal is what counts; the run only saves time.** `signal` leaves the saga due at once. If the pod dies before `run`, if it throws, or if there is no room for it, `resumeDue` finishes the saga within its period. Keep the recovery job.
- **Not inline.** `run` checks its deadline between calls: one event could hold the consumer for the deadline plus `callLock` (about 45 seconds here). Over a batch of records, that can exceed `max.poll.interval.ms` (5 minutes by default), and Kafka rebalances. Where events are few, running inline is fine: keep the `try`/`catch`, and lower `max.poll.records`.
- **Only after `Delivered`.** A `Stored` signal is used when the saga reaches its step, and there is nothing to run for the others.
- **At shutdown**, a call in flight ends and is recorded, so the pod's grace period must exceed `callLock`. Kubernetes' default, 30 seconds, is below the default `callLock`, 35 seconds.

`signal` is one short transaction. Its result says what happened:

| Result | When |
|---|---|
| `Delivered` | The step was waiting: accepted (the saga moves on) or rejected by `onSignal` (it compensates) |
| `Stored` | The step does not wait yet: the event was faster than the HTTP request. It is used as soon as the step waits |
| `Ignored` | Already received (a redelivery), too late (the saga moved on or finished), or the saga is being cancelled |
| `NoSaga` | No saga with that id |

A wrong signal name or value type throws, and so does a saga type this pod does not know: that is a bug or a deployment to finish, not an event to skip. `NoSaga` is an event for a saga that was never created here, or was purged: log it.

## What happens in each case

=== "The payment succeeds"

    The event arrives, `onSignal` accepts it, the saga confirms the reservation and completes.

=== "The payment fails"

    `onSignal` throws `StepRejected`: the payment is cancelled and the hold released, last to first. The saga ends `FAILED`.

=== "The event never comes"

    After 30 minutes, the recovery job runs the step's `check`: it asks the provider for the payment's status.

    - Succeeded: the value found goes through `onSignal` like an event, and the saga confirms.
    - No payment: the saga gives up and compensates.
    - The provider cannot tell: the saga stops for a human.

    Without a `check`, a timeout stops the saga for a human, who can deliver the lost event by hand with `engine.signal`, then `retry`.

=== "The event comes twice, or late"

    Only the first signal of a name counts: the second returns `Ignored`. An event after the timeout check decided is also `Ignored`.

=== "The customer cancels"

    ```kotlin
    engine.cancel(sagaId, reason = "customer cancelled", actor = "customer:$customerId")
    ```

    A saga that waits for the payment compensates at once: the payment is cancelled, the hold released. If a call is in flight, the saga compensates as soon as its outcome is recorded, the call included. A finished saga cannot be cancelled: undoing it is a new saga.

## Why `create-payment` is idempotent

The provider accepts idempotency keys, so after a timeout the engine replays the call with the same key instead of checking. The provider answers a replay with its first payment. See [Services with idempotency keys](idempotent-services.md). The hold, from a stock system without keys, keeps its `check`.
