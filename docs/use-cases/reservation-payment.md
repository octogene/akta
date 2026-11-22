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

One listener for every saga type: it maps provider events to signal names.

```kotlin
suspend fun onPaymentEvent(event: PaymentEvent) {
    val result = engine.signal(
        sagaId = Uuid.parse(event.reference),
        name = "payment",
        value = PaymentResult(event.paymentId, succeeded = event is PaymentCompleted, reason = event.failureReason),
    )
    if (result is SignalResult.Delivered) engine.run(result.sagaId, Clock.System.now() + 10.seconds)   // finish now
    // then commit the Kafka offset: the signal is stored, a redelivery changes nothing
}
```

`signal` is one short transaction. Its result says what happened:

| Result | When |
|---|---|
| `Delivered` | The step was waiting: accepted (the saga moves on) or rejected by `onSignal` (it compensates) |
| `Stored` | The step does not wait yet: the event was faster than the HTTP request. It is used as soon as the step waits |
| `Ignored` | Already received (a redelivery), too late (the saga moved on or finished), or the saga is being cancelled |
| `NoSaga` | No saga with that id |

A wrong signal name or value type throws: that is a bug in the listener, not an event to skip.

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
