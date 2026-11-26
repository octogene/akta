# Signals and cancellation

## Steps that wait for an event

A signal step makes no call: it waits for an event that something else delivers, a payment confirmed on Kafka for example. Its value becomes the step's output.

```kotlin
val paid = awaitSignal<PaymentResult>("await-payment", signal = "payment", timeout = 30.minutes) {
    onSignal { if (!it.succeeded) throw StepRejected("payment failed") }   // optional: accept or reject
    check { payments.status(requireResult(payment).id) }                    // optional: decides at the timeout
    logAs { it.paymentId }
}
```

- **`signal`** names the event, not the step. A name is used by one step of a saga, checked at startup.
- **`onSignal`** runs when the saga runs, in a claim transaction of its own, whatever the value came from: a signal that arrived early or woke the wait, or a value the timeout check found. It decides, it does not call anything. Throwing `StepRejected` makes the saga compensate. Anything else it throws is a bug: that transaction rolls back, and the saga stops (`RECORD_FAILED`) with the signal kept. Once the bug is fixed, `retry` uses it. `engine.signal` never runs `onSignal`, nor a hook: a bug in either never fails the listener.
- **While it waits**, the saga is a row with status `WAITING` and `next_attempt_at` set to the timeout. No thread, connection or lock is held.

### Delivering a signal

`engine.signal(sagaId, name, value)` is one short transaction, which runs no user code. It locks the saga's row, finds the step of its definition that waits for `name`, checks the value's type against the one the step declares, then stores the signal and, if its step waits for it, makes the step due at once:

```mermaid
flowchart TD
    S[signal arrives] --> N{saga exists?}
    N -->|no| NS[NoSaga]
    N -->|yes| T{a step waits for this name, with this type?}
    T -->|no| X[IllegalArgumentException, nothing stored]
    T -->|yes| L{saga past that step, finished, or being cancelled?}
    L -->|yes| I1[Ignored, not stored]
    L -->|no| D{a signal of this name already received?}
    D -->|yes| I2[Ignored]
    D -->|no| W{the step is WAITING, or between two timeout checks?}
    W -->|yes| DL[Delivered: stored, the step due at once]
    W -->|no| ST[Stored: used when the step waits]
```

**`Delivered` means the saga is ready to go on, not that it has.** `onSignal` decides when the saga next runs: call `engine.run` after `Delivered` to go on at once, as the [Kafka listener example](../use-cases/reservation-payment.md#the-kafka-listener) does; otherwise the recovery job does, within its period. Each stored signal is consumed in a claim of its own, so that `onSignal` never rolls back an outcome recorded before it, or another signal.

**Only the first signal of a name counts.** It is stored in `saga_signal`, whose primary key is the saga and the signal name, so a redelivered event changes nothing. That is what makes a Kafka listener simple: deliver, then commit the offset.

**A signal can arrive before its step waits.** The provider's event may be faster than the HTTP request that started the saga. It is stored, and used the moment the step would start waiting: the saga goes straight on.

**Routing is by saga id.** The external system must carry our saga id, as a payment's reference for example. A system that only returns its own ids would need a correlation table, which the engine does not have.

### The timeout

At the timeout, the recovery job claims the step (`WAIT_TIMED_OUT` in the history):

| The step has | What happens |
|---|---|
| A check that finds a value | It is stored as the signal, then consumed through `onSignal` like one |
| A check that finds nothing | The saga compensates |
| A check that cannot tell | The saga stops (`CANNOT_TELL`) |
| No check | The saga stops (`NO_SIGNAL`). A human delivers the event by hand, then `retry` waits again |

**A signal received after the timeout still counts**, until the saga moves on. The timeout is a fallback for an event that may be lost, not a deadline:

- A signal that arrives while the check runs is stored, and it is used, whatever the check answers: if the check found a value too, it is the same event, and the signal received first is kept. If the check finds nothing or cannot tell, the lookup ran just before the event, which did happen. If the check fails (it throws), it is not tried again: the stored signal is used.
- A check that failed is retried later, with backoff. A signal that arrives before that retry wakes the step, as a waiting one.

Either way the signal goes through `onSignal`, like any other. **A business deadline belongs there**: to refuse a payment that comes too late, compare its time with the deadline in `onSignal` and throw `StepRejected`.

## Cancellation

`engine.cancel(sagaId, reason, actor)` abandons a saga: everything done so far is compensated.

| The saga | What happens |
|---|---|
| Waits for a signal, or is ready | Compensates at once |
| Has a call in flight, or an outcome still unknown | Compensates as soon as that outcome is recorded, the call included if it was applied |
| Is stuck | Stays stuck: the cancellation is applied when a human retries or marks it done |
| Is already compensating | Nothing more to do. The request is logged, marked `already compensating: nothing changed`, and `failure_reason` keeps the original cause |
| Is finished | Refused: undoing a completed saga is a new saga |

The first cancellation counts: its time and reason stay on the row (`cancel_requested_at`, `cancel_reason`), and nothing overwrites them. Who cancelled is in the history (`CANCEL_REQUESTED`). A cancellation that waited and takes effect later is logged as `CANCELLED`.

A cancellation that is pending also makes the saga **ignore its signals**: whatever the outcome of the call in flight, it will compensate and never wait again.
