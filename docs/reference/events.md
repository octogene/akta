# Events

Every change to a saga writes one entry to `saga_log`: its time, the step, the event, a detail, the author of a manual action, and the trace id. Event names are stored and read by queries and dashboards: they never change.

## Call outcomes

Logged as `<action>:<outcome>`, on the step that ran.

| Event | Meaning |
|---|---|
| `EXECUTE:Done` | The call worked. The detail gives the output's `ref` (from `logAs`) |
| `EXECUTE:Rejected` | Refused, nothing applied: the saga compensates |
| `EXECUTE:NotSent` | The request never left: retried later |
| `EXECUTE:Unknown` | It may have been applied: checked later, or replayed with the key |
| `EXECUTE:OutputUnstorable`, `CHECK:OutputUnstorable` | The call was made, but its output cannot be stored: stuck |
| `CHECK:Done` | The check found the effect: it becomes the output |
| `CHECK:NotDone` | Not applied: executed again |
| `CHECK:CannotTell` | The check cannot decide: stuck |
| `CHECK:CheckFailed` | The lookup itself threw: checked again later |
| `CHECK:Rejected` | A signal step's timeout check found nothing, or found a value `onSignal` rejected: the saga compensates |
| `COMPENSATE:Compensated` | The step was undone |
| `COMPENSATE:CompensationFailed` | The undo failed: retried later |

## Engine events

| Event | Meaning |
|---|---|
| `CREATED` | The saga was created |
| `LOCK_EXPIRED` | A call's lock expired: the pod died, or the call never recorded its outcome |
| `LATE_RESULT_IGNORED` | A call ended after its lock expired and another pod took over: its outcome is dropped |
| `RECORD_FAILED` | The outcome could not be recorded: stuck, with a successful output saved |
| `RESUME_FAILED` | `resumeDue`, or the run after a manual action, could not run the saga (a database error, unreadable data, an unknown type): postponed |
| `KEY_EXPIRED` | An idempotent step's key may be forgotten: stuck, no call made |
| `NOT_CALLED` | A long pause after a claim (GC, a stalled thread) left no time to call before its lock expires: the step is released as it was, due at once, no attempt used |

## Signals and cancellation

| Event | Meaning |
|---|---|
| `WAIT_STARTED` | A signal step started waiting. The detail gives the signal and the deadline |
| `SIGNAL_RECEIVED` | A signal arrived. The detail says `delivered`, `stored`, `consumed (stored earlier)`, or `ignored` with why |
| `WAIT_TIMED_OUT` | The timeout passed without the signal: checked, or stuck |
| `CANCEL_REQUESTED` | `cancel` was called. The detail gives the reason; the actor, who |
| `CANCELLED` | A cancellation that waited took effect at the next claim (after a manual retry, for example) |

## Manual actions

`MANUAL_RETRY`, `MANUAL_DONE`, `MANUAL_ABORT`, `MANUAL_COMPENSATED`: each with its author in `actor`. A retry with a fresh key says so in its detail (`newKey: generation 1`).

## Reading the history

```kotlin
engine.history(sagaId).forEach { println("${it.at} ${it.stepName} ${it.event} ${it.detail.orEmpty()}") }
```

```sql
-- Everything that happened to one business key
SELECT l.at, s.type, l.step_name, l.event, l.detail, l.actor, l.trace_id
FROM saga_log l JOIN saga s ON s.id = l.saga_id
WHERE s.business_key = :key ORDER BY l.at, l.id;
```

No entry ever contains an input, an output or a signal value: only identifiers given by `logAs`, and error messages.
