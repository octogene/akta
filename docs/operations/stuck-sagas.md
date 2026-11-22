# Stuck sagas

A saga stops for a human when the engine cannot decide safely on its own. Each stuck saga carries a **reason**, which says what to do, and the detail in `last_error`.

## Finding them

```kotlin
val page = engine.findStuck(limit = 50)                                        // stuck longest first
val next = engine.findStuck(limit = 50, after = StuckCursor.after(page.last()))
val history = engine.history(page.first().id)                                  // everything that happened, in order
```

| Call | Returns |
|---|---|
| `findStuck(limit, types, after)` | Stuck sagas, by `stuckAt`, with their `stuckReason` |
| `history(sagaId)` | A saga's whole history, oldest first |
| `historyByTrace(traceId)` | What one request did, from its trace id |
| `find(id)` | One saga |

The records carry the saga's encoded input, which may hold personal data: decide who may see it on the screen. The history never carries values.

## What to do, by reason

| Reason | What happened | What to do |
|---|---|---|
| `CANNOT_TELL` | A check could not tell whether the step was applied (also at a signal's timeout) | Look in the external system. Applied: `markDone` with the output found. Not applied: `retry` |
| `CHECK_FAILING` | The check kept throwing | The same, once the lookup works again |
| `TOO_MANY_ATTEMPTS` | `maxAttempts` reached: requests not sent, re-executions, failing compensations | Fix the cause, then `retry`. For a compensation done by hand: `markCompensated` |
| `OUTPUT_UNSTORABLE` | The call was made, but its output cannot be encoded or is over `maxDataBytes` | `markDone` with an output that fits, or `abort(currentStepApplied = true)` |
| `RECORD_FAILED` | The outcome could not be recorded (a hook threw, a constraint failed) | Fix the cause, then `markDone(id, null, actor)`: a successful call's output is already saved |
| `NO_SIGNAL` | A signal step's timeout passed, and it has no check | Deliver the event with `signal`, then `retry`; or `abort` |
| `KEY_EXPIRED` | An idempotent step's service may have forgotten its key | Look in the service. Applied: `markDone`. Not applied: `retry(id, actor, newKey = true)`. Or `abort` |

## The actions

| Call | Use it when |
|---|---|
| `retry(id, actor)` | The cause is fixed. Forward, it **executes the step again without checking**: after `CANNOT_TELL`, first confirm by hand the step was not applied. An idempotent step is replayed with its key instead, which is safe. A signal step waits again, and uses a signal received meanwhile. |
| `retry(id, actor, newKey = true)` | An idempotent step was not applied, and its key expired or holds a remembered failure: send a fresh key. |
| `markDone(id, output, actor)` | The step was applied, as you confirmed by hand. `output` is its output as JSON, checked against the step's type; `null` keeps the output already saved. |
| `abort(id, actor, currentStepApplied, output)` | Give up: the completed steps are undone. With `currentStepApplied = true`, the stuck step is undone too, and its compensation receives `output`. Refused while a call is in flight or an outcome is unknown. |
| `markCompensated(id, actor)` | A compensation is stuck and you undid the step by hand, or there was nothing to undo: the saga goes on undoing the earlier steps. |
| `cancel(id, reason, actor)` | Abandon a saga that is not stuck, at any time before it finishes. |

Every action is logged with its author. Each runs the saga on right after, for a few seconds, so the screen can show where it went.

!!! tip "Build the screen around the reason"
    The reason maps to the action: an admin screen can suggest it, show the lookup to do in the external system, and ask for the output only when `markDone` needs one.
