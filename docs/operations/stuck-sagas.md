# Stuck sagas

A saga stops for a human when the engine cannot decide safely on its own. Each stuck saga carries a **reason**, which says what to do, and the detail in `last_error`.

## Finding them

```kotlin
val page = engine.findStuck(limit = 50)                                        // stuck longest first
val next = engine.findStuck(limit = 50, after = StuckCursor.after(page.last()))
val history = engine.history(page.first().id)                                  // everything that happened, in order
val sagas = engine.findByKey("item:3034257BF7194E4000001A85")                  // from the business key
```

| Call | Returns |
|---|---|
| `findStuck(limit, types, after)` | Stuck sagas, by `stuckAt`, with their `stuckReason` |
| `findByKey(key, limit)` | A business key's sagas, newest first: the unfinished one if any, then those finished and still kept. Where an operator starts from "why is this item stuck?" |
| `history(sagaId)` | A saga's whole history, oldest first |
| `historyByTrace(traceId)` | What one request did, from its trace id |
| `find(id)` | One saga |

The records carry the saga's encoded input, which may hold personal data: decide who may see it on the screen. The history never carries values.

## What to do, by reason

| Reason | What happened | What to do |
|---|---|---|
| `CANNOT_TELL` | A check could not tell whether the step was applied (also at a signal's timeout) | Look in the external system. Applied: `markDone` with the output found. Not applied: `retry` |
| `CHECK_FAILING` | The check kept throwing, or its pod kept dying before recording an outcome | The same, once the lookup works again |
| `TOO_MANY_ATTEMPTS` | `maxAttempts` reached: requests not sent, re-executions, failing compensations | Fix the cause, then `retry`. For a compensation done by hand: `markCompensated` |
| `OUTPUT_UNSTORABLE` | The call was made, but its output cannot be encoded or is over `maxDataBytes` | `markDone` with an output that fits, or `abort(currentStepApplied = true)` |
| `RECORD_FAILED` | The outcome could not be recorded (`onSignal` threw, a constraint or the database failed). What it established is kept; `last_error` names it | Fix the cause, then, by the outcome: a success → `markDone(id, null, actor)` (the output is saved); an undo → `markCompensated` (it is marked done, not run again); a refusal → `abort(currentStepApplied = false)`, which keeps the refusal as the reason (`markDone` is refused); `onSignal` or a hook threw on a signal → fix it, then `retry`, which uses the signal kept (a value the timeout check found is kept as the signal too, so the step does not wait a new timeout); anything else → look in the external system first |
| `NO_SIGNAL` | A signal step's timeout passed, and it has no check | Deliver the event with `signal`, then `retry`; or `abort` |
| `KEY_EXPIRED` | An idempotent step's service may have forgotten its key, for its call or its undo | Look in the service. Applied: `markDone`. Not applied: `retry(id, actor, newKey = true)`. Or `abort`. For an undo (`COMPENSATING`): undone, `markCompensated`; not undone, `retry(id, actor, newKey = true)` |
| `RESUME_FAILING` | The saga itself could not be run, `maxAttempts` times in a row: data that cannot be read, a step removed from its definition, a type this pod does not know. Each try is a `RESUME_FAILED` in its history; `last_error` gives the last one | Fix the cause (usually a deployment), then `retry`: the step resumes as it was, its attempts kept, and a step that may have been called is checked first. A signal step that was waiting keeps its timeout, or checks at once if it has passed. Or `markDone`, or `abort` |
| `HOOK_FAILED` | The saga is `FINISHING`: every step is done (or undone), and its `onCompleted` (or `onFailed`) threw `maxAttempts` times in a row | Fix the hook, then `retry`: it runs the hook alone, never a step. A stuck `onCompleted` can be aborted instead, which undoes every step |

## The actions

| Call | Use it when |
|---|---|
| `retry(id, actor)` | The cause is fixed. Forward, it **executes the step again without checking**: after `CANNOT_TELL`, first confirm by hand the step was not applied. An idempotent step is replayed with its key instead, which is safe. A signal step waits again, and uses a signal received meanwhile. |
| `retry(id, actor, newKey = true)` | An idempotent step was not applied, and its key expired or holds a remembered failure: send a fresh key. |
| `markDone(id, output, actor)` | The step was applied, as you confirmed by hand. `output` is its output as JSON, checked against the step's type; `null` keeps the output already saved. On a signal step, `output` is the signal's value, delivered by hand: its `onSignal` accepts or refuses it, as any signal; `null` uses the signal the saga keeps. |
| `abort(id, actor, currentStepApplied, output)` | Give up: the completed steps are undone. With `currentStepApplied = true`, the stuck step is undone too, and its compensation receives `output`. Refused while a call is in flight or an outcome is unknown; `currentStepApplied = true` is refused too for a step whose refusal is kept (`RECORD_FAILED`): nothing was applied. |
| `markCompensated(id, actor)` | A compensation is stuck and you undid the step by hand, or there was nothing to undo: the saga goes on undoing the earlier steps. |
| `cancel(id, reason, actor)` | Abandon a saga that is not stuck, at any time before it finishes. |

Every action is logged with its author. Each commits its change, then runs the saga on for up to `runFor` (10 s by default), so the screen can show where it went:

- **The wait can be longer than `runFor`**: a call started just before it may take up to `callLock` more. Pass `runFor = Duration.ZERO` to return at once, as an HTTP handler that must answer quickly would; the recovery job then takes the saga on.
- **Nothing runs when the saga cannot move**: finished, stuck, a call in flight, or not due yet.
- **The action holds even if running the saga on fails** (the database, data that cannot be read, a saga type this pod does not know): the call returns the saga as the action left it, postponed and logged as `RESUME_FAILED`, as the recovery job would.

!!! tip "Build the screen around the reason"
    The reason maps to the action: an admin screen can suggest it, show the lookup to do in the external system, and ask for the output only when `markDone` needs one.

    `engine.steps(id)` says where each step stands (done, current, pending, undone, never applied) with what its `logAs` says of its output, which is what an `abort` would undo and what to look up. `engine.actions(saga)` says which actions the saga takes now, and why it refuses the others, from the same checks the actions make: offer only those. The [admin API](admin-api.md) includes them with every saga.
