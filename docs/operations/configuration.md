# Configuration

How the durations below fit together, and how to choose them: [Timing and budgets](timing.md).

```kotlin
SagaEngine(..., config = SagaConfig(
    callLock = 35.seconds,
    settleDelay = 2.minutes,
    checkRetryDelay = 1.minutes,
    maxAttempts = 10,
    maxBackoff = 1.hours,
    resumeFailureDelay = 1.minutes,
    maxDataBytes = 16 * 1024,
))
```

| Setting | Default | What it controls | How to set it |
|---|---|---|---|
| `callLock` | 35 s | How long a call may run before it counts as interrupted. The engine cuts calls just before it | Above your HTTP clients' timeouts, plus a margin |
| `settleDelay` | 2 min | The wait before checking an unknown outcome | Above the longest time an external system may spend on a request |
| `checkRetryDelay` | 1 min | The shortest wait before checking again, after a check that failed | |
| `maxAttempts` | 10 | Calls of a step (executions, replays or compensations), failed checks in a row, and failures to run the saga at all in a row (`RESUME_FAILED`), before it is stuck | |
| `maxBackoff` | 1 h | The longest wait between attempts. Backoff is 1, 2, 4… minutes, up to this | |
| `resumeFailureDelay` | 1 min | How long a saga whose run threw in `resumeDue` waits before the next try | |
| `maxDataBytes` | 16 KB | The largest encoded input, output or signal value | Parameters and identifiers only |

`SagaConfig` refuses values that make no sense: a `callLock` under 1 second (a tenth of it, at most 5 s, is kept after each call to record its outcome), a negative delay, or a `maxBackoff` that is not positive.

Per step, not per engine:

| Setting | Where | What it controls |
|---|---|---|
| `idempotent(keyRetention)` | A step's DSL | How long its service remembers a key; must exceed `callLock` |
| `awaitSignal(…, timeout)` | A signal step | How long it waits for its event |

And per call:

| Call | Parameter | Default |
|---|---|---|
| `run(id, deadline)` | `deadline`: the time budget, checked between calls | required |
| `resumeDue(limit, budgetPerSaga, concurrency)` | sagas per batch, budget each, sagas at once | 50, 20 s, 8 |
| `purgeFinished(olderThan, batchSize)` | retention, sagas per transaction | 30 days, 1,000 |

!!! note "Retries in numbers"
    With the defaults, the attempts of a step that keeps failing are spread over about 4 hours (1 + 2 + 4 … minutes, capped at an hour). A step with an idempotency key whose service remembers keys for less than that should use a smaller `maxAttempts` or `maxBackoff`, or it will stop with `KEY_EXPIRED`.
