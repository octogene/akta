# Timing and budgets

Every duration the engine uses, what it bounds, how to choose it, and what to watch when you change it. The [configuration page](configuration.md) lists the settings. This page explains how they fit together.

## Two kinds of time

The durations fall into two families, which are easy to mix up:

- **Request time**, in seconds: how long one caller waits for one piece of work. This covers a call to the external system (`callLock`, your clients' timeouts) and one pass of `run` (its `deadline`, `budgetPerSaga`, `runFor`).
- **Saga time**, in minutes or hours: how long a saga takes to recover from trouble. This covers waiting for an unknown outcome to settle (`settleDelay`), backing off between attempts (`maxBackoff`, `maxAttempts`), how long a key or a wait stays valid (`keyRetention`, a signal's `timeout`), and how often the recovery job runs.

A request never waits on saga time. When `run` reaches a delay (a backoff, a settle delay, a wait), it returns, and the recovery job picks the saga up when it is due.

## One call, second by second

Every call (an execute, a check or an undo) is claimed with a lock, then cut before the lock expires:

```
claim                                                  cut               lock expires
  │  call runs, no transaction open                     │  record outcome  │
  ├─────────────────────────────────────────────────────┼──────────────────┤
  0 s                                               31.5 s               35 s   (defaults)
  └────────────── callLock − recordMargin ──────────────┘└─ recordMargin ──┘
                                                          callLock / 10, at most 5 s
```

| Value | Default | Set by | What it is |
|---|---|---|---|
| `callLock` | 35 s | `SagaConfig` | How long a claimed call owns the step. `locked_until = claim time + callLock` |
| `recordMargin` | 3.5 s | derived: `callLock / 10`, at most 5 s | Time kept after the call to record its outcome while the lock is still ours |
| the call's budget | 31.5 s | derived: `locked_until − now − recordMargin` | The call is cancelled when it runs out, and counts as an **unknown outcome** |
| your clients' timeouts | — | your code | Must end the call **before** the engine cuts it |

### How to choose `callLock`

Start from your slowest step. Take its client's timeout, add any retries the client makes internally, and add a margin. Then make sure:

```
client timeout (× its internal retries)  <  callLock − recordMargin
```

With the defaults, that means keeping clients' timeouts under about 30 s.

- **If a client times out before the engine cuts the call**, the step throws, and you decide what that means: `StepNotSent` (definitely not sent), `StepRejected` (refused), or any other exception (unknown). This is the case to aim for.
- **If the engine cuts the call first**, the outcome is always unknown. It leads to a check after `settleDelay`, or a replay. It's safe, but slower, and it uses a check.
- **A blocking call can't be cut.** A JDBC query, `HttpClient.send` or `Thread.sleep` runs to its end, and may outlive its lock. Use suspending clients, or `runInterruptible`.

### What `callLock` also decides

`callLock` isn't only about calls. It appears in four other places:

| Where | Effect of a larger `callLock` |
|---|---|
| **A pod that dies mid-call** | The step stays `IN_PROGRESS` until the lock expires, so recovery starts that much later (plus `settleDelay` for a check) |
| **The longest an HTTP request waits** | `deadline + callLock`: the deadline is checked between calls, never during one |
| **Shutdown** | A call in flight always ends and is recorded, so the pod's grace period must exceed `callLock`. Kubernetes' default (30 s) is **below** the default `callLock` (35 s) |
| **Idempotency keys** | `keyRetention` must exceed it (checked at startup), and a call is only sent if it can end before the key expires |

So a large `callLock` makes slow steps safe, at the cost of slower crash recovery and longer worst-case requests. It applies to the whole engine: if one step is much slower than the others, it sets the value for all of them.

## One pass of `run`: deadlines and budgets

`run(id, deadline)` goes from step to step until the saga ends, blocks, or the deadline passes. **The deadline is checked before each claim**, so:

```
worst-case wait  =  deadline  +  callLock          (one call started just before the deadline)
```

| Caller | Parameter | Default | How to choose it |
|---|---|---|---|
| An HTTP handler | `run(id, deadline)` | none | The time you're happy to make a user wait for a `201`/`422` instead of a `202`. Your proxy's and client's timeouts must exceed `deadline + callLock` |
| The recovery job | `resumeDue(budgetPerSaga)` | 20 s | Enough for a few steps. A saga that needs more simply continues at the next batch |
| A manual action | `retry(…, runFor)`, etc. | 10 s | Enough to show the operator the effect. `Duration.ZERO` leaves it to the recovery job |
| A Kafka listener | `run(id, deadline)` in the background | your choice | Run in the background, not inline (see below) |

!!! example "With the defaults"
    A handler calling `run(id, now + 8.seconds)` answers within 8 s in the usual case. In the worst case it answers within 43 s, when a call starts at 7.9 s and is cut at 31.5 s. A load balancer with a 30 s timeout would cut that request. Either raise its timeout, or lower `callLock` if your steps are fast.

**Nothing is lost when a deadline passes.** The saga is left due (`READY`, `CHECKING`, `REPLAYING` or `WAITING`, with its `next_attempt_at`), and whoever runs it next continues from there.

**A cancelled caller doesn't interrupt a call.** If the HTTP client disconnects, the call in flight still ends and is recorded (at most `callLock`), and only then does `run` stop.

### Kafka listeners

If a listener runs sagas inline, a single event can hold the consumer for `deadline + callLock`. Over a batch of records, that can exceed `max.poll.interval.ms` (5 min by default) and trigger a rebalance. Run in the background with a bounded number of sagas at once, as in the [listener example](../use-cases/reservation-payment.md#the-kafka-listener), or lower `max.poll.records`.

## The recovery job

| Value | Default | Set by | What it bounds |
|---|---|---|---|
| period | ~30 s | your scheduler | How late a due saga is picked up: up to one period |
| `limit` | 50 | `resumeDue` | Sagas per batch |
| `concurrency` | 8 | `resumeDue` | Sagas run at once. Keep it well below the connection pool size |
| `budgetPerSaga` | 20 s | `resumeDue` | One saga's pass, as above |
| `resumeFailureDelay` | 1 min | `SagaConfig` | How long a saga whose run threw waits before the next try. After `maxAttempts` in a row: `RESUME_FAILING` |

A batch takes at most about:

```
ceil(limit / concurrency) × (budgetPerSaga + callLock)
```

With the defaults that's 7 × 55 s, about 6.5 minutes, though usually far less, since most sagas need one call or none. If batches regularly take longer than the period, raise `concurrency` (within the pool) or run the job on more pods. Two batches overlapping is safe, because claims use `SKIP LOCKED`.

**The period adds to every delay.** A saga due in 2 minutes is run between 2 minutes and 2 minutes plus one period from now. The `stats()` gauge `now − oldestDueAt` shows how far behind the job is.

## When the outcome is unknown: settle, check, back off

```
unknown outcome ──settleDelay──▶ check ──┬─ done     → next step
                                         ├─ not done → execute again, at once
                                         ├─ can't tell → STUCK
                                         └─ check failed ──max(checkRetryDelay, backoff)──▶ check again
```

| Value | Default | What it bounds |
|---|---|---|
| `settleDelay` | 2 min | The wait before checking an unknown outcome |
| `checkRetryDelay` | 1 min | The shortest wait before checking again, after a failed check |
| backoff | 1, 2, 4… min | The wait before retrying a call that failed (`StepNotSent`, an idempotent replay, an undo): `2^(attempts − 1)` minutes, capped at `maxBackoff` |
| `maxBackoff` | 1 h | The longest wait between two attempts |
| `maxAttempts` | 10 | Calls of one step, failed checks in a row, or failed runs in a row, before `STUCK` |

### `settleDelay`: the one value that's unsafe when too small

A request that timed out on our side may still be **in progress** on the other side: queued, being retried by a gateway, or processed asynchronously. If the check runs before it lands, it finds nothing, the step runs again, and the first request lands too. That's a duplicate.

Set `settleDelay` above the **longest time the external system may still apply a request after you gave up on it**, from its documentation or your measurements, not its usual latency. Every other value on this page trades speed against effort. This one trades speed against correctness.

When it's too large, unknown outcomes take longer to resolve, and the caller gets a `202` in the meantime. Idempotent steps don't wait for it: they're replayed with their key after backoff (or at once, after a lock expiry).

### `maxAttempts` × backoff: how long a failing step keeps trying

The attempts of a step that keeps failing are spread over:

```
sum of backoff(1) … backoff(maxAttempts − 1)
```

With the defaults: 1 + 2 + 4 + 8 + 16 + 32 + 60 + 60 + 60 = 243 minutes, **about 4 hours**, before the saga stops for a human.

| To… | Change |
|---|---|
| reach a human sooner | lower `maxAttempts` or `maxBackoff` |
| ride out a long outage of a dependency | raise `maxBackoff` (each wait) or `maxAttempts` (more waits) |

Failed checks have their own count, separate from attempts, so a lookup outage doesn't use up executions. They wait `max(checkRetryDelay, backoff)` between tries.

## Windows that expire

| Value | Set by | What happens at the end |
|---|---|---|
| `keyRetention` | `idempotent(keyRetention)`, per step | Once its service may have forgotten the key, the engine won't send it again: `KEY_EXPIRED` |
| `timeout` | `awaitSignal(…, timeout)`, per signal step | The step's `check` runs at once (no settle delay); without a check: `NO_SIGNAL` |

### `keyRetention` has to cover the whole retry window

A call is only claimed if it can end while the service still remembers the key:

```
now + callLock  <  key first used + keyRetention
```

Otherwise the saga stops with `KEY_EXPIRED`. The key's window starts when the key is first sent (a first request that never left, `StepNotSent`, doesn't count) and doesn't move after that. So if the step can keep retrying for about 4 hours (see above), `keyRetention` must exceed that, plus the settle and recovery delays. Otherwise a long outage ends with `KEY_EXPIRED` instead of a replay. Use the **documented minimum** of the service, not its usual behaviour, and lower `maxAttempts` / `maxBackoff` if that minimum is short. At startup the engine only checks that `keyRetention > callLock`.

### A signal's `timeout`

This is a business decision: how long to wait for the payment, the approval or the event. At the timeout, the step's `check` asks the external system straight away. A signal that arrives during the check still wins.

## Hooks

`onCompleted` and `onFailed` are retried after backoff (1, 2, 4… min), and the saga stops after `maxAttempts` failures in a row (`HOOK_FAILED`). Nothing the steps did is lost in the meantime.

## What to know before changing a value

- **A change applies to the next decision, not to sagas already scheduled.** Every delay is turned into a time when it's decided: `next_attempt_at` for a retry, a check or a wait, and `locked_until` for a call. After a deploy, sagas already waiting keep the time they were given. A shorter `settleDelay` speeds up only the unknown outcomes that happen after the deploy.
- **`keyRetention` is the exception.** The key's first use is stored, and the window is computed from the current definition at each claim. Lowering it can stop sagas that are already replaying (`KEY_EXPIRED`).
- **Pods with different values during a rolling deploy are safe.** Each pod respects the `locked_until` written by the one that claimed. Only its own record margin and future decisions use its own values.
- **Clocks must agree.** Locks are compared with times written by other pods, so keep NTP running. A drift of a few seconds is harmless next to `callLock`. A drift close to it isn't: a pod could take a lock it believes expired.
- **Most of these durations add up.** A crash during a call is followed by `callLock`, then `settleDelay`, then up to one recovery period before the check. With the defaults that's about 3 minutes before the step is resolved. Add them up for the scenarios you care about before you promise anything to your users.

## A quick checklist

- [ ] Every client's timeout, retries included, is below `callLock − recordMargin` (about 30 s with the defaults).
- [ ] Calls are suspending, or wrapped in `runInterruptible`.
- [ ] Proxy and client timeouts in front of `run` exceed `deadline + callLock`.
- [ ] The pod's shutdown grace period exceeds `callLock`.
- [ ] `settleDelay` exceeds the longest time each external system may still apply a request.
- [ ] Every `keyRetention` is the service's documented minimum, and covers the retry window (`maxAttempts`, `maxBackoff`).
- [ ] The recovery job's period is short enough for your users, and its batches finish within it.
- [ ] The pods' clocks are synchronised (NTP).
