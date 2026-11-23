# Operations

Running sagas in production comes down to four jobs:

| Job | How | Page |
|---|---|---|
| Keep sagas moving | Call `resumeDue` every 30 s from the job runner | [Recovery and concurrency](../how-it-works/recovery.md) |
| Resolve stuck sagas | An admin screen on `findStuck`, `history` and the manual actions, or the HTTP API of `akta-ktor` | [Stuck sagas](stuck-sagas.md), [Admin API](admin-api.md) |
| Know when something is wrong | Gauges from `stats()`, counters from a listener, alerts | [Observability](observability.md) |
| Keep the tables small | Call `purgeFinished` once a day | [Observability](observability.md#retention) |

The engine's delays and limits are in [Configuration](configuration.md).

## A checklist for production

- [ ] `schema.sql` is in a Flyway migration.
- [ ] Exposed's automatic retries are off (`defaultMaxAttempts = 1`).
- [ ] Every step client suspends (or uses `runInterruptible`), with a timeout below `callLock`.
- [ ] `settleDelay` is above the longest processing time of every external system without keys.
- [ ] `keyRetention` is the documented minimum of every idempotent step's service.
- [ ] `resumeDue` runs every 30 s, from a random start offset per pod (not on the clock), and `purgeFinished` daily.
- [ ] The `traceId` provider is wired, so the history links to traces.
- [ ] Gauges and alerts are in place for stuck sagas and the recovery lag.
- [ ] An admin screen lists stuck sagas, with access to saga data decided.
- [ ] The pods' clocks are synchronized (NTP).
- [ ] The pods' shutdown grace period is above `callLock`: a stopping pod lets its calls in flight end and records them, and one killed before that leaves them to be checked.
