# Guarantees and limits

## What the engine guarantees

| Guarantee | How |
|---|---|
| A saga and its business change are created together, or not at all | `create` joins the caller's transaction |
| At most one unfinished saga per business key | A unique partial index |
| A saga is never run by two pods at once | Row locks; the recovery job skips locked rows |
| No external call inside a database transaction | Claim, call, record: the call runs between two commits |
| An unknown outcome is never replayed blindly | It is checked first, or replayed with an idempotency key |
| A call's outcome is recorded once | The record verifies the claim is still its own; a late result is ignored |
| The end of a saga and its business effects happen together | `onCompleted` and `onFailed` run in the saga's last transaction, once |
| No saga data in the history or the errors | Only what `logAs` returns is logged |
| Nothing is lost when the database or a pod fails | Every decision is in the saga's row; the recovery job finishes what was left |

The stress test checks these under random failures, against simulated external systems that record what was really applied.

## Duplicates are rarer, not impossible

The engine never replays a call blindly, but a duplicate can still happen when:

- **The external system applies a request late,** after `settleDelay`: the check finds nothing, the step runs again, and the late request lands too. Set `settleDelay` above the system's longest processing time, make it refuse duplicates where you can (a unique EPC), and reconcile periodically.
- **The lookup lags.** A check that reads from an eventually consistent search can answer "not done" for a call that was applied. Check against a source that already shows what was written.
- **A step classifies an error wrongly.** `StepNotSent` for a request that may have left skips the check.
- **An operator retries a step that was applied.** `retry` executes again without checking: look first.
- **An idempotent service breaks its contract:** it forgets a key early, or refuses a replay of an applied request.

## Limits to design around

=== "Timing"

    - The time budget of `run` is checked between calls: a request can wait up to its budget plus one call.
    - An unknown outcome takes at least `settleDelay` (2 minutes) to resolve.
    - No timeout on a whole saga: backoff waits up to an hour between attempts. Watch for old unfinished sagas, not only stuck ones.

=== "Calls"

    - Calls must be cancellable (suspending, or wrapped in `runInterruptible`), with timeouts below `callLock`: a blocking call cannot be cut, and may outlive its lock.
    - Compensations run more than once: they must be safe to repeat.
    - No isolation: others see the intermediate state while a saga runs.

=== "Definitions"

    - Steps run one after the other: no parallel branches.
    - One signal per name and saga; signals are routed by saga id only.
    - Renaming or reordering steps affects running sagas, which store their current step's index and name.
    - Removing a saga type leaves its unfinished sagas untouched, with their business keys blocked.
    - `onCompleted` and `onFailed` run in the engine's transaction: quick, no external calls.

=== "Operations"

    - No admin screen in the library: it gives the reads and the actions, the application exposes them.
    - Listener events are at most once: a crash right after a commit loses them.
    - A database outage postpones a whole `resumeDue` batch by `resumeFailureDelay`.
    - Retention deletes history for good.
    - PostgreSQL only: the repository relies on `SKIP LOCKED`, `ON CONFLICT` and partial indexes.
