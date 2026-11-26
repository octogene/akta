# Saga lifecycle

A saga has two levels of state: the **state** of the saga, which gives its direction or its end, and the **status** of its current step.

## Saga states

| State | Meaning |
|---|---|
| `RUNNING` | Executing steps forward |
| `COMPENSATING` | A step was refused, or the saga was cancelled or aborted: undoing the completed steps, last to first |
| `FINISHING` | Every step is done, or undone, and the saga's `onCompleted` or `onFailed` is still to run, in a transaction of its own. Only for a saga that declares that hook |
| `COMPLETED` | Every step is done. `onCompleted` ran |
| `FAILED` | Every completed step was undone. `onFailed` ran |

```mermaid
stateDiagram-v2
    direction LR
    [*] --> RUNNING : create, in the business transaction
    RUNNING --> RUNNING : step done, next step
    RUNNING --> FINISHING : last step done
    RUNNING --> COMPLETED : last step done, no onCompleted
    RUNNING --> COMPENSATING : step or signal rejected, timeout check found nothing, cancel, abort
    RUNNING --> FINISHING : the same at the first step, nothing to undo
    COMPENSATING --> COMPENSATING : step undone, previous step
    COMPENSATING --> FINISHING : first step undone
    COMPENSATING --> FAILED : first step undone, no onFailed
    FINISHING --> COMPLETED : onCompleted ran
    FINISHING --> FAILED : onFailed ran
    FINISHING --> COMPENSATING : abort, onCompleted stuck
    COMPLETED --> [*] : purgeFinished, after the retention period
    FAILED --> [*] : purgeFinished, after the retention period
```

`RUNNING`, `COMPENSATING` and `FINISHING` are **unfinished**: the saga holds its business key (no other unfinished saga, of any type, may use it), and the recovery job picks it up whenever it is due. `COMPLETED` and `FAILED` are **final**: nothing changes them, and a new saga may start for the same key.

## Step statuses

| Status | Meaning |
|---|---|
| `READY` | Will be called (or undone) at `next_attempt_at` |
| `IN_PROGRESS` | A call is in flight, protected by a lock until `locked_until` |
| `WAITING` | A signal step waits for its event, until its timeout at `next_attempt_at`. No call is in flight |
| `CHECKING` | The last call's outcome is unknown: it may have been applied. Its check runs at `next_attempt_at` |
| `REPLAYING` | The same, for an idempotent step: it is executed again with the same key at `next_attempt_at`, which its service answers with the first result |
| `STUCK` | A human must decide. `stuck_reason` says why |

```mermaid
stateDiagram-v2
    [*] --> READY
    READY --> IN_PROGRESS : claimed
    IN_PROGRESS --> READY : done (next step), or not sent (retry later)
    IN_PROGRESS --> CHECKING : unknown outcome, or lock expired
    IN_PROGRESS --> REPLAYING : the same, idempotent step
    CHECKING --> IN_PROGRESS : check, after the settle delay
    REPLAYING --> IN_PROGRESS : replay with the key
    IN_PROGRESS --> STUCK : cannot tell, too many attempts, output not stored
    STUCK --> READY : manual retry
    READY --> WAITING : signal step, no signal yet
    WAITING --> READY : signal accepted (next step)
    WAITING --> CHECKING : timeout, with a check
    WAITING --> STUCK : timeout, without a check
```

## Who moves a saga

| Who | Calls | When |
|---|---|---|
| The request that starts it | `create`, then `run` | At once, within a time budget |
| The recovery job | `resumeDue` | Every 30 s or so: whatever is due |
| A listener | `signal` | When an awaited event arrives |
| An operator, or the customer | `retry`, `markDone`, `abort`, `markCompensated`, `cancel` | When a saga is stuck, or must be abandoned |

Each of them goes through the same row lock, so they never act on a saga at the same time. Every change they make is written to `saga_log`.

## The hooks

`onCompleted` and `onFailed` never run in the transaction that records a step's outcome. Once the last outcome is committed, the saga is `FINISHING`, and its hook runs in a transaction of its own, which also sets `COMPLETED` or `FAILED`: the business change and the saga's end happen together, and a hook that throws loses nothing the steps did.

- **A hook that throws** rolls back what it wrote. It runs again after backoff, and after `maxAttempts` failures in a row the saga stops (`HOOK_FAILED`). `retry` runs it again; it never calls a step again.
- **A stuck `onCompleted`** can be aborted instead: every step is undone, the last one included. `cancel` is refused while finishing, since the steps are done.
- **A saga without the hook** ends in the transaction of its last outcome, as there is nothing to run.
