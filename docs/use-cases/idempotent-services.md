# Services with idempotency keys

Some services accept an **idempotency key**: a request sent again with the same key returns the first result instead of being applied twice. Payment providers usually do. For such a step, the engine replays the call after a timeout instead of waiting and checking.

```kotlin
val payment = step<Payment>("create-payment") {
    idempotent(keyRetention = 24.hours)          // how long the service remembers a key
    execute { payments.create(input.amount, idempotencyKey = idempotencyKey) }
    compensate { own -> own?.let { payments.refund(it.id, idempotencyKey = idempotencyKey) } }
}
```

## What changes, and what does not

| | Step without a key | Idempotent step |
|---|---|---|
| Unknown outcome (timeout, `5xx`) | Wait 2 minutes, then `check` | Replay with the same key, after backoff |
| Pod died mid-call | Wait 2 minutes, then `check` | Replay with the same key, at once |
| A lookup to write | Yes, or every timeout needs a human | No |
| Typical stuck reasons | `CANNOT_TELL`, `CHECK_FAILING` | `TOO_MANY_ATTEMPTS`, `KEY_EXPIRED` |
| Compensation, signals, crash recovery, history | Same | Same |

Keys prevent duplicates. They do not undo anything or remember where a saga stands: everything else the engine does is still needed.

## The key

The engine derives it from what never changes during a saga, so every pod computes the same one: `<saga id>/<step name>/execute` for every attempt of the call, and `/compensate` for the undo. `idempotencyUuid` gives the same key as a UUID (version 5), for services that want one. It holds no saga data, and may be logged.

## Rules that keep it safe

- **A used key means "maybe applied".** Once a request may have left with the key, the step stays `UNKNOWN` until a replay answers. Meanwhile `abort` is refused, and `cancel` waits, then undoes the step too if the replay finds it done.
- **The key's window.** A service forgets keys after a while. The engine never starts a call that could end after `keyRetention` since the key's first use: the step stops with `KEY_EXPIRED` instead. If the step was not applied, an operator sends a fresh key with `retry(id, actor, newKey = true)`.
- **A request that never left does not use the key.** A first attempt that throws `StepNotSent` does not start the window.

## Before marking a step idempotent

"Supports idempotency keys" covers different guarantees. Confirm these with the service's team. Until then, the step can send the key without being marked idempotent: it gets the service's protection, and the engine keeps checking.

| Question | What the step does |
|---|---|
| How long is a key remembered? | Use it as `keyRetention`: the documented minimum |
| Does a replay of an applied request always return its first result? | **Required.** A refusal would be taken as "not applied", and the saga would compensate the earlier steps only |
| What happens to the same key with a different request? | Build the request only from the input and earlier outputs, which never change |
| A replay while the first request still runs? | Map the "in progress" answer (often `409`) to `StepNotSent` |
| Is a failure remembered for the key? | A remembered refusal is a `StepRejected`; for a temporary one, an operator uses `retry(newKey = true)` |
| Format and length? | `idempotencyKey` is about 70 characters (step names are limited to 100); `idempotencyUuid` is 36 |

!!! note "If every service had keys"
    The main reason this engine exists, replaying an unknown outcome being unsafe, would disappear. It would still be useful for crash recovery, compensation, signals and the synchronous answer. But an existing workflow engine would then be a fair alternative.
