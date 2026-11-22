# Observability

Two sources for two kinds of numbers: **gauges** from a query, for what alerts watch, and **counters** from a listener, for rates.

## Gauges: `stats()`

```kotlin
val stats = engine.stats()
stats.unfinished          // Map<(SagaState, StepStatus), Long>, e.g. (RUNNING, WAITING) to 12
stats.stuckByReason       // Map<StuckReason, Long>
stats.oldestUnfinishedAt  // the oldest unfinished saga that is not stuck (waiting ones count)
stats.oldestDueAt         // the oldest due next_attempt_at: now - this = how far behind recovery is
```

It reads partial indexes only (finished sagas, the bulk of the table, are never read): sample it about once a minute. A gauge from a query is right after every restart, which a counter is not.

## Counters: a `SagaListener`

```kotlin
val engine = SagaEngine(..., listeners = listOf(SagaListener { event -> /* count it */ }))
```

A listener is told about each history entry **after its transaction commits**, with the saga's type, state, step status and stuck reason as the entry left them.

- A change that rolls back is never reported.
- At most once: a crash, or a connection lost right after a commit, loses that transaction's events. The history stays the record.
- A listener that throws is ignored: the saga is not affected. Keep it quick.
- Creations are not reported: `create` runs in your transaction, whose commit the engine cannot see.

## With OpenTelemetry

```kotlin
val meter = GlobalOpenTelemetry.getMeter("akta")
val events = meter.counterBuilder("saga.events").build()

val engine = SagaEngine(
    transactions, repository, definitions, KotlinxJsonSagaSerializer(),
    listeners = listOf(SagaListener { e ->
        events.add(1, Attributes.of(stringKey("saga.type"), e.sagaType, stringKey("saga.event"), e.entry.event))
    }),
)

val latest = AtomicReference<SagaStats?>(null)        // a gauge callback cannot suspend: sample in a ticker
scope.launch { while (isActive) { latest.set(runCatching { engine.stats() }.getOrNull()); delay(60.seconds) } }
meter.gaugeBuilder("saga.stuck").ofLongs().buildWithCallback { m ->
    latest.get()?.stuckByReason?.forEach { (reason, n) -> m.record(n, Attributes.of(stringKey("reason"), reason.name)) }
}
```

Keep attributes low in cardinality (type, event, reason): never a saga id or a business key. Every pod reports the same gauges, so aggregate them with `max`, not `sum`.

## Alerts

| Alert | From | Starting threshold |
|---|---|---|
| Stuck sagas | `stuckByReason` | > 0 for 5 minutes |
| Recovery job behind | `now - oldestDueAt` | > 5 minutes |
| Old unfinished sagas | `now - oldestUnfinishedAt` | Above the longest signal timeout, plus an hour |
| Sagas that cannot be read | `RESUME_FAILED` events | More than a few per 10 minutes |
| Pods dying mid-call | `LOCK_EXPIRED` events | A rise against the usual rate |

Tune the thresholds after a few weeks in production.

## Tracing

Pass a `traceId` provider to the engine: every history entry then carries the OpenTelemetry trace id of the call, so a database row leads to its trace. The engine reads it on the caller's coroutine, so the coroutine must carry the OpenTelemetry context (for example `Context.current().asContextElement()`). The `traceparent` of the request that started a saga is stored on it, to link later recovery runs back to that request.

The history (`saga_log`) is the durable record of every exchange, which matters when application logs only live as long as a pod.

## Retention

Each saga's history takes about 1.5 KB, plus its input and outputs. Keep one month:

```kotlin
engine.purgeFinished(olderThan = 30.days)      // once a day, from the job runner
```

It deletes the sagas that finished more than 30 days ago, with their results, signals and history, in batches of 1,000 (one short transaction each). Unfinished and stuck sagas are never deleted, however old. Export what you must keep longer before it runs.
