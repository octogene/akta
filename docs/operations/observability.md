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

It reads every unfinished saga in one grouped query, and never the finished ones, the bulk of the table: sample it about once a minute, not per request. A gauge from a query is right after every restart, which a counter is not.

## Counters: a `SagaListener`

```kotlin
val engine = SagaEngine(..., listeners = listOf(SagaListener { event -> /* count it */ }))
```

A listener is told about each history entry **after its transaction commits**, with the saga's type, state, step status and stuck reason as the entry left them.

- A change that rolls back is never reported.
- At most once: a crash, or a connection lost right after a commit, loses that transaction's events. The history stays the record.
- A listener that throws is ignored: the saga is not affected. Keep it quick.
- Creations are not reported: `create` runs in your transaction, whose commit the engine cannot see.

## With OpenTelemetry: `akta-opentelemetry`

```kotlin
dependencies {
    implementation("dev.octogene:akta-opentelemetry:0.1.0")     // akta-core and the OpenTelemetry API
}
```

```kotlin
val engine = SagaEngine(
    transactions, repository, definitions, KotlinxJsonSagaSerializer(),
    traceId = ::openTelemetryTraceId,                               // each history entry names its trace
    listeners = listOf(OpenTelemetrySagaListener(openTelemetry)),   // the counter saga.events
    tracer = OpenTelemetrySagaTracer(openTelemetry),                // a span per step call
)
sagaGauges(engine, openTelemetry, scope)                            // stats() as gauges, sampled every minute

// In the handler that starts a saga: a later run, from the recovery job, is then linked to this request.
engine.create(reservation, key, input, traceparent = currentTraceparent())
```

The module uses only the OpenTelemetry **API**: the application gives its own `OpenTelemetry`, an SDK exporting OTLP or the one a Java agent installs. The coroutine that calls the engine must carry the OpenTelemetry context, for example with `Context.current().asContextElement()`. Ktor's OpenTelemetry instrumentation does that for each request.

### Spans

`OpenTelemetrySagaTracer` makes one span per call a step makes:

| | |
|---|---|
| **Name** | `saga.execute ORDER create-payment`, `saga.replay …`, `saga.check …`, `saga.compensate …` |
| **Parent** | the span current where `run` is called, so a run from an HTTP request is part of its trace. Without one (the recovery job, hours later, on another pod), a trace of its own, **linked** to the request that created the saga |
| **Children** | the step's own calls: the span is current while the step runs, so an instrumented HTTP client's spans sit under it |
| **Attributes** | `saga.id`, `saga.type`, `saga.key` (leave it out with `withKey = false` if your keys hold personal data), `saga.step`, `saga.step.index`, `saga.action`, `saga.attempt`, then `saga.outcome` (`Done`, `Unknown`, `Rejected`, `NotSent`, `CheckFailed`, `Compensated`…) and `saga.error` |
| **Status** | error for a failed call: an unknown outcome, a request not sent, a failed check or undo, an output that cannot be stored. A refusal (`Rejected`) is not an error: the service answered, and the saga compensates as it should |

A saga that lasts hours does not fit in one trace. Follow its links instead, or search by `saga.id`. A history entry carries the trace id of its call's span, so you can go from the history to the trace, and the reverse.

A tracer cannot change what a saga does. If it throws before the call, the call is made without it. If it throws after the call, or runs it twice, the call is still made once and its outcome is kept.

### Metrics

| Metric | Kind | Attributes | For |
|---|---|---|---|
| `saga.events` | counter | `saga.type`, `saga.event` | rates: retries, compensations, `LOCK_EXPIRED`, `RESUME_FAILED`, manual actions |
| `saga.stuck` | gauge | `saga.stuck_reason` | the alert: above 0 for a few minutes |
| `saga.unfinished` | gauge | `saga.state`, `saga.step_status` | the normal load |
| `saga.recovery.lag` | gauge, s | | how far behind the recovery job is |
| `saga.oldest_unfinished.age` | gauge, s | | sagas that never end |

Metric attributes stay few: types, events, reasons. A saga id or key belongs on spans, never on a metric. Every pod reports the same gauges, so aggregate them with `max`, not `sum`.

### With Datadog

Send OTLP to the Datadog Agent, or use `dd-java-agent`, which implements the OpenTelemetry API. Then:

- **Monitors** on `saga.stuck` (by reason) and `saga.recovery.lag`, with your operators' screen (the console's queue, say) as their link.
- **APM:** search spans by `@saga.id`, `@saga.key` or `@saga.outcome:Unknown`. A slow request shows its saga steps and their HTTP calls.
- **From the console to the trace:** a history entry's trace id opens it with `https://app.datadoghq.com/apm/trace/{traceId}`. Use the address of your Datadog site.

## Alerts

| Alert | From | Starting threshold |
|---|---|---|
| Stuck sagas | `stuckByReason` | > 0 for 5 minutes |
| Recovery job behind | `now - oldestDueAt` | > 5 minutes |
| Old unfinished sagas | `now - oldestUnfinishedAt` | Above the longest signal timeout, plus an hour |
| Sagas that cannot be read | `RESUME_FAILED` events | More than a few per 10 minutes |
| Pods dying mid-call | `LOCK_EXPIRED` events | A rise against the usual rate |

Tune the thresholds after a few weeks in production.

## Tracing without the module

Pass a `traceId` provider to the engine: every history entry then carries the trace id of its call, so a database row leads to its trace. The engine reads it on the caller's coroutine, so the coroutine must carry the context. The `traceparent` given to `create` is stored on the saga. Any other tracing system can implement `SagaTracer`, which `akta-opentelemetry` implements for OpenTelemetry.

The history (`saga_log`) is the durable record of every exchange, which matters when application logs only live as long as a pod.

## Retention

Each saga's history takes about 1.5 KB, plus its input and outputs. Keep one month:

```kotlin
engine.purgeFinished(olderThan = 30.days)      // once a day, from the job runner
```

It deletes the sagas that finished more than 30 days ago, with their results, signals and history, in batches of 1,000 (one short transaction each). Unfinished and stuck sagas are never deleted, however old. Export what you must keep longer before it runs.
