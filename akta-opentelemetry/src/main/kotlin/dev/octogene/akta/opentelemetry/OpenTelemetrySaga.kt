package dev.octogene.akta.opentelemetry

import dev.octogene.akta.SagaCall
import dev.octogene.akta.SagaCallOutcome
import dev.octogene.akta.SagaEngine
import dev.octogene.akta.SagaEvent
import dev.octogene.akta.SagaListener
import dev.octogene.akta.SagaStats
import dev.octogene.akta.SagaTracer
import io.opentelemetry.api.OpenTelemetry
import io.opentelemetry.api.common.AttributeKey
import io.opentelemetry.api.common.Attributes
import io.opentelemetry.api.trace.Span
import io.opentelemetry.api.trace.SpanContext
import io.opentelemetry.api.trace.SpanKind
import io.opentelemetry.api.trace.StatusCode
import io.opentelemetry.api.trace.propagation.W3CTraceContextPropagator
import io.opentelemetry.context.Context
import io.opentelemetry.context.propagation.TextMapGetter
import io.opentelemetry.extension.kotlin.asContextElement
import io.opentelemetry.extension.kotlin.getOpenTelemetryContext
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Job
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.delay
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext
import java.util.concurrent.atomic.AtomicReference
import kotlin.coroutines.cancellation.CancellationException
import kotlin.time.Clock
import kotlin.time.Duration
import kotlin.time.Duration.Companion.seconds

/*
 * akta with OpenTelemetry: a span per step call (OpenTelemetrySagaTracer), the engine's events as a counter
 * (OpenTelemetrySagaListener), and its stats as gauges (sagaGauges). Datadog, Honeycomb, Grafana… read them
 * through the application's OpenTelemetry: an SDK exporting OTLP, or a Java agent.
 *
 *   val engine = SagaEngine(
 *       transactions, repository, definitions, serializer,
 *       traceId = ::openTelemetryTraceId,
 *       listeners = listOf(OpenTelemetrySagaListener(openTelemetry)),
 *       tracer = OpenTelemetrySagaTracer(openTelemetry),
 *   )
 *   engine.create(definition, key, input, traceparent = currentTraceparent())
 */

private const val INSTRUMENTATION = "dev.octogene.akta"

private val SAGA_ID = AttributeKey.stringKey("saga.id")
private val SAGA_TYPE = AttributeKey.stringKey("saga.type")
private val SAGA_KEY = AttributeKey.stringKey("saga.key")
private val SAGA_STEP = AttributeKey.stringKey("saga.step")
private val SAGA_STEP_INDEX = AttributeKey.longKey("saga.step.index")
private val SAGA_ACTION = AttributeKey.stringKey("saga.action")
private val SAGA_ATTEMPT = AttributeKey.longKey("saga.attempt")
private val SAGA_OUTCOME = AttributeKey.stringKey("saga.outcome")
private val SAGA_ERROR = AttributeKey.stringKey("saga.error")
private val SAGA_EVENT = AttributeKey.stringKey("saga.event")
private val SAGA_STATE = AttributeKey.stringKey("saga.state")
private val SAGA_STEP_STATUS = AttributeKey.stringKey("saga.step_status")
private val SAGA_STUCK_REASON = AttributeKey.stringKey("saga.stuck_reason")

/**
 * A span for each call a step makes: `saga.execute ORDER create-payment`, `saga.check …`, `saga.compensate …`.
 *
 * - **Its parent:** the span current where [SagaEngine.run] is called, so that a run from an HTTP request is part
 *   of its trace. Without one (the recovery job, hours later, on another pod), the span starts a trace of its own,
 *   **linked** to the request that created the saga, when its `traceparent` was given to `create`.
 * - **Its children:** the span is current while the step runs, so the spans of the step's own calls (an instrumented
 *   HTTP client) are its children.
 * - **Its attributes:** `saga.id`, `saga.type`, `saga.key` (unless [withKey] is false), `saga.step`,
 *   `saga.step.index`, `saga.action`, `saga.attempt`, and once it ends `saga.outcome` (`Done`, `Unknown`,
 *   `Rejected`…). A failed call (to check, retry, or look at) sets the span's status to error, with its error;
 *   a refusal does not: the service answered, and the saga compensates as it should.
 *
 * Never a saga's input or a step's output: only what the history shows.
 */
public class OpenTelemetrySagaTracer @JvmOverloads constructor(
    openTelemetry: OpenTelemetry,
    /** Whether spans carry the saga's business key, `saga.key`: leave it out if your keys hold personal data. */
    private val withKey: Boolean = true,
) : SagaTracer {
    private val tracer = openTelemetry.getTracer(INSTRUMENTATION)

    override suspend fun trace(call: SagaCall, block: suspend () -> SagaCallOutcome): SagaCallOutcome {
        val parent = parentContext()
        val builder = tracer.spanBuilder("saga.${call.action.name.lowercase()} ${call.sagaType} ${call.step}")
            .setParent(parent)
            .setSpanKind(SpanKind.INTERNAL)
            .setAttribute(SAGA_ID, call.sagaId.toString())
            .setAttribute(SAGA_TYPE, call.sagaType)
            .setAttribute(SAGA_STEP, call.step)
            .setAttribute(SAGA_STEP_INDEX, call.stepIndex.toLong())
            .setAttribute(SAGA_ACTION, call.action.name)
            .setAttribute(SAGA_ATTEMPT, call.attempt.toLong())
        if (withKey) builder.setAttribute(SAGA_KEY, call.key)
        // No request around it: a trace of its own, linked to the one that created the saga.
        if (!Span.fromContext(parent).spanContext.isValid) spanContextOf(call.traceparent)?.let(builder::addLink)
        val span = builder.startSpan()
        try {
            val outcome = withContext(parent.with(span).asContextElement()) { block() }
            span.setAttribute(SAGA_OUTCOME, outcome.name)
            outcome.error?.let { span.setAttribute(SAGA_ERROR, it) }
            if (outcome.failed) span.setStatus(StatusCode.ERROR, outcome.error ?: outcome.name)
            return outcome
        } catch (e: Throwable) {
            if (e !is CancellationException) span.recordException(e)
            span.setStatus(StatusCode.ERROR, e.toString())
            throw e
        } finally {
            span.end()
        }
    }

    /** The context the run was called in: from the coroutine if it carries one, else the thread's. */
    private suspend fun parentContext(): Context {
        val fromCoroutine = currentCoroutineContext().getOpenTelemetryContext()
        return if (Span.fromContext(fromCoroutine).spanContext.isValid) fromCoroutine else Context.current()
    }
}

/** The span context a W3C `traceparent` names, if it is one. */
private fun spanContextOf(traceparent: String?): SpanContext? {
    if (traceparent == null) return null
    val context = W3CTraceContextPropagator.getInstance().extract(Context.root(), traceparent, Traceparent)
    return Span.fromContext(context).spanContext.takeIf { it.isValid }
}

private object Traceparent : TextMapGetter<String> {
    override fun keys(carrier: String): Iterable<String> = listOf("traceparent")

    override fun get(carrier: String?, key: String): String? = carrier.takeIf { key == "traceparent" }
}

/**
 * The current span's trace id, for [SagaEngine]'s `traceId`: every history entry then names the trace it was
 * written in, and with [OpenTelemetrySagaTracer], a call's entry names its own span's. Null without a span.
 */
public fun openTelemetryTraceId(): String? = Span.current().spanContext.takeIf { it.isValid }?.traceId

/**
 * The current span as a W3C `traceparent`, for [SagaEngine.create]: a later run of the saga, from the recovery job,
 * is then linked to the request that created it. Null without a span.
 */
public fun currentTraceparent(): String? {
    val span = Span.current().spanContext.takeIf { it.isValid } ?: return null
    return "00-${span.traceId}-${span.spanId}-${span.traceFlags.asHex()}"
}

/**
 * The engine's history as a counter, `saga.events`, by `saga.type` and `saga.event` (`EXECUTE:Done`,
 * `LOCK_EXPIRED`, `MANUAL_RETRY`…): retries, compensations and lock expirations as rates. Pass it to
 * [SagaEngine]'s listeners. Its attributes stay few (types and events), never a saga id or a key.
 */
public class OpenTelemetrySagaListener(openTelemetry: OpenTelemetry) : SagaListener {
    private val events = openTelemetry.getMeter(INSTRUMENTATION).counterBuilder("saga.events")
        .setDescription("Entries the engine wrote to its sagas' history, once committed")
        .build()

    override fun onEvent(event: SagaEvent) {
        events.add(1, Attributes.of(SAGA_TYPE, event.sagaType, SAGA_EVENT, event.entry.event))
    }
}

/**
 * The engine's [SagaEngine.stats] as gauges, sampled every [period] in [scope] (stats reads every unfinished saga:
 * about once a minute, not more):
 *
 * - `saga.stuck`, by `saga.stuck_reason`: what to alert on (above 0 for a few minutes);
 * - `saga.unfinished`, by `saga.state` and `saga.step_status`;
 * - `saga.recovery.lag`, in seconds: how far behind the recovery job is (0 when nothing is due);
 * - `saga.oldest_unfinished.age`, in seconds.
 *
 * Every pod reports the same values: aggregate them with max, not sum. Cancel the job returned to stop sampling.
 */
public fun sagaGauges(
    engine: SagaEngine,
    openTelemetry: OpenTelemetry,
    scope: CoroutineScope,
    period: Duration = 60.seconds,
    clock: Clock = Clock.System,
): Job {
    require(period.isPositive()) { "period must be positive" }
    val latest = AtomicReference<SagaStats?>(null)
    val meter = openTelemetry.getMeter(INSTRUMENTATION)
    meter.gaugeBuilder("saga.stuck").ofLongs().setDescription("Stuck sagas, by reason").buildWithCallback { m ->
        latest.get()?.stuckByReason?.forEach { (reason, n) -> m.record(n, Attributes.of(SAGA_STUCK_REASON, reason.name)) }
    }
    meter.gaugeBuilder("saga.unfinished").ofLongs().setDescription("Unfinished sagas, by state and step status").buildWithCallback { m ->
        latest.get()?.unfinished?.forEach { (status, n) ->
            m.record(n, Attributes.of(SAGA_STATE, status.first.name, SAGA_STEP_STATUS, status.second.name))
        }
    }
    meter.gaugeBuilder("saga.recovery.lag").setUnit("s").setDescription("How long the oldest saga due has waited").buildWithCallback { m ->
        latest.get()?.let { stats -> m.record(stats.oldestDueAt?.let { (clock.now() - it).inWholeMilliseconds / 1000.0 } ?: 0.0) }
    }
    meter.gaugeBuilder("saga.oldest_unfinished.age").setUnit("s").setDescription("Age of the oldest unfinished saga").buildWithCallback { m ->
        latest.get()?.oldestUnfinishedAt?.let { m.record((clock.now() - it).inWholeMilliseconds / 1000.0) }
    }
    return scope.launch {
        while (isActive) {
            try {
                latest.set(engine.stats())
            } catch (e: CancellationException) {
                throw e
            } catch (_: Exception) {
                // The database, for once: the gauges keep their last values until the next sample.
            }
            delay(period)
        }
    }
}
