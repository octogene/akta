package dev.octogene.akta

import kotlin.uuid.Uuid

/**
 * Wraps each call a step makes (an execution, a replay, a check or a compensation), to trace it: a span per call,
 * with OpenTelemetry for one (the akta-opentelemetry module). Pass it to [SagaEngine].
 *
 * [trace] runs [block], which makes the call and returns its outcome, and returns that outcome. The engine guards
 * itself against a tracer that does not: if the tracer throws before the call, the call is made without it; if it
 * throws after, or runs [block] twice, the call is still made once, and its outcome kept. A tracer cannot change
 * what the saga does.
 */
public fun interface SagaTracer {
    public suspend fun trace(call: SagaCall, block: suspend () -> SagaCallOutcome): SagaCallOutcome
}

/** One call a step makes, as a [SagaTracer] sees it before it is made. */
public data class SagaCall(
    val sagaId: Uuid,
    val sagaType: String,
    /** The saga's business key. */
    val key: String,
    val step: String,
    val stepIndex: Int,
    val action: CallAction,
    /** The step's calls so far, this one included (for a check: the failed checks in a row, this one included). */
    val attempt: Int,
    /** The W3C traceparent of the request that created the saga, if it gave one: to link a later run to it. */
    val traceparent: String?,
)

/**
 * The outcome of a call, as the history logs it: [name] is the second half of its event (`Done`, `Unknown`,
 * `Rejected`, `NotSent`, `CheckFailed`, `Compensated`…, see [SagaEvents]). [failed]: the call failed, and is to be
 * checked, retried or looked at (a refusal is not a failure: the service answered). [error]: the error, if any.
 */
public data class SagaCallOutcome(val name: String, val failed: Boolean, val error: String?)
