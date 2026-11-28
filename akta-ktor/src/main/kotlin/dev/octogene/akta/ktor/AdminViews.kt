package dev.octogene.akta.ktor

import dev.octogene.akta.SagaEngine
import dev.octogene.akta.SagaLogEntry
import dev.octogene.akta.SagaRecord
import dev.octogene.akta.SagaStats
import dev.octogene.akta.SagaStepProgress
import kotlinx.serialization.Serializable
import kotlinx.serialization.json.JsonElement

/*
 * The JSON bodies of the admin API. Dates are ISO-8601 instants (UTC), ids are UUID strings, and enums
 * their names. No input or step output value ever appears in a response: only what saga_log and the
 * saga row already show to operators.
 */

/** A saga as an operator sees it. Its input is left out: it may hold personal data. */
@Serializable
public data class SagaView(
    val id: String,
    val type: String,
    val key: String,
    val state: String,
    val stepIndex: Int,
    val stepStatus: String,
    /** While the step is IN_PROGRESS, what the call in flight does: EXECUTE, REPLAY, CHECK or COMPENSATE. */
    val callAction: String?,
    val attempts: Int,
    val checkFailures: Int,
    /** Times in a row the saga could not be resumed, as counted towards RESUME_FAILING. */
    val resumeFailures: Int,
    val nextAttemptAt: String,
    val lockedUntil: String?,
    /** The latest problem: why the current step is retried, checked or stuck. */
    val lastError: String?,
    /** Why the saga compensates, or failed. Never overwritten. */
    val failureReason: String?,
    val stuckReason: String?,
    val stuckAt: String?,
    /** After RESUME_FAILING, the step status a retry resumes; null otherwise. */
    val stuckFrom: String?,
    val cancelRequestedAt: String?,
    val cancelReason: String?,
    val keySince: String?,
    val keyGeneration: Int,
    val traceparent: String?,
    val createdAt: String,
    /**
     * Every manual action, and whether the saga takes it now: what a screen offers. Computed by the checks the
     * actions make, as the saga was read: it may have moved on since, and an output may still not fit the step.
     */
    val actions: List<ActionView>,
    /**
     * The saga's steps, in the order of its definition, and where each stands: what a screen draws as a line. Only
     * in answers about one saga (`/{id}` and the actions): null in lists, where it would cost a read per saga, and
     * for a saga type this engine does not know.
     */
    val steps: List<StepView>?,
)

/**
 * One step: [progress] is DONE, CURRENT, PENDING, UNDONE or NOT_APPLIED. [ref] is what its `logAs` says of its
 * output, an identifier to look up in the external system: never the output itself.
 */
@Serializable
public data class StepView(
    val index: Int,
    val name: String,
    val progress: String,
    val ref: String?,
    val doneAt: String?,
    val compensatedAt: String?,
)

/**
 * A manual action ([action]: RETRY, RETRY_WITH_NEW_KEY, MARK_DONE, ABORT, ABORT_CURRENT_STEP_APPLIED,
 * MARK_COMPENSATED or CANCEL) and, when it is refused, why, in the words the action would answer 409 with.
 */
@Serializable
public data class ActionView(val action: String, val allowed: Boolean, val refusal: String?)

/** One page of stuck sagas, stuck longest first. Pass [next] as `after` for the following page. */
@Serializable
public data class StuckPage(val sagas: List<SagaView>, val next: String?)

@Serializable
public data class StatsView(
    /** Unfinished sagas, by state and step status. */
    val unfinished: List<StatusCount>,
    val stuckByReason: Map<String, Long>,
    /** When the oldest unfinished saga that is not stuck was created. */
    val oldestUnfinishedAt: String?,
    /** The oldest next attempt already due: how far behind the recovery job is. */
    val oldestDueAt: String?,
)

@Serializable
public data class StatusCount(val state: String, val stepStatus: String, val count: Long)

@Serializable
public data class LogEntryView(
    val sagaId: String,
    val at: String,
    val stepIndex: Int,
    val stepName: String?,
    val event: String,
    val detail: String?,
    val actor: String?,
    val traceId: String?,
)

/** Every error: what was wrong, in a sentence. */
@Serializable
public data class ErrorView(val error: String)

// ---- Request bodies ----------------------------------------------------------------------

@Serializable
public data class RetryRequest(
    /** Send a fresh idempotency key: only once the step is known not to be applied. */
    val newKey: Boolean = false,
)

@Serializable
public data class MarkDoneRequest(
    /**
     * The step's output, as a JSON value (`"STK-1"`, `{"id": "H-1"}`): what KotlinxJsonSagaSerializer stores.
     * Left out, the output already saved for the step is kept (after a RECORD_FAILED), or there is none.
     * For a signal step, the signal's value: its onSignal accepts or refuses it, as for a signal received.
     */
    val output: JsonElement? = null,
)

@Serializable
public data class AbortRequest(
    /** The stuck step was applied, as found in the external system: it is compensated too. */
    val currentStepApplied: Boolean,
    /** With [currentStepApplied], the output its compensation receives, as in [MarkDoneRequest]. */
    val output: JsonElement? = null,
)

@Serializable
public data class CancelRequest(val reason: String)

// ---- Mapping -----------------------------------------------------------------------------

internal fun SagaRecord.toView(engine: SagaEngine, steps: List<SagaStepProgress>? = null) = SagaView(
    id = id.toString(), type = type, key = key, state = state.name, stepIndex = stepIndex,
    stepStatus = stepStatus.name, callAction = callAction?.name, attempts = attempts, checkFailures = checkFailures,
    resumeFailures = resumeFailures, nextAttemptAt = nextAttemptAt.toString(), lockedUntil = lockedUntil?.toString(),
    lastError = lastError, failureReason = failureReason, stuckReason = stuckReason?.name,
    stuckAt = stuckAt?.toString(), stuckFrom = stuckFrom?.name, cancelRequestedAt = cancelRequestedAt?.toString(),
    cancelReason = cancelReason, keySince = keySince?.toString(), keyGeneration = keyGeneration,
    traceparent = traceparent, createdAt = createdAt.toString(),
    actions = engine.actions(this).map { ActionView(it.action.name, it.allowed, it.refusal) },
    steps = steps?.map { StepView(it.index, it.name, it.progress.name, it.ref, it.doneAt?.toString(), it.compensatedAt?.toString()) },
)

internal fun SagaStats.toView() = StatsView(
    unfinished = unfinished.map { (status, count) -> StatusCount(status.first.name, status.second.name, count) }
        .sortedWith(compareBy({ it.state }, { it.stepStatus })),
    stuckByReason = stuckByReason.mapKeys { it.key.name }.toSortedMap(),
    oldestUnfinishedAt = oldestUnfinishedAt?.toString(),
    oldestDueAt = oldestDueAt?.toString(),
)

internal fun SagaLogEntry.toView() = LogEntryView(
    sagaId = sagaId.toString(), at = at.toString(), stepIndex = stepIndex, stepName = stepName,
    event = event, detail = detail, actor = actor, traceId = traceId,
)
