package dev.octogene.akta.ktor

import dev.octogene.akta.SagaLogEntry
import dev.octogene.akta.SagaRecord
import dev.octogene.akta.SagaStats
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
    val attempts: Int,
    val checkFailures: Int,
    val nextAttemptAt: String,
    val lockedUntil: String?,
    /** The latest problem: why the current step is retried, checked or stuck. */
    val lastError: String?,
    /** Why the saga compensates, or failed. Never overwritten. */
    val failureReason: String?,
    val stuckReason: String?,
    val stuckAt: String?,
    val cancelRequestedAt: String?,
    val cancelReason: String?,
    val keySince: String?,
    val keyGeneration: Int,
    val traceparent: String?,
    val createdAt: String,
)

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

internal fun SagaRecord.toView() = SagaView(
    id = id.toString(), type = type, key = key, state = state.name, stepIndex = stepIndex,
    stepStatus = stepStatus.name, attempts = attempts, checkFailures = checkFailures,
    nextAttemptAt = nextAttemptAt.toString(), lockedUntil = lockedUntil?.toString(),
    lastError = lastError, failureReason = failureReason, stuckReason = stuckReason?.name,
    stuckAt = stuckAt?.toString(), cancelRequestedAt = cancelRequestedAt?.toString(),
    cancelReason = cancelReason, keySince = keySince?.toString(), keyGeneration = keyGeneration,
    traceparent = traceparent, createdAt = createdAt.toString(),
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
