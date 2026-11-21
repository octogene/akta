package com.example.saga

import kotlin.time.Duration
import kotlin.time.Duration.Companion.hours
import kotlin.time.Duration.Companion.minutes
import kotlin.time.Duration.Companion.seconds
import kotlin.time.Instant
import kotlin.uuid.Uuid

/** Direction of the saga, or its end. */
enum class SagaState {
    /** Executing steps forward. */
    RUNNING,
    /** A step was rejected: undoing the completed steps, last to first. */
    COMPENSATING,
    /** Every step is done. */
    COMPLETED,
    /** Every completed step was undone. */
    FAILED;

    val isFinished: Boolean get() = this == COMPLETED || this == FAILED
}

/** Status of the current step. */
enum class StepStatus {
    /** Ready to run (or to be undone) from `nextAttemptAt`. */
    READY,
    /** A call is in flight until `lockedUntil`. */
    IN_PROGRESS,
    /** A signal step waits for its signal, until `nextAttemptAt` (its timeout). No call is in flight. */
    WAITING,
    /** The outcome of the last call is unknown: a check is scheduled at `nextAttemptAt`. */
    UNKNOWN,
    /** Blocked: a human must step in (retry, mark as done, or abort). */
    STUCK,
}

/** The persisted state of a saga: one row. */
data class SagaRecord(
    val id: Uuid,
    val type: String,
    /** Business key, e.g. the id of the item. Only one unfinished saga per key. */
    val key: String,
    val state: SagaState,
    /** Index of the current step in the definition. */
    val stepIndex: Int,
    val stepStatus: StepStatus,
    /** Calls of the current step: executions, or compensations. Limited by [SagaConfig.maxAttempts]. */
    val attempts: Int,
    /** When the engine should look at this saga again. While a call is in flight, equals [lockedUntil]. */
    val nextAttemptAt: Instant,
    val lockedUntil: Instant?,
    val lastError: String?,
    /** W3C traceparent of the request that started the saga. */
    val traceparent: String?,
    val createdAt: Instant,
    /**
     * Consecutive failed checks of the current step (the lookup itself threw). Counted apart from
     * [attempts], so that a lookup outage does not use up the step's executions. Limited by
     * [SagaConfig.maxAttempts]: past it the step is STUCK, like a check that cannot tell.
     */
    val checkFailures: Int = 0,
    /** The saga's input, as written by the engine's [SagaSerializer]. Null when the saga has none. Never changes. */
    val input: String? = null,
    /** Set by [SagaEngine.cancel]: the saga compensates as soon as no call of it is in flight. */
    val cancelRequestedAt: Instant? = null,
)

/** One line of the saga history. */
data class SagaLogEntry(
    val sagaId: Uuid,
    val at: Instant,
    val stepIndex: Int,
    val stepName: String?,
    /** One of [SagaEvents]. */
    val event: String,
    val detail: String?,
    /** Author of a manual action. */
    val actor: String? = null,
    val traceId: String? = null,
)

/**
 * Values of [SagaLogEntry.event]. They are stored in the database and read by queries and
 * dashboards: never rename one.
 */
object SagaEvents {
    const val CREATED = "CREATED"
    const val LOCK_EXPIRED = "LOCK_EXPIRED"
    /** A call ended after its lock expired and another instance took over. */
    const val LATE_RESULT_IGNORED = "LATE_RESULT_IGNORED"
    /** The outcome of a call could not be saved: the saga is STUCK; a successful call's output is kept with its result. */
    const val RECORD_FAILED = "RECORD_FAILED"
    /** resumeDue could not run the saga (database error, bug…): postponed by [SagaConfig.resumeFailureDelay]. */
    const val RESUME_FAILED = "RESUME_FAILED"
    const val MANUAL_RETRY = "MANUAL_RETRY"
    const val MANUAL_DONE = "MANUAL_DONE"
    const val MANUAL_ABORT = "MANUAL_ABORT"
    /** A signal step started waiting; the detail gives the signal and the deadline. */
    const val WAIT_STARTED = "WAIT_STARTED"
    /** A signal step's timeout passed without its signal: checked, or stuck. */
    const val WAIT_TIMED_OUT = "WAIT_TIMED_OUT"
    /** A signal arrived; the detail says delivered, stored, consumed (stored earlier) or ignored. */
    const val SIGNAL_RECEIVED = "SIGNAL_RECEIVED"
    /** [SagaEngine.cancel] was called; the detail gives the reason. */
    const val CANCEL_REQUESTED = "CANCEL_REQUESTED"

    /*
     * Outcomes of a call are logged as "<action>:<outcome>":
     *   EXECUTE:Done, EXECUTE:Rejected, EXECUTE:NotSent, EXECUTE:Unknown,
     *   CHECK:Done, CHECK:NotDone, CHECK:CannotTell, CHECK:CheckFailed,
     *   COMPENSATE:Compensated, COMPENSATE:CompensationFailed,
     *   EXECUTE:OutputUnstorable, CHECK:OutputUnstorable (the call was made but its output could not be
     *   encoded, or is over SagaConfig.maxDataBytes: the step is STUCK).
     */
}

data class SagaConfig(
    /** How long a call may take before it is considered interrupted: HTTP timeout + margin. */
    val callLock: Duration = 35.seconds,
    /**
     * Wait before checking an unknown outcome. Must be longer than the time the external system
     * may still spend processing a request, otherwise the check could miss it and cause a duplicate.
     */
    val settleDelay: Duration = 2.minutes,
    /** Wait before a new check when the check itself failed. */
    val checkRetryDelay: Duration = 1.minutes,
    /** Calls of one step (executions or compensations), and consecutive failed checks, before it becomes STUCK. */
    val maxAttempts: Int = 10,
    val maxBackoff: Duration = 1.hours,
    /** Wait before resumeDue tries again a saga whose run threw, so that it does not block the others. */
    val resumeFailureDelay: Duration = 1.minutes,
    /**
     * Largest encoded input or step output, in UTF-8 bytes. Inputs and outputs are for parameters and
     * identifiers: anything large belongs in a business table, referenced by id.
     */
    val maxDataBytes: Int = 16 * 1024,
) {
    init {
        require(maxAttempts >= 1) { "maxAttempts must be at least 1" }
        require(maxDataBytes >= 1) { "maxDataBytes must be at least 1" }
    }

    /** 1, 2, 4… minutes, capped at [maxBackoff]. */
    fun backoff(attempts: Int): Duration =
        (1 shl (attempts - 1).coerceIn(0, 20)).minutes.coerceAtMost(maxBackoff)
}
