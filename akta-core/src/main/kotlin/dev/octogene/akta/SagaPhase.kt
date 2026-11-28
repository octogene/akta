package dev.octogene.akta

import kotlin.time.Instant
import kotlin.uuid.Uuid

/*
 * A saga's row, typed: what each state of a saga holds, and nothing else. SagaRecord is the row as stored, with its
 * fields side by side; a SagaPhase has only the fields its state gives a meaning to, so that a combination the engine
 * must never write (a lock without a call in flight, a stuck saga without a reason, a refusal on a saga that moves on)
 * cannot be built. SagaPhase.of reads a row and refuses one that breaks these rules; record() writes it back, the same.
 */

/** What a saga keeps from its creation to its purge. */
internal data class Identity(
    val id: Uuid,
    val type: String,
    val key: String,
    val input: String?,
    val traceparent: String?,
    val createdAt: Instant,
)

/** A cancellation requested, and why. */
internal data class Cancellation(val at: Instant, val reason: String)

/** The current step's idempotency key: first used [since], [generation] bumped by `retry(newKey = true)`. */
internal data class KeyState(val since: Instant?, val generation: Int)

/** Where the current step of an unfinished saga stands. [due]: when the engine looks at it again (nextAttemptAt). */
internal sealed interface StepState {
    val due: Instant
    val status: StepStatus

    /** To run (or to undo), or to start waiting for its signal, from [due]. */
    data class Ready(override val due: Instant) : StepState {
        override val status get() = StepStatus.READY
    }

    /** A signal step waiting for its signal, until [due], its timeout. */
    data class Waiting(override val due: Instant) : StepState {
        override val status get() = StepStatus.WAITING
    }

    /** The outcome of a call is unknown: checked at [due]. */
    data class Checking(override val due: Instant) : StepState {
        override val status get() = StepStatus.CHECKING
    }

    /** The outcome of an idempotent step's call is unknown: replayed with its key at [due]. */
    data class Replaying(override val due: Instant) : StepState {
        override val status get() = StepStatus.REPLAYING
    }

    /**
     * A [call] in flight until [lockedUntil]. [due] is that too, unless a resume failed after the lock expired and
     * postponed the saga: the row is IN_PROGRESS until a claim records the expiry.
     */
    data class InFlight(val call: CallAction, val lockedUntil: Instant, override val due: Instant) : StepState {
        override val status get() = StepStatus.IN_PROGRESS
    }

    /** Stopped for a human, for [reason], since [at]. [from]: what it was, when resume failures stopped it. */
    data class Stuck(val reason: StuckReason, val at: Instant, val from: StepStatus?, override val due: Instant) : StepState {
        override val status get() = StepStatus.STUCK
    }
}

internal sealed interface SagaPhase {
    val identity: Identity
    val id: Uuid get() = identity.id
    val type: String get() = identity.type
    /** The state the row stores: the phase itself, or for a finished saga how it ended. */
    val state: SagaState
    val stepIndex: Int
    val attempts: Int
    val lastError: String?
    val cancellation: Cancellation?
    /** When the engine looks at the saga again: its step's, or for a finished saga the last one it had. */
    val due: Instant

    /** Running or compensating: a current step to act on. */
    sealed interface Unfinished : SagaPhase {
        val step: StepState
        val checkFailures: Int
        val resumeFailures: Int
        val key: KeyState
        override val due: Instant get() = step.due

        /** The same saga, with these fields changed; those not named are kept. */
        fun with(
            step: StepState = this.step,
            attempts: Int = this.attempts,
            checkFailures: Int = this.checkFailures,
            resumeFailures: Int = this.resumeFailures,
            key: KeyState = this.key,
            cancellation: Cancellation? = this.cancellation,
            lastError: String? = this.lastError,
        ): Unfinished = when (this) {
            is Running -> copy(
                step = step, attempts = attempts, checkFailures = checkFailures, resumeFailures = resumeFailures, key = key,
                cancellation = cancellation, lastError = lastError,
            )
            is Compensating -> copy(
                step = step, attempts = attempts, checkFailures = checkFailures, resumeFailures = resumeFailures, key = key,
                cancellation = cancellation, lastError = lastError,
            )
            is Finishing -> {
                check(checkFailures == 0 && key == NO_KEY) { "Saga ${identity.id} is finishing: it has no check nor key" }
                copy(step = step, attempts = attempts, resumeFailures = resumeFailures, cancellation = cancellation, lastError = lastError)
            }
        }
    }

    /** Executing its steps forward. [refusal]: a step's refusal that could not be recorded, kept while it is stuck. */
    data class Running(
        override val identity: Identity,
        override val stepIndex: Int,
        override val step: StepState,
        override val attempts: Int,
        override val checkFailures: Int,
        override val resumeFailures: Int,
        override val key: KeyState,
        override val cancellation: Cancellation?,
        override val lastError: String?,
        val refusal: String?,
    ) : Unfinished {
        override val state: SagaState get() = SagaState.RUNNING
    }

    /** Undoing its completed steps, last to first, because of [failureReason]. */
    data class Compensating(
        override val identity: Identity,
        override val stepIndex: Int,
        override val step: StepState,
        override val attempts: Int,
        override val checkFailures: Int,
        override val resumeFailures: Int,
        override val key: KeyState,
        override val cancellation: Cancellation?,
        override val lastError: String?,
        val failureReason: String,
    ) : Unfinished {
        override val state: SagaState get() = SagaState.COMPENSATING
    }

    /**
     * Every step done ([failureReason] null), or every completed step undone: only its hook is left to run, in a
     * transaction of its own. [attempts] counts the hook's failures; [step] is READY (to run it) or STUCK.
     */
    data class Finishing(
        override val identity: Identity,
        override val stepIndex: Int,
        override val step: StepState,
        override val attempts: Int,
        override val resumeFailures: Int,
        override val cancellation: Cancellation?,
        override val lastError: String?,
        val failureReason: String?,
    ) : Unfinished {
        override val state: SagaState get() = SagaState.FINISHING
        override val checkFailures: Int get() = 0
        override val key: KeyState get() = NO_KEY
        /** What it ends as once its hook ran. */
        val ending: SagaState get() = if (failureReason == null) SagaState.COMPLETED else SagaState.FAILED
    }

    /**
     * Ended: every step done ([failureReason] null), or every completed step undone. What it was doing last is kept
     * as it was: the step it ended on, its attempts, when it was due, and its last error once it failed.
     */
    data class Finished(
        override val identity: Identity,
        override val stepIndex: Int,
        override val attempts: Int,
        val lastDue: Instant,
        override val cancellation: Cancellation?,
        override val lastError: String?,
        val failureReason: String?,
    ) : SagaPhase {
        override val state: SagaState get() = if (failureReason == null) SagaState.COMPLETED else SagaState.FAILED
        override val due: Instant get() = lastDue
    }

    /** The row, as stored. */
    fun record(): SagaRecord {
        val base = SagaRecord(
            id = identity.id, type = identity.type, key = identity.key, input = identity.input,
            traceparent = identity.traceparent, createdAt = identity.createdAt,
            state = SagaState.RUNNING, stepIndex = stepIndex, stepStatus = StepStatus.READY, attempts = attempts,
            nextAttemptAt = identity.createdAt, lockedUntil = null, lastError = lastError,
            cancelRequestedAt = cancellation?.at, cancelReason = cancellation?.reason,
        )
        return when (this) {
            is Running -> base.withStep(step).copy(
                checkFailures = checkFailures, resumeFailures = resumeFailures,
                keySince = key.since, keyGeneration = key.generation, failureReason = refusal,
            )
            is Compensating -> base.withStep(step).copy(
                state = SagaState.COMPENSATING, checkFailures = checkFailures, resumeFailures = resumeFailures,
                keySince = key.since, keyGeneration = key.generation, failureReason = failureReason,
            )
            is Finishing -> base.withStep(step).copy(state = SagaState.FINISHING, resumeFailures = resumeFailures, failureReason = failureReason)
            is Finished -> base.copy(state = state, nextAttemptAt = lastDue, failureReason = failureReason)
        }
    }

    companion object {
        /** The typed form of [row]; throws IllegalStateException, naming the rule, if [row] breaks one. */
        fun of(row: SagaRecord): SagaPhase {
            fun rule(ok: Boolean, what: () -> String) = check(ok) { "Saga ${row.id} (${row.state}, ${row.stepStatus}): ${what()}" }
            rule(row.stepIndex >= 0 && row.attempts >= 0 && row.checkFailures >= 0 && row.keyGeneration >= 0 && row.resumeFailures >= 0) {
                "negative step index or counter"
            }
            rule((row.cancelRequestedAt == null) == (row.cancelReason == null)) { "a cancellation needs both its time and its reason" }
            rule((row.stepStatus == StepStatus.IN_PROGRESS) == (row.lockedUntil != null)) { "a lock without a call in flight, or the reverse" }
            rule((row.stepStatus == StepStatus.IN_PROGRESS) == (row.callAction != null)) { "a call in flight says which one" }
            rule((row.stepStatus == StepStatus.STUCK) == (row.stuckReason != null)) { "a stuck saga has a reason, and only a stuck one" }
            rule((row.stuckReason == null) == (row.stuckAt == null)) { "a stuck reason goes with its time" }
            rule(row.stuckFrom == null || row.stuckReason == StuckReason.RESUME_FAILING) { "stuckFrom is only set by resume failures" }
            rule(row.stuckFrom != StepStatus.STUCK) { "a saga is not stuck from being stuck" }
            rule((row.stuckReason == StuckReason.HOOK_FAILED) <= (row.state == SagaState.FINISHING)) { "only a finishing saga's hook fails" }

            val identity = Identity(row.id, row.type, row.key, row.input, row.traceparent, row.createdAt)
            val cancellation = row.cancelRequestedAt?.let { Cancellation(it, checkNotNull(row.cancelReason)) }
            val key = KeyState(row.keySince, row.keyGeneration)
            val step = when (row.stepStatus) {
                StepStatus.READY -> StepState.Ready(row.nextAttemptAt)
                StepStatus.WAITING -> StepState.Waiting(row.nextAttemptAt)
                StepStatus.CHECKING -> StepState.Checking(row.nextAttemptAt)
                StepStatus.REPLAYING -> StepState.Replaying(row.nextAttemptAt)
                StepStatus.IN_PROGRESS -> StepState.InFlight(checkNotNull(row.callAction), checkNotNull(row.lockedUntil), row.nextAttemptAt)
                StepStatus.STUCK -> StepState.Stuck(checkNotNull(row.stuckReason), checkNotNull(row.stuckAt), row.stuckFrom, row.nextAttemptAt)
            }
            return when (row.state) {
                SagaState.RUNNING -> {
                    rule(row.failureReason == null || row.stuckReason == StuckReason.RECORD_FAILED) {
                        "a running saga keeps a refusal only while stuck because it could not be recorded"
                    }
                    rule(row.callAction != CallAction.COMPENSATE) { "a running saga does not compensate" }
                    SagaPhase.Running(
                        identity, row.stepIndex, step, row.attempts, row.checkFailures, row.resumeFailures, key, cancellation, row.lastError,
                        row.failureReason,
                    )
                }
                SagaState.COMPENSATING -> {
                    rule(row.failureReason != null) { "a saga compensates for a reason" }
                    rule(row.stepStatus != StepStatus.WAITING && !row.stepStatus.outcomeUnknown) { "an undo neither waits nor is checked" }
                    rule(row.callAction == null || row.callAction == CallAction.COMPENSATE) { "a compensating saga only compensates" }
                    SagaPhase.Compensating(
                        identity, row.stepIndex, step, row.attempts, row.checkFailures, row.resumeFailures, key, cancellation, row.lastError,
                        checkNotNull(row.failureReason),
                    )
                }
                SagaState.FINISHING -> {
                    rule(row.stepStatus == StepStatus.READY || row.stepStatus == StepStatus.STUCK) {
                        "a finishing saga only runs its hook, or is stuck"
                    }
                    rule(row.checkFailures == 0 && row.keySince == null && row.keyGeneration == 0) { "a finishing saga has no check nor key" }
                    rule(row.stuckReason == null || row.stuckReason == StuckReason.HOOK_FAILED || row.stuckReason == StuckReason.RESUME_FAILING) {
                        "a finishing saga is stuck by its hook, or by resume failures"
                    }
                    rule(row.failureReason != null || row.lastError == null || row.stepStatus == StepStatus.STUCK || row.attempts > 0) {
                        "a completing saga has no error, but its hook's"
                    }
                    SagaPhase.Finishing(
                        identity, row.stepIndex, step, row.attempts, row.resumeFailures, cancellation, row.lastError, row.failureReason,
                    )
                }
                SagaState.COMPLETED, SagaState.FAILED -> {
                    rule(row.stepStatus == StepStatus.READY) { "a finished saga has no step in progress" }
                    rule(row.checkFailures == 0 && row.resumeFailures == 0) { "a finished saga has no failure counted" }
                    rule(row.keySince == null && row.keyGeneration == 0) { "a finished saga has no key" }
                    rule((row.state == SagaState.FAILED) == (row.failureReason != null)) { "a saga failed for a reason, and completed without one" }
                    rule(row.state == SagaState.FAILED || row.lastError == null) { "a completed saga has no error" }
                    SagaPhase.Finished(identity, row.stepIndex, row.attempts, row.nextAttemptAt, cancellation, row.lastError, row.failureReason)
                }
            }
        }
    }
}

private val NO_KEY = KeyState(since = null, generation = 0)

/** The same step, due at [due]. */
internal fun StepState.dueAt(due: Instant): StepState = when (this) {
    is StepState.Ready -> copy(due = due)
    is StepState.Waiting -> copy(due = due)
    is StepState.Checking -> copy(due = due)
    is StepState.Replaying -> copy(due = due)
    is StepState.InFlight -> copy(due = due)
    is StepState.Stuck -> copy(due = due)
}

/** CHECKING or REPLAYING: the outcome of the last call is unknown, it may have been applied. */
internal val StepState.outcomeUnknown: Boolean get() = this is StepState.Checking || this is StepState.Replaying

/** CHECKING or REPLAYING: the outcome of the last call is unknown, it may have been applied. */
internal val StepStatus.outcomeUnknown: Boolean get() = this == StepStatus.CHECKING || this == StepStatus.REPLAYING

/** The typed form of this row: see [SagaPhase.of]. */
internal fun SagaRecord.phase(): SagaPhase = SagaPhase.of(this)

private fun SagaRecord.withStep(step: StepState): SagaRecord = when (step) {
    is StepState.Ready, is StepState.Waiting, is StepState.Checking, is StepState.Replaying ->
        copy(stepStatus = step.status, nextAttemptAt = step.due)
    is StepState.InFlight -> copy(stepStatus = step.status, nextAttemptAt = step.due, lockedUntil = step.lockedUntil, callAction = step.call)
    is StepState.Stuck -> copy(stepStatus = step.status, nextAttemptAt = step.due, stuckReason = step.reason, stuckAt = step.at, stuckFrom = step.from)
}
