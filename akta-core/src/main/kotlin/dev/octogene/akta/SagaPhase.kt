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

    /** The outcome of a call is unknown: checked, or replayed with its key, at [due]. */
    data class Unknown(override val due: Instant) : StepState {
        override val status get() = StepStatus.UNKNOWN
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
    ) : Unfinished

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
    ) : Unfinished

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
        val state: SagaState get() = if (failureReason == null) SagaState.COMPLETED else SagaState.FAILED
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
            is Finished -> base.copy(state = state, nextAttemptAt = lastDue, failureReason = failureReason)
        }
    }

    companion object {
        /** The typed form of [r]; throws IllegalStateException, naming the rule, if [r] breaks one. */
        fun of(r: SagaRecord): SagaPhase {
            fun rule(ok: Boolean, what: () -> String) = check(ok) { "Saga ${r.id} (${r.state}, ${r.stepStatus}): ${what()}" }
            rule(r.stepIndex >= 0 && r.attempts >= 0 && r.checkFailures >= 0 && r.keyGeneration >= 0 && r.resumeFailures >= 0) {
                "negative step index or counter"
            }
            rule((r.cancelRequestedAt == null) == (r.cancelReason == null)) { "a cancellation needs both its time and its reason" }
            rule((r.stepStatus == StepStatus.IN_PROGRESS) == (r.lockedUntil != null)) { "a lock without a call in flight, or the reverse" }
            rule((r.stepStatus == StepStatus.IN_PROGRESS) == (r.callAction != null)) { "a call in flight says which one" }
            rule((r.stepStatus == StepStatus.STUCK) == (r.stuckReason != null)) { "a stuck saga has a reason, and only a stuck one" }
            rule((r.stuckReason == null) == (r.stuckAt == null)) { "a stuck reason goes with its time" }
            rule(r.stuckFrom == null || r.stuckReason == StuckReason.RESUME_FAILING) { "stuckFrom is only set by resume failures" }
            rule(r.stuckFrom != StepStatus.STUCK) { "a saga is not stuck from being stuck" }

            val identity = Identity(r.id, r.type, r.key, r.input, r.traceparent, r.createdAt)
            val cancellation = r.cancelRequestedAt?.let { Cancellation(it, checkNotNull(r.cancelReason)) }
            val key = KeyState(r.keySince, r.keyGeneration)
            val step = when (r.stepStatus) {
                StepStatus.READY -> StepState.Ready(r.nextAttemptAt)
                StepStatus.WAITING -> StepState.Waiting(r.nextAttemptAt)
                StepStatus.UNKNOWN -> StepState.Unknown(r.nextAttemptAt)
                StepStatus.IN_PROGRESS -> StepState.InFlight(checkNotNull(r.callAction), checkNotNull(r.lockedUntil), r.nextAttemptAt)
                StepStatus.STUCK -> StepState.Stuck(checkNotNull(r.stuckReason), checkNotNull(r.stuckAt), r.stuckFrom, r.nextAttemptAt)
            }
            return when (r.state) {
                SagaState.RUNNING -> {
                    rule(r.failureReason == null || r.stuckReason == StuckReason.RECORD_FAILED) {
                        "a running saga keeps a refusal only while stuck because it could not be recorded"
                    }
                    rule(r.callAction != CallAction.COMPENSATE) { "a running saga does not compensate" }
                    SagaPhase.Running(identity, r.stepIndex, step, r.attempts, r.checkFailures, r.resumeFailures, key, cancellation, r.lastError, r.failureReason)
                }
                SagaState.COMPENSATING -> {
                    rule(r.failureReason != null) { "a saga compensates for a reason" }
                    rule(r.stepStatus != StepStatus.WAITING && r.stepStatus != StepStatus.UNKNOWN) { "an undo neither waits nor is checked" }
                    rule(r.callAction == null || r.callAction == CallAction.COMPENSATE) { "a compensating saga only compensates" }
                    SagaPhase.Compensating(
                        identity, r.stepIndex, step, r.attempts, r.checkFailures, r.resumeFailures, key, cancellation, r.lastError,
                        checkNotNull(r.failureReason),
                    )
                }
                SagaState.COMPLETED, SagaState.FAILED -> {
                    rule(r.stepStatus == StepStatus.READY) { "a finished saga has no step in progress" }
                    rule(r.checkFailures == 0 && r.resumeFailures == 0) { "a finished saga has no failure counted" }
                    rule(r.keySince == null && r.keyGeneration == 0) { "a finished saga has no key" }
                    rule((r.state == SagaState.FAILED) == (r.failureReason != null)) { "a saga failed for a reason, and completed without one" }
                    rule(r.state == SagaState.FAILED || r.lastError == null) { "a completed saga has no error" }
                    SagaPhase.Finished(identity, r.stepIndex, r.attempts, r.nextAttemptAt, cancellation, r.lastError, r.failureReason)
                }
            }
        }
    }
}

/** The same step, due at [due]. */
internal fun StepState.dueAt(due: Instant): StepState = when (this) {
    is StepState.Ready -> copy(due = due)
    is StepState.Waiting -> copy(due = due)
    is StepState.Unknown -> copy(due = due)
    is StepState.InFlight -> copy(due = due)
    is StepState.Stuck -> copy(due = due)
}

/** The typed form of this row: see [SagaPhase.of]. */
internal fun SagaRecord.phase(): SagaPhase = SagaPhase.of(this)

/** The typed form of this row, which must be an unfinished saga's. */
internal fun SagaRecord.unfinished(): SagaPhase.Unfinished =
    phase() as? SagaPhase.Unfinished ?: error("Saga $id is $state: it has no step to act on")

private fun SagaRecord.withStep(step: StepState): SagaRecord = when (step) {
    is StepState.Ready, is StepState.Waiting, is StepState.Unknown -> copy(stepStatus = step.status, nextAttemptAt = step.due)
    is StepState.InFlight -> copy(stepStatus = step.status, nextAttemptAt = step.due, lockedUntil = step.lockedUntil, callAction = step.call)
    is StepState.Stuck -> copy(stepStatus = step.status, nextAttemptAt = step.due, stuckReason = step.reason, stuckAt = step.at, stuckFrom = step.from)
}
