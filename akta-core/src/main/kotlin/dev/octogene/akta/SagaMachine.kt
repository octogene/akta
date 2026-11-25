package dev.octogene.akta

import kotlin.time.Instant
import kotlin.uuid.Uuid

/*
 * The decisions of the engine, apart from what runs them: no store, clock, serializer or step code here. Each takes
 * the saga's row as it is and returns the row as it must be, and what to write besides it. SagaEngine reads and
 * writes the store, calls the steps and runs the hooks, in its transactions.
 */

/** [event]: the first half of the logged event. Stored: never change it. */
internal enum class Action(val event: String) {
    EXECUTE("EXECUTE"),
    CHECK("CHECK"),
    COMPENSATE("COMPENSATE"),
}

/** [event]: the second half of the logged event (see [SagaEvents]). Stored: never change it. */
internal sealed class Outcome(val event: String, val detail: String? = null) {
    /** [output] is encoded; [logged] is what the step's describe() returned: never the value itself. */
    class Done(val output: String?, logged: String?) : Outcome("Done", logged?.let { "ref=$it" })
    /** The call was made, but its output could not be encoded or is too large: the step must not run again. */
    class OutputUnstorable(val reason: String) : Outcome("OutputUnstorable", reason)
    /** [noSignalFound]: a signal step's timeout check found nothing, rather than a refusal. */
    class Rejected(val error: String, val noSignalFound: Boolean = false) : Outcome("Rejected", error)
    class NotSent(val error: String) : Outcome("NotSent", error)
    class Unknown(val error: String) : Outcome("Unknown", error)
    object NotDone : Outcome("NotDone")
    object CannotTell : Outcome("CannotTell")
    class CheckFailed(val error: String) : Outcome("CheckFailed", error)
    object Compensated : Outcome("Compensated")
    class CompensationFailed(val error: String) : Outcome("CompensationFailed", error)
    /**
     * A signal step's timeout check found [value], encoded in [done]. Its onSignal decides in record(), inside the
     * transaction: [done] if it accepts, Rejected if it refuses.
     */
    class Found(val value: Any?, val done: Done) : Outcome("Done", done.detail)
    /** No time was left to call before the lock expires: nothing was called. Logged as NOT_CALLED. */
    class NotCalled(reason: String) : Outcome("NotCalled", reason)
}

/** What a decision writes besides the saga's row, for its current step. */
internal sealed interface Effect {
    /** The step is done, with its encoded [output]. */
    data class SaveResult(val output: String?) : Effect
    /** The step is undone. */
    data object MarkCompensated : Effect
    /** The signal [signal] is stored with [value], encoded, unless one of that name was received first. */
    data class SaveSignal(val signal: String, val value: String) : Effect
}

/** What a signal does on arrival: see [SagaMachine.arrival]. */
internal enum class Arrival { TOO_LATE, CANCELLED, STORE }

/** What becomes of a signal once stored: see [SagaMachine.useOfStored]. */
internal enum class Use {
    /** Its application failed before: the saga stops, the signal kept. */
    BLOCK,
    /** Its step is listening: onSignal decides now. */
    APPLY,
    /** Its step does not listen yet: kept for it. */
    KEEP,
}

/** A decision: the saga's row as it must be, what to write besides it, and what to log about it ([detail]). */
internal class Decision(val record: SagaRecord, val effects: List<Effect>, val detail: String? = null)

/** A move on a saga that calls nothing: see [SagaMachine.move]. Each but [Rest] and [Act] is saved and logged. */
internal sealed interface Move {
    /** Nothing to do now: finished, stuck, a call in flight elsewhere, or not due. */
    data class Rest(val record: SagaRecord) : Move
    /** The step is to be acted on (its data read, then waited for, consumed or called): [record] is the row so far. */
    data class Act(val record: SagaRecord) : Move
    /** The lock of a call expired: [record] says what follows (logged LOCK_EXPIRED, with its lastError). */
    data class LockExpired(val record: SagaRecord) : Move
    /** A cancellation that waited for a call to end: compensating now, or FAILED (logged CANCELLED). */
    data class Cancelled(val record: SagaRecord) : Move
    /** A step with nothing to undo, passed over (logged NOTHING_TO_UNDO). */
    data class NothingToUndo(val record: SagaRecord, val effects: List<Effect>) : Move
    /** A signal step's timeout passed and it has no check: stuck (logged WAIT_TIMED_OUT). The moves end. */
    data class NoSignal(val record: SagaRecord) : Move
}

/** The call claimed by [SagaMachine.claim], or why none is. */
internal sealed interface Claim {
    /** [record] is IN_PROGRESS; [replay]: an idempotent step whose key was used, an earlier attempt may be applied. */
    data class Call(val record: SagaRecord, val action: Action, val replay: Boolean) : Claim
    /** The step's idempotency key may be forgotten by its service: stuck (logged KEY_EXPIRED), no call. */
    data class KeyExpired(val record: SagaRecord) : Claim
}

internal class SagaMachine(private val config: SagaConfig) {
    /** A new saga, due at once on its first step. */
    fun created(id: Uuid, type: String, key: String, input: String?, traceparent: String?, now: Instant): SagaRecord =
        SagaRecord(
            id = id, type = type, key = key, state = SagaState.RUNNING,
            stepIndex = 0, stepStatus = StepStatus.READY, attempts = 0, nextAttemptAt = now,
            lockedUntil = null, lastError = null, traceparent = traceparent, createdAt = now, input = input,
        )

    /** Whether running the saga now could do anything: not finished, not stuck, and due. */
    fun canMoveOn(r: SagaRecord, now: Instant): Boolean =
        !r.state.isFinished && r.stepStatus != StepStatus.STUCK && r.nextAttemptAt <= now

    /** False when the lock of the call claimed as [claimed] expired during it, and another instance took over. */
    fun stillClaimed(current: SagaRecord, claimed: SagaRecord): Boolean =
        current.stepStatus == StepStatus.IN_PROGRESS &&
            current.stepIndex == claimed.stepIndex &&
            current.lockedUntil == claimed.lockedUntil

    /** The saga after its call, before its outcome decides: the lock released. Any outcome but a failed check ends the series of failed checks. */
    fun released(current: SagaRecord): SagaRecord = current.copy(lockedUntil = null, checkFailures = 0)

    /**
     * What a call's [outcome] makes of the saga [r] (its lock released), whose current step is [step] of [stepCount]:
     * the row, and what to write besides it, in the same transaction. [checkFailures] is the count before this record
     * reset it. [replay]: an earlier attempt may have been applied; [unclaimed]: the row before the claim.
     */
    fun decide(
        r: SagaRecord,
        checkFailures: Int,
        outcome: Outcome,
        step: SagaStep<*, *>,
        stepCount: Int,
        replay: Boolean,
        unclaimed: SagaRecord,
        now: Instant,
    ): Decision {
        val effects = mutableListOf<Effect>()
        val record = when (outcome) {
            is Outcome.Done -> {
                effects += Effect.SaveResult(outcome.output)
                next(r, stepCount, now)
            }
            is Outcome.OutputUnstorable -> stuck(
                r, StuckReason.OUTPUT_UNSTORABLE,
                "The call was made, but its output could not be stored (${outcome.reason}): " +
                    "record it with markDone, or undo the step with abort",
                now,
            )
            is Outcome.Rejected -> refused(r, outcome.error, now)
            // A replay that never left says nothing of the earlier attempt, which may be applied: still UNKNOWN.
            // A first attempt that never left did not use the key: its window has not started (keySince cleared).
            is Outcome.NotSent -> if (replay) {
                replayLater(r, "${outcome.error} (an earlier attempt may be applied)", now)
            } else {
                retryLater(r.copy(keySince = null), outcome.error, now)
            }
            is Outcome.Unknown -> when {
                step.idempotency == null -> r.copy(
                    stepStatus = StepStatus.UNKNOWN,
                    nextAttemptAt = now + config.settleDelay,
                    lastError = outcome.error,
                )
                else -> replayLater(r, outcome.error, now)
            }
            // Not applied: safe to execute again, or to stop here if the step keeps failing.
            Outcome.NotDone -> if (r.attempts >= config.maxAttempts) {
                stuck(r, StuckReason.TOO_MANY_ATTEMPTS, "Too many attempts: ${r.lastError}", now)
            } else {
                r.copy(stepStatus = StepStatus.READY, nextAttemptAt = now)
            }
            Outcome.CannotTell -> stuck(r, StuckReason.CANNOT_TELL, "Cannot tell whether step '${step.name}' was applied", now)
            // Counted apart from attempts (a lookup outage must not use up the executions), with backoff.
            // Past maxAttempts: STUCK, as when a check cannot tell, so that alerts see it and a human
            // can decide (markDone, abort, or retry once the step is known not to be applied).
            is Outcome.CheckFailed -> {
                val failures = checkFailures                   // this check was counted when claimed
                if (failures >= config.maxAttempts) {
                    stuck(
                        r, StuckReason.CHECK_FAILING,
                        "Check keeps failing, step '${step.name}' may have been applied: ${outcome.error}", now,
                    ).copy(checkFailures = failures)
                } else {
                    r.copy(
                        stepStatus = StepStatus.UNKNOWN, checkFailures = failures,
                        nextAttemptAt = now + maxOf(config.checkRetryDelay, config.backoff(failures)),
                        lastError = outcome.error,
                    )
                }
            }
            Outcome.Compensated -> {
                effects += Effect.MarkCompensated
                compensateFrom(r, r.stepIndex - 1, now)
            }
            is Outcome.CompensationFailed -> retryLater(r, outcome.error, now)
            is Outcome.Found -> error("A value found is decided by its onSignal, in the engine")
            // What the claim changed goes back, due at once: no attempt used, the idempotency key not used.
            // The rest is kept: a cancellation requested meanwhile, for one.
            is Outcome.NotCalled -> unclaimed.let { before ->
                r.copy(
                    stepStatus = before.stepStatus, attempts = before.attempts, checkFailures = before.checkFailures,
                    keySince = before.keySince, nextAttemptAt = now,
                )
            }
        }
        return Decision(record, effects)
    }

    // ---- Manual actions: each checks the saga can take it (IllegalArgumentException), then decides -----------

    /** See [SagaEngine.retry]. [stepOf] gives the current step; [signalStored] whether a signal of a name was received. */
    fun retry(
        r: SagaRecord,
        newKey: Boolean,
        now: Instant,
        stepOf: () -> SagaStep<*, *>,
        signalStored: (signal: String) -> Boolean,
    ): Decision {
        require(r.stepStatus == StepStatus.STUCK) { "Saga ${r.id} is not stuck" }
        require(newKey || r.stuckReason != StuckReason.KEY_EXPIRED) {
            "The idempotency key of saga ${r.id} may be forgotten by its service: confirm the step was not applied, " +
                "then retry with newKey = true (or markDone, or abort)"
        }
        val generation = if (newKey) r.keyGeneration + 1 else r.keyGeneration
        val step = if (r.state == SagaState.RUNNING) stepOf() else null
        // An idempotent step whose key was used may be applied: it is replayed, and stays UNKNOWN meanwhile.
        val replay = step?.idempotency != null && !newKey && r.keySince != null
        // Stopped by resume failures, not by the step: it resumes as it was.
        val resuming = r.stuckReason == StuckReason.RESUME_FAILING
        val (status, due) = when {
            replay -> StepStatus.UNKNOWN to now
            resuming && step != null -> resumeAs(r, step, now, signalStored)
            else -> StepStatus.READY to now
        }
        return Decision(
            r.copy(
                stepStatus = status, nextAttemptAt = due,
                attempts = if (resuming) r.attempts else 0, checkFailures = if (resuming) r.checkFailures else 0,
                lastError = null, stuckReason = null, stuckAt = null, stuckFrom = null, resumeFailures = 0,
                // Forward, a refusal kept by RECORD_FAILED is dropped: the step is called again.
                failureReason = if (r.state == SagaState.RUNNING) null else r.failureReason,
                keySince = if (newKey) null else r.keySince, keyGeneration = generation,
            ),
            emptyList(),
            detail = if (newKey) "newKey: generation $generation" else null,
        )
    }

    /** See [SagaEngine.markDone], which then saves the output and moves on with [next]. */
    fun checkMarkDone(r: SagaRecord) {
        require(r.state == SagaState.RUNNING && r.stepStatus == StepStatus.STUCK) { "Saga ${r.id} has no stuck forward step" }
        // Set on a saga still running only when its step's refusal could not be recorded (RECORD_FAILED).
        require(r.failureReason == null) {
            "The stuck step of saga ${r.id} was refused (${r.failureReason}): it cannot be marked done. " +
                "abort(currentStepApplied = false) gives up; retry calls it again"
        }
    }

    /** See [SagaEngine.abort], which saves the step's output in between, when it was applied. */
    fun checkAbort(r: SagaRecord, currentStepApplied: Boolean) {
        require(r.state == SagaState.RUNNING) { "Saga ${r.id} is not running forward" }
        require(r.stepStatus == StepStatus.READY || r.stepStatus == StepStatus.WAITING || r.stepStatus == StepStatus.STUCK) {
            "A call of saga ${r.id} may have been applied: wait for its outcome"
        }
        require(!currentStepApplied || r.stepStatus == StepStatus.STUCK) {
            "The current step of saga ${r.id} is ${r.stepStatus}: it was not applied"
        }
        // As in markDone: a refused step has nothing to undo, and its compensation must not run.
        require(!currentStepApplied || r.failureReason == null) {
            "The stuck step of saga ${r.id} was refused (${r.failureReason}), nothing applied: abort with currentStepApplied = false"
        }
    }

    fun abort(r: SagaRecord, currentStepApplied: Boolean, actor: String, now: Instant): SagaRecord =
        startCompensation(r, if (currentStepApplied) r.stepIndex else r.stepIndex - 1, "Aborted by $actor", now)

    /** See [SagaEngine.markCompensated]. */
    fun markCompensated(r: SagaRecord, now: Instant): Decision {
        require(r.state == SagaState.COMPENSATING && r.stepStatus == StepStatus.STUCK) { "Saga ${r.id} has no stuck compensation" }
        return Decision(compensateFrom(r, r.stepIndex - 1, now), listOf(Effect.MarkCompensated))
    }

    /** See [SagaEngine.cancel]. */
    fun cancel(r: SagaRecord, reason: String, now: Instant): Decision {
        require(!r.state.isFinished) { "Saga ${r.id} is finished: undoing it is a new saga" }
        // Already compensating, for another reason: nothing changes, and the history says so.
        if (r.state != SagaState.RUNNING) return Decision(r, emptyList(), detail = "$reason (already compensating: nothing changed)")
        // The first cancellation counts: a second one is only logged.
        val requested = if (r.cancelRequestedAt != null) r else r.copy(cancelRequestedAt = now, cancelReason = reason)
        return Decision(cancelIfRequested(requested, now), emptyList(), detail = reason)
    }

    // ---- Claims -------------------------------------------------------------------------------------------

    /**
     * The next move on the saga [r], before its step's data is read: a [Move.Rest] or [Move.Act] ends the moves; the
     * others are saved, then the next move is asked. [definitionOf] is asked once the saga is due (an unknown type
     * throws there), [stepOf] after a cancellation, [idempotent] only for a forward step whose lock expired.
     */
    fun move(
        r: SagaRecord,
        now: Instant,
        definitionOf: () -> SagaDefinition<*>,
        stepOf: () -> SagaStep<*, *>,
        idempotent: () -> Boolean,
    ): Move {
        if (r.state.isFinished || r.stepStatus == StepStatus.STUCK) return Move.Rest(r)
        var saga = r
        if (saga.stepStatus == StepStatus.IN_PROGRESS) {
            val lockedUntil = checkNotNull(saga.lockedUntil) { "Saga ${saga.id} is IN_PROGRESS without a lock" }
            if (lockedUntil > now) return Move.Rest(saga)                // a call is in flight elsewhere
            // Saved and logged; then the next move, at the same time: it rests if the saga is stuck or not due yet.
            return Move.LockExpired(lockExpired(saga, lockedUntil, now, idempotent))
        }
        if (saga.nextAttemptAt > now) return Move.Rest(saga)
        // Every move from here saves the saga: if this transaction commits, the saga could be run, which ends a
        // series of resume failures.
        if (saga.resumeFailures != 0) saga = saga.copy(resumeFailures = 0)
        definitionOf()
        // A cancellation that waited for a call to end: compensate now that none is in flight. Before the data is
        // decoded: the claim decodes it, in this same transaction.
        val cancelled = cancelIfRequested(saga, now)
        if (cancelled !== saga) return Move.Cancelled(cancelled)
        val step = stepOf()
        // A step with nothing to undo: marked undone at once, no call, and on to the previous one, all in this
        // transaction (usually the one that recorded the refusal or the previous undo).
        if (saga.state == SagaState.COMPENSATING && !step.compensates) {
            return Move.NothingToUndo(compensateFrom(saga, saga.stepIndex - 1, now), listOf(Effect.MarkCompensated))
        }
        // A signal step's timeout passed without its signal, and nothing will check: stuck, no data needed.
        if (step is SignalStep<*, *> && saga.state == SagaState.RUNNING && saga.stepStatus == StepStatus.WAITING && !step.checksOnTimeout) {
            return Move.NoSignal(stuck(saga, StuckReason.NO_SIGNAL, "No signal '${step.signal}' before the timeout", now))
        }
        return Move.Act(saga)
    }

    /** A signal step whose timeout passed: its check decides. */
    fun timedOut(r: SagaRecord, step: SignalStep<*, *>): SagaRecord =
        r.copy(stepStatus = StepStatus.UNKNOWN, lastError = "No signal '${step.signal}' before the timeout")

    /** The call of [step] that the saga [r] is due for, claimed: or the step stops, its key possibly forgotten. */
    fun claim(r: SagaRecord, step: SagaStep<*, *>, now: Instant): Claim {
        val idempotency = step.idempotency.takeIf { r.state == SagaState.RUNNING }
        val action = when {
            // An idempotent step's unknown outcome is replayed with the same key, never checked.
            r.stepStatus == StepStatus.UNKNOWN && idempotency == null -> Action.CHECK
            r.state == SagaState.RUNNING -> Action.EXECUTE
            else -> Action.COMPENSATE
        }
        var keySince = r.keySince
        val replay = idempotency != null && keySince != null
        // The key's window holds for the undo too: an idempotent step's compensation may rely on its key
        // (`/compensate`) to be safe to repeat, so an expired key is not sent again either. Only forward does
        // the key also replace the check.
        val keyWindow = step.idempotency
        if (keyWindow != null) {
            val since = keySince ?: now
            // The whole call must end while the service still knows the key: past that, a replay could duplicate.
            if (now + config.callLock >= since + keyWindow.keyRetention) {
                val call = if (action == Action.COMPENSATE) "undo" else "call"
                return Claim.KeyExpired(
                    stuck(
                        r, StuckReason.KEY_EXPIRED,
                        "The idempotency key of the $call of step '${step.name}', first used at $since, may be forgotten " +
                            "by its service (kept ${keyWindow.keyRetention}): sending it again could duplicate the $call",
                        now,
                    ),
                )
            }
            keySince = since
        }
        val lockedUntil = now + config.callLock
        val claimed = r.copy(
            stepStatus = StepStatus.IN_PROGRESS,
            attempts = if (action == Action.CHECK) r.attempts else r.attempts + 1,
            // Counted now, as an attempt is, and reset by any outcome but a failure: a check that ends without
            // recording one (its pod died, the record failed) is limited too.
            checkFailures = if (action == Action.CHECK) r.checkFailures + 1 else r.checkFailures,
            lockedUntil = lockedUntil,
            nextAttemptAt = lockedUntil,      // the recovery job looks again when the lock expires
            keySince = keySince,
        )
        return Claim.Call(claimed, action, replay)
    }

    // ---- Failures around a run ------------------------------------------------------------------------------

    /**
     * After a run threw (resumeDue, or the run after a manual action): the saga postponed, or stopped after too
     * many failures in a row; null to leave it as it is (finished, stuck, or a call in flight).
     */
    fun postpone(r: SagaRecord, error: String, now: Instant): SagaRecord? {
        val inFlight = r.stepStatus == StepStatus.IN_PROGRESS && r.lockedUntil?.let { it > now } == true
        if (r.state.isFinished || r.stepStatus == StepStatus.STUCK || inFlight) return null
        val failures = r.resumeFailures + 1
        return if (failures >= config.maxAttempts) {
            // The step is left as it was (attempts, key): retry resumes it, see there. No definition is
            // read here: an unknown type or a step that no longer exists may be the very cause.
            stuck(
                r, StuckReason.RESUME_FAILING,
                "Could not run the saga, $failures times in a row: $error. Fix the cause, then retry: " +
                    "the step resumes as it was, checked first if it may have been called",
                now,
            ).copy(lockedUntil = null, resumeFailures = failures, stuckFrom = r.stepStatus)
        } else {
            // lastError keeps the step's own error: the resume failure goes to the log only.
            r.copy(nextAttemptAt = maxOf(r.nextAttemptAt, now + config.resumeFailureDelay), resumeFailures = failures)
        }
    }

    /**
     * The record of [outcome] (described as [described]) failed, then the outcome alone too, with [error]: the saga
     * [current] stops (RECORD_FAILED) on the [step] that ran, keeping what the outcome established. [onSignalFailed]:
     * the error is onSignal's, or a hook's after a signal. [signalStored]: whether a signal of a name was received.
     */
    fun recordFailed(
        current: SagaRecord,
        step: SagaStep<*, *>,
        action: Action,
        outcome: Outcome,
        described: String,
        error: Throwable,
        onSignalFailed: Boolean,
        now: Instant,
        signalStored: (signal: String) -> Boolean,
    ): Decision {
        // A signal step's timeout check, whose value or signal is what failed to be recorded (onSignal or a hook
        // threw on it): the value found is stored as the step's signal (a real one stored first wins), so that
        // retry uses it at once instead of waiting a whole timeout again; a signal received during the check is
        // stored already. No refusal is kept then: the step got its value. An explicit StepRejected from the
        // check still refuses, as in record().
        val signalStep = (step as? SignalStep<*, *>)?.takeIf { action == Action.CHECK }
        val valueOrSignal = outcome is Outcome.Found || outcome == Outcome.CannotTell ||
            (outcome is Outcome.Rejected && outcome.noSignalFound)
        val effects = mutableListOf<Effect>()
        if (signalStep != null && outcome is Outcome.Found) {
            effects += Effect.SaveSignal(signalStep.signal, checkNotNull(outcome.done.output))
        }
        // The value found is stored above, unless a signal was stored first: either way, one is there.
        val signalKept = signalStep != null && valueOrSignal && (outcome is Outcome.Found || signalStored(signalStep.signal))
        val then = when {
            signalKept -> "The signal is kept" + (if (outcome is Outcome.Found) " (the value the check found)" else "") +
                ": fix the cause, then retry, which uses it"
            onSignalFailed -> "onSignal failed: fix the cause, then retry (a signal received is used; " +
                "otherwise the step waits again, then checks), or markDone with the value found"
            else -> when (outcome) {
            is Outcome.Done -> {
                effects += Effect.SaveResult(outcome.output)
                "The output is saved: markDone(id, null) continues with it"
            }
            Outcome.Compensated -> {
                effects += Effect.MarkCompensated
                "The undo was done: markCompensated(id) goes on with the previous steps"
            }
            is Outcome.Rejected -> "The step was refused, nothing applied: abort(id, currentStepApplied = false) gives up, as the refusal asked"
            else -> "Look in the external system for what the call did, then markDone, retry or abort"
            }
        }
        val refusal = (outcome as? Outcome.Rejected)?.error?.takeUnless { signalKept }
        val blocked = stuck(current, StuckReason.RECORD_FAILED, "Could not record $described: $error. $then", now)
            .copy(lockedUntil = null, failureReason = refusal ?: current.failureReason)
        return Decision(blocked, effects)
    }

    // ---- Signals ----------------------------------------------------------------------------------------------

    /** What a signal for the step at [index] does on arrival, before it is stored. */
    fun arrival(r: SagaRecord, index: Int): Arrival = when {
        // Past the step (moved on, compensating or finished): the saga never goes back, so nothing would
        // read the signal. Not stored: its value may hold personal data, kept for nothing until the purge.
        r.state != SagaState.RUNNING || r.stepIndex > index -> Arrival.TOO_LATE
        // Cancelled while a call is in flight: whatever its outcome, the saga compensates and never waits again.
        r.cancelRequestedAt != null -> Arrival.CANCELLED
        else -> Arrival.STORE
    }

    /** What becomes of a signal for the step at [index], once stored; [failed]: its application failed before. */
    fun useOfStored(r: SagaRecord, index: Int, failed: Boolean): Use {
        // WAITING, or UNKNOWN: its timeout passed and a check failed, to be tried again. No call is in flight
        // either way (a check in flight is IN_PROGRESS), so the signal answers now what the check looks up.
        val listening = r.stepStatus == StepStatus.WAITING || r.stepStatus == StepStatus.UNKNOWN
        return when {
            r.stepIndex == index && listening && failed -> Use.BLOCK
            r.stepIndex == index && listening -> Use.APPLY
            // Not waiting yet (an earlier step runs, or this one is ready, checking or stuck): kept for it.
            else -> Use.KEEP
        }
    }

    /** The signal step [step] starts waiting, until its timeout. */
    fun waiting(r: SagaRecord, step: SignalStep<*, *>, now: Instant): SagaRecord =
        r.copy(stepStatus = StepStatus.WAITING, nextAttemptAt = now + step.timeout, attempts = 0, lastError = null)

    /** The step was refused ([error]): the steps before are undone. */
    fun refused(r: SagaRecord, error: String, now: Instant): SagaRecord =
        startCompensation(r.copy(lastError = error), r.stepIndex - 1, error, now)

    /** The saga, stopped because applying its signal failed ([message]), the signal kept: once fixed, retry uses it. */
    fun blockedBy(r: SagaRecord, message: String?, now: Instant): SagaRecord =
        stuck(r, StuckReason.RECORD_FAILED, "$message. The signal is kept: fix the cause, then retry, which uses it", now)
            .copy(lockedUntil = null)

    /** Whether a claim that failed on a signal stored earlier stops the saga, whose current step is a signal step. */
    fun blocksOnStoredSignal(r: SagaRecord, now: Instant): Boolean {
        // The claim that threw may first have expired the lock of a timeout check whose process died: that
        // rolled back too, so the row is still IN_PROGRESS. Left as it is, every resume would do the same again.
        val checkDied = r.stepStatus == StepStatus.IN_PROGRESS && r.lockedUntil?.let { it <= now } == true
        val waiting = r.stepStatus == StepStatus.READY || r.stepStatus == StepStatus.UNKNOWN || checkDied
        return r.state == SagaState.RUNNING && waiting
    }

    // ---- Transitions ----------------------------------------------------------------------------------------

    /** A saga whose cancellation was requested compensates once no call of it is in flight. */
    fun cancelIfRequested(r: SagaRecord, now: Instant): SagaRecord =
        if (r.cancelRequestedAt != null && r.state == SagaState.RUNNING &&
            (r.stepStatus == StepStatus.READY || r.stepStatus == StepStatus.WAITING)
        ) {
            startCompensation(r, r.stepIndex - 1, cancelled(r), now)
        } else {
            r
        }


    /**
     * What the forward [step] of [r], stopped by [StuckReason.RESUME_FAILING], goes back to, and when it is due.
     * A compensation needs none of this: it simply runs again.
     */
    fun resumeAs(r: SagaRecord, step: SagaStep<*, *>, now: Instant, signalStored: (signal: String) -> Boolean): Pair<StepStatus, Instant> {
        // An idempotent step whose key was used is replayed (see retry): here its key is fresh, and it is called.
        if (step.idempotency != null) return StepStatus.READY to now
        return when (r.stuckFrom) {
            StepStatus.UNKNOWN -> StepStatus.UNKNOWN to now
            // Its pod died during the call, and its lock's expiry could not be recorded: checked after the settle
            // delay, which starts at the expiry, before the saga was stopped.
            StepStatus.IN_PROGRESS -> StepStatus.UNKNOWN to maxOf(now, (r.stuckAt ?: now) + config.settleDelay)
            // Its timeout still stands, passed or not. Except for what must be acted on at once: a signal received
            // while it was stuck (READY uses it), or a cancellation.
            StepStatus.WAITING ->
                if (r.cancelRequestedAt != null || step !is SignalStep<*, *> || signalStored(step.signal)) {
                    StepStatus.READY to now
                } else {
                    StepStatus.WAITING to r.nextAttemptAt
                }
            // Nothing that may have been applied, even if it was attempted (its request never left).
            StepStatus.READY -> StepStatus.READY to now
            // Stopped before stuckFrom was stored: a call that was attempted counts as possibly applied.
            StepStatus.STUCK, null ->
                (if (step !is SignalStep<*, *> && r.attempts > 0) StepStatus.UNKNOWN else StepStatus.READY) to now
        }
    }


    /**
     * Step done: move on to the next one, or complete the saga. A saga whose cancellation was requested
     * compensates instead, this step included, even when it was the last one.
     */
    fun next(r: SagaRecord, stepCount: Int, now: Instant): SagaRecord {
        if (r.cancelRequestedAt != null && r.state == SagaState.RUNNING) return startCompensation(r, r.stepIndex, cancelled(r), now)
        val index = r.stepIndex + 1
        return if (index == stepCount) {
            r.copy(
                state = SagaState.COMPLETED, stepStatus = StepStatus.READY, lastError = null, stuckReason = null, stuckAt = null, stuckFrom = null,
                keySince = null, keyGeneration = 0, checkFailures = 0, resumeFailures = 0,
            )
        } else {
            r.copy(
                stepIndex = index, stepStatus = StepStatus.READY, attempts = 0, checkFailures = 0, nextAttemptAt = now,
                lastError = null, stuckReason = null, stuckAt = null, stuckFrom = null, keySince = null, keyGeneration = 0, resumeFailures = 0,
            )
        }
    }

    /**
     * Starts undoing the completed steps, from [index] down to the first one. [reason] is kept as the
     * saga's failureReason, which nothing changes afterwards: lastError may then be overwritten by a
     * compensation that fails and is retried.
     */
    fun startCompensation(r: SagaRecord, index: Int, reason: String, now: Instant): SagaRecord {
        check(r.state == SagaState.RUNNING) { "Saga ${r.id} is already ${r.state}" }
        // A refusal kept by RECORD_FAILED stays the reason, when an operator then aborts.
        return compensateFrom(r.copy(failureReason = r.failureReason ?: reason), index, now)
    }

    fun cancelled(r: SagaRecord) = "Cancelled: ${r.cancelReason}"

    /** Undoes the completed steps, from [index] down to the first one. */
    fun compensateFrom(r: SagaRecord, index: Int, now: Instant): SagaRecord =
        if (index < 0) {
            r.copy(
                state = SagaState.FAILED, stepStatus = StepStatus.READY, checkFailures = 0, stuckReason = null, stuckAt = null, stuckFrom = null,
                keySince = null, keyGeneration = 0, resumeFailures = 0,
            )
        } else {
            r.copy(
                state = SagaState.COMPENSATING, stepIndex = index, stepStatus = StepStatus.READY, attempts = 0, checkFailures = 0,
                nextAttemptAt = now, stuckReason = null, stuckAt = null, stuckFrom = null, keySince = null, keyGeneration = 0, resumeFailures = 0,
            )
        }

    /**
     * An idempotent step that may have been applied: it stays UNKNOWN (abort is refused, cancel waits) and is
     * replayed with the same key after backoff, until a replay answers. No settle delay: the service answers
     * a replay with the first result.
     */
    fun replayLater(r: SagaRecord, error: String, now: Instant): SagaRecord =
        if (r.attempts >= config.maxAttempts) {
            stuck(r, StuckReason.TOO_MANY_ATTEMPTS, "Too many attempts: $error", now)
        } else {
            r.copy(stepStatus = StepStatus.UNKNOWN, nextAttemptAt = now + config.backoff(r.attempts), lastError = "$error; retrying with the same key")
        }

    fun retryLater(r: SagaRecord, error: String, now: Instant): SagaRecord =
        if (r.attempts >= config.maxAttempts) {
            stuck(r, StuckReason.TOO_MANY_ATTEMPTS, "Too many attempts: $error", now)
        } else {
            r.copy(stepStatus = StepStatus.READY, nextAttemptAt = now + config.backoff(r.attempts), lastError = error)
        }

    /**
     * Lock expired. Forward: the call may have left, so the outcome is unknown and will be checked. A check
     * whose lock expires counts as a failed one, up to [SagaConfig.maxAttempts].
     * Compensation: compensations are safe to repeat, so it simply runs again, up to [SagaConfig.maxAttempts].
     */
    fun lockExpired(r: SagaRecord, since: Instant, now: Instant, idempotent: () -> Boolean): SagaRecord =
        if (r.state == SagaState.RUNNING && idempotent()) {
            // Idempotent: replayed with the same key from the lock's expiry, as after an unknown outcome.
            if (r.attempts >= config.maxAttempts) {
                stuck(r, StuckReason.TOO_MANY_ATTEMPTS, "Too many attempts: lock expired during the call", now).copy(lockedUntil = null)
            } else {
                r.copy(
                    stepStatus = StepStatus.UNKNOWN, lockedUntil = null, nextAttemptAt = since,
                    lastError = "Lock expired during the call; retrying with the same key",
                )
            }
        } else if (r.state == SagaState.RUNNING && r.checkFailures >= config.maxAttempts) {
            // A check: an execution is claimed with no failed check. It never recorded an outcome, again and again.
            stuck(
                r, StuckReason.CHECK_FAILING,
                "Check keeps failing, the step may have been applied: lock expired during the check", now,
            ).copy(lockedUntil = null)
        } else if (r.state == SagaState.RUNNING) {
            r.copy(
                stepStatus = StepStatus.UNKNOWN, lockedUntil = null, nextAttemptAt = since + config.settleDelay,
                lastError = "Lock expired during the ${if (r.checkFailures > 0) "check" else "call"}",
            )
        } else if (r.attempts >= config.maxAttempts) {
            stuck(r, StuckReason.TOO_MANY_ATTEMPTS, "Too many attempts: lock expired during the compensation", now)
                .copy(lockedUntil = null)
        } else {
            r.copy(
                stepStatus = StepStatus.READY, lockedUntil = null, nextAttemptAt = since,
                lastError = "Lock expired during the compensation",
            )
        }

    /**
     * The idempotency key of a call: `<saga id>/<step>/execute` (a check shares it, so that it may replay), or
     * `/compensate`; `/<generation>` added after `retry(newKey = true)`. Derived, so every pod computes it.
     */
    fun keyOf(r: SagaRecord, stepName: String, action: Action): String {
        val base = "${r.id}/$stepName/${if (action == Action.COMPENSATE) "compensate" else "execute"}"
        return if (r.keyGeneration > 0) "$base/${r.keyGeneration}" else base
    }

    /** The saga stops for a human, for [reason]. Leaving STUCK clears both (see [next], [compensateFrom], [retry]). */
    fun stuck(r: SagaRecord, reason: StuckReason, error: String?, now: Instant): SagaRecord =
        r.copy(stepStatus = StepStatus.STUCK, stuckReason = reason, stuckAt = now, lastError = error)
}
