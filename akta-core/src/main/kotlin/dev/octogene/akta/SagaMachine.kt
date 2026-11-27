package dev.octogene.akta

import dev.octogene.akta.SagaPhase.Compensating
import dev.octogene.akta.SagaPhase.Finished
import dev.octogene.akta.SagaPhase.Finishing
import dev.octogene.akta.SagaPhase.Running
import dev.octogene.akta.SagaPhase.Unfinished
import kotlin.time.Instant
import kotlin.uuid.Uuid

/*
 * The decisions of the engine, apart from what runs them: no store, clock, serializer or step code here. Each takes
 * the saga as it is, typed as a [SagaPhase], and returns the saga as it must be, and what to write besides it.
 * SagaEngine reads and writes the store (where a phase becomes a [SagaRecord] row, and back), calls the steps and runs
 * the hooks, in its transactions.
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
     * A signal step's timeout check found the signal's value, encoded in [done]. [SagaMachine.decide] stores it as the
     * step's signal, and the step's onSignal decides on it at the next claim, as for a signal delivered.
     */
    class Found(val done: Done) : Outcome("Done", done.detail)
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
    /** Its step is listening: woken, due at once, to consume it (onSignal decides then). */
    WAKE,
    /** Its step does not listen yet: kept for it. */
    KEEP,
}

/** A decision: the saga as it must be, what to write besides it, and what to log about it ([detail]). */
internal class Decision(val saga: SagaPhase, val effects: List<Effect>, val detail: String? = null)

/** A move on a saga that calls nothing: see [SagaMachine.move]. Each but [Rest] and [Act] is saved and logged. */
internal sealed interface Move {
    /** Nothing to do now: finished, stuck, a call in flight elsewhere, or not due. */
    data class Rest(val saga: SagaPhase) : Move
    /** The step is to be acted on (its data read, then waited for, consumed or called): [saga] is the saga so far. */
    data class Act(val saga: Unfinished) : Move
    /** The lock of a call expired: [saga] says what follows (logged LOCK_EXPIRED, with its lastError). */
    data class LockExpired(val saga: Unfinished) : Move
    /** A cancellation that waited for a call to end: compensating now, or FAILED (logged CANCELLED). */
    data class Cancelled(val saga: SagaPhase) : Move
    /** A step with nothing to undo, passed over (logged NOTHING_TO_UNDO). */
    data class NothingToUndo(val saga: SagaPhase, val effects: List<Effect>) : Move
    /** A signal step's timeout passed and it has no check: stuck (logged WAIT_TIMED_OUT). The moves end. */
    data class NoSignal(val saga: Unfinished) : Move
    /** A finishing saga, due: its hook is to run, in a transaction of its own. The moves end. */
    data class Finish(val saga: Finishing) : Move
}

/** The call claimed by [SagaMachine.claim], or why none is. */
internal sealed interface Claim {
    /** [saga]'s call is in flight; [replay]: an idempotent step whose key was used, an earlier attempt may be applied. */
    data class Call(val saga: Unfinished, val action: Action, val replay: Boolean) : Claim
    /** The step's idempotency key may be forgotten by its service: stuck (logged KEY_EXPIRED), no call. */
    data class KeyExpired(val saga: Unfinished) : Claim
}

/**
 * [definitions]: the definition of a saga type, null for a type this engine does not know. Read for a saga's current
 * step, and for its hooks: a saga that has the hook of its end goes through FINISHING, which runs it in a transaction
 * of its own; one without it ends at once.
 */
internal class SagaMachine(private val config: SagaConfig, private val definitions: (type: String) -> SagaDefinition<*>?) {
    private fun definition(type: String): SagaDefinition<*> =
        definitions(type) ?: throw IllegalArgumentException("Unknown saga type: $type")

    /** The step [r] is on. Throws for a type this engine does not know, or a step its definition no longer has. */
    private fun stepOf(r: SagaPhase): SagaStep<*, *> = definition(r.type).steps[r.stepIndex]

    /** A type this engine does not know is taken to have both hooks: it then waits in FINISHING for an engine that does. */
    private fun hooks(type: String): Hooks = definitions(type)?.hooks ?: Hooks(completed = true, failed = true)

    /** A new saga, due at once on its first step. */
    fun created(id: Uuid, type: String, key: String, input: String?, traceparent: String?, now: Instant): Running =
        Running(
            Identity(id, type, key, input, traceparent, createdAt = now), stepIndex = 0, step = StepState.Ready(now),
            attempts = 0, checkFailures = 0, resumeFailures = 0, key = KeyState(since = null, generation = 0),
            cancellation = null, lastError = null, refusal = null,
        )

    /** Whether running the saga now could do anything: not finished, not stuck, and due. */
    fun canMoveOn(r: SagaPhase, now: Instant): Boolean =
        r is Unfinished && r.step !is StepState.Stuck && r.due <= now

    /**
     * False when the call claimed as [claimed] is no longer the one in flight: its lock expired and another instance
     * took over, or its outcome was already recorded and another call claimed (a commit reported as failed). The same
     * step and lock time are not enough: the state and the call must be the same too, or the undo of a step claimed
     * in the same instant would be taken for its execution. True only for an unfinished saga.
     */
    fun stillClaimed(current: SagaPhase, claimed: Unfinished): Boolean {
        val inFlight = (current as? Unfinished)?.step as? StepState.InFlight ?: return false
        val ours = claimed.step as? StepState.InFlight ?: return false
        return current.state == claimed.state &&
            inFlight.call == ours.call &&
            current.stepIndex == claimed.stepIndex &&
            inFlight.lockedUntil == ours.lockedUntil
    }

    /**
     * The saga after its call, before its outcome decides: the lock released. Any outcome but a failed check ends the
     * series of failed checks. Its step is READY here only to be a valid phase: every outcome sets what it becomes.
     */
    fun released(current: Unfinished): Unfinished =
        current.with(step = StepState.Ready(current.due), checkFailures = 0)

    /**
     * What a call's [outcome] makes of the saga [r] (its lock released), whose current step is [step] of [stepCount]:
     * the saga, and what to write besides it, in the same transaction. [checkFailures] is the count before this record
     * reset it. [replay]: an earlier attempt may have been applied; [unclaimed]: the saga before the claim.
     */
    fun decide(
        r: Unfinished,
        checkFailures: Int,
        outcome: Outcome,
        step: SagaStep<*, *>,
        stepCount: Int,
        replay: Boolean,
        unclaimed: Unfinished,
        now: Instant,
        signalStored: (signal: String) -> Boolean = { false },
    ): Decision {
        val effects = mutableListOf<Effect>()
        // A signal step's timeout check: what it found is stored as the signal (one received first wins), and the step
        // is due at once, to consume it in a claim of its own, through onSignal. If it found nothing, cannot tell, or
        // failed, a signal received meanwhile answers instead: the event did happen, whatever the lookup said. A failed
        // check is not tried again, and does not count towards CHECK_FAILING.
        if (step is SignalStep<*, *>) {
            if (outcome is Outcome.Found) {
                effects += Effect.SaveSignal(step.signal, checkNotNull(outcome.done.output) { "Saga ${r.id}: a signal has a value" })
                return Decision(r.with(step = StepState.Ready(now)), effects)
            }
            val foundNothing = outcome is Outcome.Rejected && outcome.noSignalFound
            val answered = foundNothing || outcome == Outcome.CannotTell || outcome is Outcome.CheckFailed
            if (answered && signalStored(step.signal)) {
                return Decision(r.with(step = StepState.Ready(now)), effects)
            }
        }
        val saga = when (outcome) {
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
            // A replay that never left says nothing of the earlier attempt, which may be applied: still REPLAYING.
            // A first attempt that never left did not use the key: its window has not started (keySince cleared).
            is Outcome.NotSent -> if (replay) {
                replayLater(r, "${outcome.error} (an earlier attempt may be applied)", now)
            } else {
                retryLater(r.with(key = KeyState(since = null, generation = r.key.generation)), outcome.error, now)
            }
            is Outcome.Unknown -> when {
                step.idempotency == null -> r.with(step = StepState.Checking(now + config.settleDelay), lastError = outcome.error)
                else -> replayLater(r, outcome.error, now)
            }
            // Not applied: safe to execute again, or to stop here if the step keeps failing.
            Outcome.NotDone -> if (r.attempts >= config.maxAttempts) {
                stuck(r, StuckReason.TOO_MANY_ATTEMPTS, "Too many attempts: ${r.lastError}", now)
            } else {
                r.with(step = StepState.Ready(now))
            }
            Outcome.CannotTell -> stuck(r, StuckReason.CANNOT_TELL, "Cannot tell whether step '${step.name}' was applied", now)
            // Counted apart from attempts (a lookup outage must not use up the executions), with backoff.
            // Past maxAttempts: STUCK, as when a check cannot tell, so that alerts see it and a human
            // can decide (markDone, abort, or retry once the step is known not to be applied).
            is Outcome.CheckFailed -> {
                val failures = checkFailures                   // this check was counted when claimed
                if (failures >= config.maxAttempts) {
                    stuck(
                        r.with(checkFailures = failures), StuckReason.CHECK_FAILING,
                        "Check keeps failing, step '${step.name}' may have been applied: ${outcome.error}", now,
                    )
                } else {
                    r.with(
                        step = StepState.Checking(now + maxOf(config.checkRetryDelay, config.backoff(failures))),
                        lastError = outcome.error, checkFailures = failures,
                    )
                }
            }
            Outcome.Compensated -> {
                effects += Effect.MarkCompensated
                previousUndo(r, now)
            }
            is Outcome.CompensationFailed -> retryLater(r, outcome.error, now)
            is Outcome.Found -> error("Saga ${r.id}: a value found is only for a signal step")
            // What the claim changed goes back, due at once: no attempt used, the idempotency key not used.
            // The rest is kept: a cancellation requested meanwhile, for one.
            is Outcome.NotCalled -> {
                val step = when (unclaimed.step) {
                    is StepState.Ready -> StepState.Ready(now)
                    is StepState.Checking -> StepState.Checking(now)
                    is StepState.Replaying -> StepState.Replaying(now)
                    else -> error("Saga ${r.id}: a call is claimed from READY, CHECKING or REPLAYING, not ${unclaimed.step.status}")
                }
                r.with(step = step, attempts = unclaimed.attempts, checkFailures = unclaimed.checkFailures, key = unclaimed.key)
            }
        }
        return Decision(saga, effects)
    }

    // ---- Manual actions: each checks the saga can take it (IllegalArgumentException), then decides -----------

    /** See [SagaEngine.retry]. [signalStored]: whether a signal of a name was received. */
    fun retry(r: SagaPhase, newKey: Boolean, now: Instant, signalStored: (signal: String) -> Boolean): Decision {
        val p = r as? Unfinished
        val stuck = p?.step as? StepState.Stuck
        require(p != null && stuck != null) { "Saga ${r.id} is not stuck" }
        require(!newKey || p !is Finishing) { "Saga ${r.id} is finishing: it has no call, nor a key to renew" }
        require(newKey || stuck.reason != StuckReason.KEY_EXPIRED) {
            "The idempotency key of saga ${r.id} may be forgotten by its service: confirm the step was not applied, " +
                "then retry with newKey = true (or markDone, or abort)"
        }
        val generation = if (newKey) p.key.generation + 1 else p.key.generation
        val step = if (p is Running) stepOf(p) else null
        // An idempotent step whose key was used may be applied: it is replayed, and stays REPLAYING meanwhile.
        val replay = step?.idempotency != null && !newKey && p.key.since != null
        // Stopped by resume failures, not by the step: it resumes as it was.
        val resuming = stuck.reason == StuckReason.RESUME_FAILING
        val resumed = p.with(
            step = when {
                replay -> StepState.Replaying(now)
                resuming && step != null -> resumeAs(p, stuck, step, now, signalStored)
                else -> StepState.Ready(now)
            },
            attempts = if (resuming) p.attempts else 0, checkFailures = if (resuming) p.checkFailures else 0,
            resumeFailures = 0, key = KeyState(since = if (newKey) null else p.key.since, generation = generation), lastError = null,
        )
        return Decision(
            // Forward, a refusal kept by RECORD_FAILED is dropped: the step is called again.
            if (resumed is Running) resumed.copy(refusal = null) else resumed,
            emptyList(),
            detail = if (newKey) "newKey: generation $generation" else null,
        )
    }

    /** See [SagaEngine.markDone], which then saves the output and moves on with [next]. Returns [r], running. */
    fun checkMarkDone(r: SagaPhase): Running {
        require(r is Running && r.step is StepState.Stuck) { "Saga ${r.id} has no stuck forward step" }
        // Set on a saga still running only when its step's refusal could not be recorded (RECORD_FAILED).
        require(r.refusal == null) {
            "The stuck step of saga ${r.id} was refused (${r.refusal}): it cannot be marked done. " +
                "abort(currentStepApplied = false) gives up; retry calls it again"
        }
        return r
    }

    /** markDone on a stuck signal step, whose signal is now stored: due at once, to consume it through onSignal. */
    fun signalByHand(r: Running, now: Instant): Unfinished =
        r.with(step = StepState.Ready(now), checkFailures = 0, resumeFailures = 0, lastError = null)

    /** See [SagaEngine.abort], which saves the step's output in between, when it was applied. Returns [r], unfinished. */
    fun checkAbort(r: SagaPhase, currentStepApplied: Boolean): Unfinished {
        if (r is Finishing) {
            // Its onCompleted cannot run: every step is done, and all are undone. currentStepApplied changes nothing.
            require(r.failureReason == null && r.step is StepState.Stuck) {
                "Saga ${r.id} is finishing: only a stuck onCompleted can be aborted"
            }
            return r
        }
        require(r is Running) { "Saga ${r.id} is not running forward" }
        require(r.step is StepState.Ready || r.step is StepState.Waiting || r.step is StepState.Stuck) {
            "A call of saga ${r.id} may have been applied: wait for its outcome"
        }
        require(!currentStepApplied || r.step is StepState.Stuck) {
            "The current step of saga ${r.id} is ${r.step.status}: it was not applied"
        }
        // As in markDone: a refused step has nothing to undo, and its compensation must not run.
        require(!currentStepApplied || r.refusal == null) {
            "The stuck step of saga ${r.id} was refused (${r.refusal}), nothing applied: abort with currentStepApplied = false"
        }
        return r
    }

    fun abort(r: Unfinished, currentStepApplied: Boolean, actor: String, now: Instant): SagaPhase =
        if (r is Finishing) {
            compensateFrom(r, r.stepIndex, now, failureReason = "Aborted by $actor")          // every step, the last included
        } else {
            startCompensation(r, if (currentStepApplied) r.stepIndex else r.stepIndex - 1, "Aborted by $actor", now)
        }

    /** See [SagaEngine.markCompensated]. */
    fun markCompensated(r: SagaPhase, now: Instant): Decision {
        require(r is Compensating && r.step is StepState.Stuck) { "Saga ${r.id} has no stuck compensation" }
        return Decision(previousUndo(r, now), listOf(Effect.MarkCompensated))
    }

    /** See [SagaEngine.cancel]. */
    fun cancel(r: SagaPhase, reason: String, now: Instant): Decision {
        require(r !is Finished) { "Saga ${r.id} is finished: undoing it is a new saga" }
        require(r !is Finishing) { "Saga ${r.id} is finishing, its steps all done or undone: undoing it is a new saga" }
        // Already compensating, for another reason: nothing changes, and the history says so.
        if (r !is Running) return Decision(r, emptyList(), detail = "$reason (already compensating: nothing changed)")
        // The first cancellation counts: a second one is only logged.
        val requested = if (r.cancellation != null) r else r.copy(cancellation = Cancellation(now, reason))
        return Decision(cancelIfRequested(requested, now), emptyList(), detail = reason)
    }

    // ---- Claims -------------------------------------------------------------------------------------------

    /**
     * The next move on the saga [r], before its step's data is read: [Move.Rest], [Move.Act], [Move.Finish] and
     * [Move.NoSignal] end the moves; the others are saved, then the next move is asked. The definition is only read
     * once it is needed: a saga at rest, or a compensation whose lock expired, never throws for an unknown type.
     */
    fun move(r: SagaPhase, now: Instant): Move {
        if (r !is Unfinished || r.step is StepState.Stuck) return Move.Rest(r)
        // Its steps are done: only its hook is left, in a transaction of its own.
        if (r is Finishing) return if (r.due <= now) Move.Finish(r) else Move.Rest(r)
        val inFlight = r.step
        if (inFlight is StepState.InFlight) {
            if (inFlight.lockedUntil > now) return Move.Rest(r)                // a call is in flight elsewhere
            // Saved and logged; then the next move, at the same time: it rests if the saga is stuck or not due yet.
            return Move.LockExpired(lockExpired(r, now) { stepOf(r).idempotency != null })
        }
        if (r.due > now) return Move.Rest(r)
        // Every move from here saves the saga: if this transaction commits, the saga could be run, which ends a
        // series of resume failures.
        val saga = if (r.resumeFailures != 0) r.with(resumeFailures = 0) else r
        definition(saga.type)                                           // an unknown type throws here, once due
        // A cancellation that waited for a call to end: compensate now that none is in flight. Before the data is
        // decoded: the claim decodes it, in this same transaction.
        val cancelled = cancelIfRequested(saga, now)
        if (cancelled !== saga) return Move.Cancelled(cancelled)
        val step = stepOf(saga)
        // A step with nothing to undo: marked undone at once, no call, and on to the previous one, all in this
        // transaction (usually the one that recorded the refusal or the previous undo).
        if (saga is Compensating && !step.compensates) {
            return Move.NothingToUndo(previousUndo(saga, now), listOf(Effect.MarkCompensated))
        }
        // A signal step's timeout passed without its signal, and nothing will check: stuck, no data needed.
        if (step is SignalStep<*, *> && saga is Running && saga.step is StepState.Waiting && !step.checksOnTimeout) {
            return Move.NoSignal(stuck(saga, StuckReason.NO_SIGNAL, "No signal '${step.signal}' before the timeout", now))
        }
        return Move.Act(saga)
    }

    /** A signal step whose timeout passed: its check decides. */
    fun timedOut(r: Unfinished, step: SignalStep<*, *>): Unfinished =
        r.with(step = StepState.Checking(r.due), lastError = "No signal '${step.signal}' before the timeout")

    /** The call of [step] that the saga [r] is due for, claimed: or the step stops, its key possibly forgotten. */
    fun claim(r: Unfinished, step: SagaStep<*, *>, now: Instant): Claim {
        val idempotency = step.idempotency.takeIf { r is Running }
        val action = when {
            // An idempotent step's unknown outcome is replayed with the same key, never checked.
            r.step.outcomeUnknown && idempotency == null -> Action.CHECK
            r is Running -> Action.EXECUTE
            else -> Action.COMPENSATE
        }
        var keySince = r.key.since
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
        val call = when (action) {
            Action.EXECUTE -> if (replay) CallAction.REPLAY else CallAction.EXECUTE
            Action.CHECK -> CallAction.CHECK
            Action.COMPENSATE -> CallAction.COMPENSATE
        }
        val claimed = r.with(
            // Due when the lock expires: the recovery job looks again then.
            step = StepState.InFlight(call, lockedUntil, due = lockedUntil),
            attempts = if (action == Action.CHECK) r.attempts else r.attempts + 1,
            // Counted now, as an attempt is, and reset by any outcome but a failure: a check that ends without
            // recording one (its pod died, the record failed) is limited too.
            checkFailures = if (action == Action.CHECK) r.checkFailures + 1 else r.checkFailures,
            key = KeyState(keySince, r.key.generation),
        )
        return Claim.Call(claimed, action, replay)
    }

    // ---- Failures around a run ------------------------------------------------------------------------------

    /**
     * After a run threw (resumeDue, or the run after a manual action): the saga postponed, or stopped after too
     * many failures in a row; null to leave it as it is (finished, stuck, or a call in flight).
     */
    fun postpone(r: SagaPhase, error: String, now: Instant): Unfinished? {
        if (r !is Unfinished) return null
        val step = r.step
        if (step is StepState.Stuck || (step is StepState.InFlight && step.lockedUntil > now)) return null
        val failures = r.resumeFailures + 1
        return if (failures >= config.maxAttempts) {
            // The step is left as it was (attempts, key): retry resumes it, see there. No definition is
            // read here: an unknown type or a step that no longer exists may be the very cause.
            r.with(
                step = StepState.Stuck(StuckReason.RESUME_FAILING, at = now, from = step.status, due = r.due),
                resumeFailures = failures,
                lastError = "Could not run the saga, $failures times in a row: $error. Fix the cause, then retry: " +
                    "the step resumes as it was, checked first if it may have been called",
            )
        } else {
            // lastError keeps the step's own error: the resume failure goes to the log only.
            r.with(step = step.dueAt(maxOf(r.due, now + config.resumeFailureDelay)), resumeFailures = failures)
        }
    }

    /**
     * The record of [outcome] (described as [described]) failed, then the outcome alone too, with [error]: the saga
     * [current] stops (RECORD_FAILED) on the [step] that ran, keeping what the outcome established. [onSignalFailed]:
     * the error is onSignal's. [signalStored]: whether a signal of a name was received.
     */
    fun recordFailed(
        current: Unfinished,
        step: SagaStep<*, *>,
        action: Action,
        outcome: Outcome,
        described: String,
        error: Throwable,
        onSignalFailed: Boolean,
        now: Instant,
        signalStored: (signal: String) -> Boolean,
    ): Decision {
        // A signal step's timeout check, whose value or signal is what failed to be recorded (onSignal threw on
        // it): the value found is stored as the step's signal (a real one stored first wins), so that
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
        val stopped = stuck(current, StuckReason.RECORD_FAILED, "Could not record $described: $error. $then", now)
        val blocked = if (refusal != null && stopped is Running) stopped.copy(refusal = refusal) else stopped
        return Decision(blocked, effects)
    }

    // ---- Signals ----------------------------------------------------------------------------------------------

    /** What a signal for the step at [index] does on arrival, before it is stored. */
    fun arrival(r: SagaPhase, index: Int): Arrival = when {
        // Past the step (moved on, compensating or finished): the saga never goes back, so nothing would
        // read the signal. Not stored: its value may hold personal data, kept for nothing until the purge.
        r !is Running || r.stepIndex > index -> Arrival.TOO_LATE
        // Cancelled while a call is in flight: whatever its outcome, the saga compensates and never waits again.
        r.cancellation != null -> Arrival.CANCELLED
        else -> Arrival.STORE
    }

    /** What becomes of a signal for the step at [index], once stored. */
    fun useOfStored(r: SagaPhase, index: Int): Use {
        // WAITING, or CHECKING: its timeout passed and a check failed, to be tried again. No call is in flight
        // either way (a check in flight is IN_PROGRESS), so the signal answers now what the check looks up.
        val step = (r as? Unfinished)?.step
        val listening = step is StepState.Waiting || step?.outcomeUnknown == true
        return when {
            r.stepIndex == index && listening -> Use.WAKE
            // Not waiting yet (an earlier step runs, or this one is ready, checking or stuck): kept for it.
            else -> Use.KEEP
        }
    }

    /** A signal arrived for the step that waits for it ([useOfStored] said WAKE): due at once, to consume it. */
    fun woken(r: SagaPhase, now: Instant): Unfinished {
        check(r is Running) { "Saga ${r.id} is ${r.state}: only a running saga waits for a signal" }
        return r.with(step = StepState.Ready(now))
    }

    /** The signal step [step] starts waiting, until its timeout. */
    fun waiting(r: Unfinished, step: SignalStep<*, *>, now: Instant): Unfinished =
        r.with(step = StepState.Waiting(now + step.timeout), lastError = null, attempts = 0)

    /** The step was refused ([error]): the steps before are undone. */
    fun refused(r: Unfinished, error: String, now: Instant): SagaPhase =
        startCompensation(r.with(lastError = error), r.stepIndex - 1, error, now)

    /**
     * A claim of the saga [r], whose current step is a signal step, failed because onSignal threw ([message]) on a
     * signal stored earlier: the saga stopped, the signal kept, for retry to use once fixed. Null when it is not
     * waiting for that signal any more, and is left as it is.
     */
    fun blockedByStoredSignal(r: SagaPhase, message: String?, now: Instant): Unfinished? {
        if (r !is Running) return null
        // The claim that threw may first have expired the lock of a timeout check whose process died: that
        // rolled back too, so the row is still IN_PROGRESS. Left as it is, every resume would do the same again.
        val step = r.step
        val checkDied = step is StepState.InFlight && step.lockedUntil <= now
        if (step !is StepState.Ready && !step.outcomeUnknown && !checkDied) return null
        return stuck(r, StuckReason.RECORD_FAILED, "$message. The signal is kept: fix the cause, then retry, which uses it", now)
    }

    // ---- Finishing ---------------------------------------------------------------------------------------------

    /** A finishing saga whose hook is about to run: COMPLETED or FAILED, as the hook will see it. */
    fun finished(p: Finishing): Finished =
        Finished(
            p.identity, p.stepIndex, p.attempts, lastDue = p.due, p.cancellation,
            lastError = if (p.failureReason == null) null else p.lastError, failureReason = p.failureReason,
        )

    /** The hook of the finishing saga [p] threw [error]: run again after backoff, or stopped after too many failures. */
    fun hookFailed(p: Finishing, error: String, now: Instant): Unfinished {
        val hook = if (p.failureReason == null) "onCompleted" else "onFailed"
        val attempts = p.attempts + 1
        return if (attempts >= config.maxAttempts) {
            p.with(
                step = StepState.Stuck(StuckReason.HOOK_FAILED, at = now, from = null, due = p.due), attempts = attempts,
                lastError = "$hook threw, $attempts times in a row: $error. Every step is " +
                    (if (p.failureReason == null) "done: fix it, then retry, or abort to undo them" else "undone: fix it, then retry"),
            )
        } else {
            p.with(step = StepState.Ready(now + config.backoff(attempts)), attempts = attempts, lastError = "$hook threw: $error")
        }
    }

    // ---- Transitions ----------------------------------------------------------------------------------------

    /** A saga whose cancellation was requested compensates once no call of it is in flight. */
    fun cancelIfRequested(r: SagaPhase, now: Instant): SagaPhase =
        if (r is Running && r.cancellation != null && (r.step is StepState.Ready || r.step is StepState.Waiting)) {
            startCompensation(r, r.stepIndex - 1, cancelled(r), now)
        } else {
            r
        }

    /**
     * What the forward [step] of [r], stopped by [StuckReason.RESUME_FAILING] as [stuck], goes back to, and when it is
     * due. A compensation needs none of this: it simply runs again.
     */
    private fun resumeAs(
        r: Unfinished,
        stuck: StepState.Stuck,
        step: SagaStep<*, *>,
        now: Instant,
        signalStored: (signal: String) -> Boolean,
    ): StepState {
        // An idempotent step whose key was used is replayed (see retry): here its key is fresh, and it is called.
        if (step.idempotency != null) return StepState.Ready(now)
        return when (stuck.from) {
            StepStatus.CHECKING -> StepState.Checking(now)
            StepStatus.REPLAYING -> StepState.Replaying(now)
            // Its pod died during the call, and its lock's expiry could not be recorded: checked after the settle
            // delay, which starts at the expiry, before the saga was stopped.
            StepStatus.IN_PROGRESS -> StepState.Checking(maxOf(now, stuck.at + config.settleDelay))
            // Its timeout still stands, passed or not. Except for what must be acted on at once: a signal received
            // while it was stuck (READY uses it), or a cancellation.
            StepStatus.WAITING ->
                if (r.cancellation != null || step !is SignalStep<*, *> || signalStored(step.signal)) {
                    StepState.Ready(now)
                } else {
                    StepState.Waiting(r.due)
                }
            // Nothing that may have been applied, even if it was attempted (its request never left).
            StepStatus.READY -> StepState.Ready(now)
            // Stopped before stuckFrom was stored: a call that was attempted counts as possibly applied.
            StepStatus.STUCK, null ->
                if (step !is SignalStep<*, *> && r.attempts > 0) StepState.Checking(now) else StepState.Ready(now)
        }
    }

    /**
     * Step done: move on to the next one, or complete the saga. A saga whose cancellation was requested
     * compensates instead, this step included, even when it was the last one.
     */
    fun next(r: Unfinished, stepCount: Int, now: Instant): SagaPhase {
        check(r is Running) { "Saga ${r.id} is ${r.state}: only a running saga moves on" }
        if (r.cancellation != null) return startCompensation(r, r.stepIndex, cancelled(r), now)
        val index = r.stepIndex + 1
        return if (index == stepCount && hooks(r.type).completed) {
            Finishing(r.identity, r.stepIndex, StepState.Ready(now), attempts = 0, resumeFailures = 0, null, null, failureReason = null)
        } else if (index == stepCount) {
            Finished(r.identity, r.stepIndex, r.attempts, lastDue = r.due, cancellation = null, lastError = null, failureReason = null)
        } else {
            Running(
                r.identity, index, StepState.Ready(now), attempts = 0, checkFailures = 0, resumeFailures = 0,
                key = KeyState(since = null, generation = 0), cancellation = null, lastError = null, refusal = null,
            )
        }
    }

    /**
     * Starts undoing the completed steps, from [index] down to the first one. [reason] is kept as the
     * saga's failureReason, which nothing changes afterwards: lastError may then be overwritten by a
     * compensation that fails and is retried.
     */
    fun startCompensation(r: Unfinished, index: Int, reason: String, now: Instant): SagaPhase {
        check(r is Running) { "Saga ${r.id} is already ${r.state}" }
        // A refusal kept by RECORD_FAILED stays the reason, when an operator then aborts.
        return compensateFrom(r, index, now, failureReason = r.refusal ?: reason)
    }

    fun cancelled(r: SagaPhase) = "Cancelled: ${r.cancellation?.reason}"

    /** The current step of the compensating saga [r] is undone: on to the previous one. */
    private fun previousUndo(r: Unfinished, now: Instant): SagaPhase {
        check(r is Compensating) { "Saga ${r.id} is ${r.state}: only a compensating saga undoes its steps" }
        return compensateFrom(r, r.stepIndex - 1, now, r.failureReason)
    }

    /**
     * Undoes the completed steps, from [index] down to the first one, because of [failureReason]. Nothing to undo:
     * FINISHING if the saga has an onFailed hook, else FAILED, keeping what it was doing last and its last error.
     */
    private fun compensateFrom(p: Unfinished, index: Int, now: Instant, failureReason: String): SagaPhase =
        if (index < 0 && hooks(p.type).failed) {
            Finishing(p.identity, p.stepIndex, StepState.Ready(now), attempts = 0, resumeFailures = 0, p.cancellation, p.lastError, failureReason)
        } else if (index < 0) {
            Finished(p.identity, p.stepIndex, p.attempts, lastDue = p.due, p.cancellation, p.lastError, failureReason)
        } else {
            Compensating(
                p.identity, index, StepState.Ready(now), attempts = 0, checkFailures = 0, resumeFailures = 0,
                key = KeyState(since = null, generation = 0), p.cancellation, p.lastError, failureReason,
            )
        }

    /**
     * An idempotent step that may have been applied: it stays REPLAYING (abort is refused, cancel waits) and is
     * replayed with the same key after backoff, until a replay answers. No settle delay: the service answers
     * a replay with the first result.
     */
    private fun replayLater(r: Unfinished, error: String, now: Instant): Unfinished =
        if (r.attempts >= config.maxAttempts) {
            stuck(r, StuckReason.TOO_MANY_ATTEMPTS, "Too many attempts: $error", now)
        } else {
            r.with(step = StepState.Replaying(now + config.backoff(r.attempts)), lastError = "$error; retrying with the same key")
        }

    private fun retryLater(r: Unfinished, error: String, now: Instant): Unfinished =
        if (r.attempts >= config.maxAttempts) {
            stuck(r, StuckReason.TOO_MANY_ATTEMPTS, "Too many attempts: $error", now)
        } else {
            r.with(step = StepState.Ready(now + config.backoff(r.attempts)), lastError = error)
        }

    /**
     * The lock of [r]'s call expired. Forward: the call may have left, so the outcome is unknown and will be checked.
     * A check whose lock expires counts as a failed one, up to [SagaConfig.maxAttempts].
     * Compensation: compensations are safe to repeat, so it simply runs again, up to [SagaConfig.maxAttempts].
     */
    fun lockExpired(r: Unfinished, now: Instant, idempotent: () -> Boolean): Unfinished {
        val call = r.step as? StepState.InFlight ?: error("Saga ${r.id} is ${r.step.status}: no call is in flight")
        val since = call.lockedUntil
        return if (r is Running && idempotent()) {
            // Idempotent: replayed with the same key from the lock's expiry, as after an unknown outcome.
            if (r.attempts >= config.maxAttempts) {
                stuck(r, StuckReason.TOO_MANY_ATTEMPTS, "Too many attempts: lock expired during the call", now)
            } else {
                r.with(step = StepState.Replaying(since), lastError = "Lock expired during the call; retrying with the same key")
            }
        } else if (call.call == CallAction.CHECK && r.checkFailures >= config.maxAttempts) {
            // A check that never recorded an outcome, again and again.
            stuck(
                r, StuckReason.CHECK_FAILING,
                "Check keeps failing, the step may have been applied: lock expired during the check", now,
            )
        } else if (r is Running) {
            r.with(
                step = StepState.Checking(since + config.settleDelay),
                lastError = "Lock expired during the ${if (call.call == CallAction.CHECK) "check" else "call"}",
            )
        } else if (r.attempts >= config.maxAttempts) {
            stuck(r, StuckReason.TOO_MANY_ATTEMPTS, "Too many attempts: lock expired during the compensation", now)
        } else {
            r.with(step = StepState.Ready(since), lastError = "Lock expired during the compensation")
        }
    }

    /**
     * The idempotency key of a call: `<saga id>/<step>/execute` (a check shares it, so that it may replay), or
     * `/compensate`; `/<generation>` added after `retry(newKey = true)`. Derived, so every pod computes it.
     */
    fun keyOf(r: Unfinished, stepName: String, action: Action): String {
        val base = "${r.id}/$stepName/${if (action == Action.COMPENSATE) "compensate" else "execute"}"
        return if (r.key.generation > 0) "$base/${r.key.generation}" else base
    }

    /** The saga stops for a human, for [reason], its lock released if it had one. Leaving STUCK clears it all (see [next], [compensateFrom], [retry]). */
    fun stuck(r: Unfinished, reason: StuckReason, error: String?, now: Instant): Unfinished =
        r.with(step = StepState.Stuck(reason, at = now, from = null, due = r.due), lastError = error)
}
