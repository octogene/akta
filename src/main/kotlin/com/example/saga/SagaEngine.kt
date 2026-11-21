package com.example.saga

import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.ensureActive
import kotlinx.coroutines.sync.Semaphore
import kotlinx.coroutines.sync.withPermit
import kotlinx.coroutines.withTimeout
import kotlin.coroutines.cancellation.CancellationException
import kotlin.time.Clock
import kotlin.time.Duration
import kotlin.time.Duration.Companion.days
import kotlin.time.Duration.Companion.seconds
import kotlin.time.Instant
import kotlin.uuid.ExperimentalUuidApi
import kotlin.uuid.Uuid

/**
 * Runs sagas.
 *
 * Golden rule: no external call inside a transaction. Every step follows the same cycle:
 * transaction (claim the step), call, transaction (record the outcome and decide what is next).
 */
class SagaEngine(
    private val transactions: SagaTransactions,
    private val repository: SagaRepository,
    definitions: List<SagaDefinition>,
    private val config: SagaConfig = SagaConfig(),
    private val clock: Clock = Clock.System,
    /**
     * Current OpenTelemetry trace id, stored in the saga log. Called on the caller's coroutine
     * (in [create], [run] and the manual actions), never inside [SagaTransactions.inTransaction].
     */
    private val traceId: () -> String? = { null },
) {
    private val definitions: Map<String, SagaDefinition> = definitions.associateBy { it.type }

    init {
        require(this.definitions.size == definitions.size) { "Duplicate saga types" }
    }

    // ---- Start and run ----------------------------------------------------------------------

    /**
     * Creates a saga. Call it INSIDE the transaction that makes the business change
     * (e.g. inserting the item), then call [run] once that transaction is committed.
     *
     * @throws DuplicateSagaException if an unfinished saga already exists for [key].
     */
    @OptIn(ExperimentalUuidApi::class)
    fun create(type: String, key: String, traceparent: String? = null): Uuid {
        definition(type)
        val now = currentTime()
        val record = SagaRecord(
            id = Uuid.generateV7(), type = type, key = key, state = SagaState.RUNNING,
            stepIndex = 0, stepStatus = StepStatus.READY, attempts = 0, nextAttemptAt = now,
            lockedUntil = null, lastError = null, traceparent = traceparent, createdAt = now,
        )
        repository.insert(record, now)
        log(record.id, record.stepIndex, null, SagaEvents.CREATED, null, now, trace = traceId())
        return record.id
    }

    /**
     * Moves the saga forward until it ends, blocks (waiting for a retry, an unknown outcome,
     * a human) or [deadline] passes. Returns its state: an HTTP handler can map it to 201, 422 or 202.
     */
    suspend fun run(id: Uuid, deadline: Instant, skipLocked: Boolean = false): SagaRecord {
        val trace = traceId()      // here, on the caller's coroutine: transactions may switch threads
        var cycle = if (currentTime() < deadline) {
            transactions.inTransaction { claim(id, skipLocked, trace) }
        } else {
            Cycle(saga = null, next = null)
        }
        while (true) {
            val work = cycle.next ?: break
            val outcome = perform(work)
            cycle = try {
                // Records this outcome and, in the same transaction, claims the next step if it is due.
                transactions.inTransaction { record(work, outcome, claimUntil = deadline) }
            } catch (e: Throwable) {
                if (e is CancellationException || e.isFatal()) throw e
                recordAfterFailure(work, outcome, e)
            }
        }
        // Read again only when no transaction gave us the saga (deadline already passed, row locked elsewhere).
        return cycle.saga ?: transactions.inTransaction { repository.find(id) } ?: error("Saga $id not found")
    }

    /**
     * The transaction that recorded [outcome] (and claimed the next step) was rolled back. First, record
     * the outcome alone: if only the claim of the next step failed, the call's result must not be lost.
     * If that fails too (a throwing onCompleted/onFailed, a constraint…), the saga becomes STUCK with the
     * outcome kept for a human: otherwise the step would loop through lock expiry and check, forever.
     */
    private suspend fun recordAfterFailure(work: Work, outcome: Outcome, error: Throwable): Cycle =
        try {
            Cycle(transactions.inTransaction { record(work, outcome, claimUntil = null) }.saga, next = null)
        } catch (e: Throwable) {
            if (e is CancellationException || e.isFatal()) throw e
            Cycle(transactions.inTransaction { recordFailed(work, outcome, error) }, next = null)
        }

    /**
     * Resumes due sagas. Call it periodically (e.g. every 30 seconds from a job runner).
     * Safe to run on several instances at once: each saga is claimed with SKIP LOCKED.
     *
     * Up to [concurrency] sagas run at the same time, so a slow external system does not hold up the
     * others. Keep it well below the connection pool size: a saga holds a connection only during its
     * short transactions, never during a call, but several of them may be in a transaction at once.
     *
     * A saga that throws does not stop the others: its error is returned in [ResumeResult.failures].
     */
    suspend fun resumeDue(
        limit: Int = 50,
        budgetPerSaga: Duration = 20.seconds,
        concurrency: Int = 8,
    ): ResumeResult {
        require(concurrency >= 1) { "concurrency must be at least 1" }
        // Only the types this engine knows: during a rolling deploy, old pods leave new types to new pods.
        val ids = transactions.inTransaction { repository.findDue(currentTime(), limit, definitions.keys) }
        val permits = Semaphore(concurrency)
        val failures = coroutineScope {
            ids.map { id ->
                async {
                    permits.withPermit {
                        try {
                            run(id, deadline = currentTime() + budgetPerSaga, skipLocked = true)
                            null
                        } catch (e: Throwable) {
                            // Any non-fatal error, even an Error such as a TODO() left in a step, stays with
                            // its saga: escaping, it would cancel the other sagas of the batch mid-call.
                            if (e is CancellationException || e.isFatal()) throw e
                            postpone(id, e)
                            id to e
                        }
                    }
                }
            }.awaitAll().filterNotNull().toMap()
        }
        return ResumeResult(sagas = ids.size, failures = failures)
    }

    /**
     * After [run] threw in [resumeDue] (a database error, a bug…): moves the saga back in the queue,
     * so that sagas failing on every attempt cannot fill each batch and starve the others.
     * Best effort: if this fails too, the error is already reported by [resumeDue].
     */
    private suspend fun postpone(id: Uuid, error: Throwable) {
        try {
            transactions.inTransaction {
                val r = repository.lock(id, skipLocked = true) ?: return@inTransaction
                val now = currentTime()
                val inFlight = r.stepStatus == StepStatus.IN_PROGRESS && r.lockedUntil?.let { it > now } == true
                if (r.state.isFinished || r.stepStatus == StepStatus.STUCK || inFlight) return@inTransaction
                // lastError keeps the step's own error: the resume failure goes to the log only.
                val postponed = r.copy(nextAttemptAt = maxOf(r.nextAttemptAt, now + config.resumeFailureDelay))
                repository.save(postponed, now)
                log(r.id, r.stepIndex, null, SagaEvents.RESUME_FAILED, error.toString(), now, trace = null)
            }
        } catch (e: CancellationException) {
            throw e
        } catch (_: Exception) {
            // The database is probably down: nothing more to do here.
        }
    }

    /** What [resumeDue] did: how many sagas it looked at, and the ones that threw. */
    data class ResumeResult(val sagas: Int, val failures: Map<Uuid, Throwable>)

    suspend fun find(id: Uuid): SagaRecord? = transactions.inTransaction { repository.find(id) }

    // ---- Retention --------------------------------------------------------------------------

    /**
     * Deletes the sagas that finished (COMPLETED or FAILED) more than [olderThan] ago, with their results
     * and their log. Unfinished and stuck sagas are never deleted, however old. Call it periodically
     * from the job runner (daily keeps each run small). It works in batches of [batchSize] sagas, one
     * short transaction each, so it never holds locks for long. Returns the number of sagas deleted.
     */
    suspend fun purgeFinished(olderThan: Duration = 30.days, batchSize: Int = 1000): Int {
        require(olderThan.isPositive()) { "olderThan must be positive" }
        require(batchSize >= 1) { "batchSize must be at least 1" }
        val before = currentTime() - olderThan
        var total = 0
        do {
            val deleted = transactions.inTransaction { repository.purgeFinished(before, batchSize) }
            total += deleted
        } while (deleted == batchSize)
        return total
    }

    // ---- Manual actions on a stuck saga -----------------------------------------------------

    /**
     * Retries the stuck step. Forward, this EXECUTES the step again without checking first:
     * when the saga is stuck because a check could not tell, only use it once you have confirmed
     * by hand that the step was not applied (otherwise use [markDone] or [abort]).
     */
    suspend fun retry(id: Uuid, actor: String): SagaRecord = manual(id, actor, SagaEvents.MANUAL_RETRY) { r, now ->
        require(r.stepStatus == StepStatus.STUCK) { "Saga $id is not stuck" }
        r.copy(stepStatus = StepStatus.READY, attempts = 0, checkFailures = 0, nextAttemptAt = now, lastError = null)
    }

    /** The stuck step was found done by hand (with its reference, if any): move on to the next one. */
    suspend fun markDone(id: Uuid, ref: String?, actor: String): SagaRecord =
        manual(id, actor, SagaEvents.MANUAL_DONE, detail = ref?.let { "ref=$it" }) { r, now ->
            require(r.state == SagaState.RUNNING && r.stepStatus == StepStatus.STUCK) { "Saga $id has no stuck forward step" }
            val def = definition(r.type)
            repository.saveResult(r.id, r.stepIndex, def.steps[r.stepIndex].name, ref, now)
            next(r, def, now)
        }

    /**
     * Gives up: undoes what was done. Refused while a call is in flight or its outcome is unknown.
     *
     * A stuck step may have been applied (its check could not tell): say so with [currentStepApplied],
     * after looking in the external system. When true, the current step is compensated too, and
     * receives [ref] as its own result. A step that is READY was never applied.
     */
    suspend fun abort(id: Uuid, actor: String, currentStepApplied: Boolean, ref: String? = null): SagaRecord =
        manual(id, actor, SagaEvents.MANUAL_ABORT, detail = "currentStepApplied=$currentStepApplied") { r, now ->
            require(r.state == SagaState.RUNNING) { "Saga $id is not running forward" }
            require(r.stepStatus == StepStatus.READY || r.stepStatus == StepStatus.STUCK) {
                "A call of saga $id may have been applied: wait for its outcome"
            }
            require(!currentStepApplied || r.stepStatus == StepStatus.STUCK) {
                "The current step of saga $id is READY: it was not applied"
            }
            if (currentStepApplied) {
                repository.saveResult(r.id, r.stepIndex, definition(r.type).steps[r.stepIndex].name, ref, now)
                compensateFrom(r, r.stepIndex, now)
            } else {
                compensateFrom(r, r.stepIndex - 1, now)
            }
        }

    private suspend fun manual(
        id: Uuid,
        actor: String,
        event: String,
        detail: String? = null,
        change: (SagaRecord, Instant) -> SagaRecord,
    ): SagaRecord {
        val trace = traceId()
        transactions.inTransaction {
            val r = repository.lock(id) ?: throw NoSuchElementException("Saga $id not found")
            val now = currentTime()
            val updated = change(r, now)
            repository.save(updated, now)
            // r: logged on the step the action was taken on
            log(r.id, r.stepIndex, null, event, detail, now, trace, actor)
            finishIfNeeded(r, updated)
        }
        return run(id, deadline = currentTime() + 10.seconds)
    }

    // ---- One step cycle ---------------------------------------------------------------------

    /** [event]: the first half of the logged event. Stored: never change it. */
    private enum class Action(val event: String) {
        EXECUTE("EXECUTE"),
        CHECK("CHECK"),
        COMPENSATE("COMPENSATE"),
    }

    private class Work(
        val saga: SagaRecord,
        val step: SagaStep,
        val action: Action,
        val ctx: SagaContext,
        val trace: String?,
    )

    /** [event]: the second half of the logged event (see [SagaEvents]). Stored: never change it. */
    private sealed class Outcome(val event: String, val detail: String? = null) {
        class Done(val ref: String?) : Outcome("Done", ref?.let { "ref=$it" })
        class Rejected(val error: String) : Outcome("Rejected", error)
        class NotSent(val error: String) : Outcome("NotSent", error)
        class Unknown(val error: String) : Outcome("Unknown", error)
        object NotDone : Outcome("NotDone")
        object CannotTell : Outcome("CannotTell")
        class CheckFailed(val error: String) : Outcome("CheckFailed", error)
        object Compensated : Outcome("Compensated")
        class CompensationFailed(val error: String) : Outcome("CompensationFailed", error)
    }

    /** The logged event, e.g. "EXECUTE:Done". */
    private fun eventOf(work: Work, outcome: Outcome): String = "${work.action.event}:${outcome.event}"

    /** The event with the outcome's detail, e.g. "EXECUTE:Done ref=STK-1": for LATE_RESULT_IGNORED, RECORD_FAILED. */
    private fun describe(work: Work, outcome: Outcome): String =
        listOfNotNull(eventOf(work, outcome), outcome.detail).joinToString(" ")

    /**
     * What a transaction leaves: the saga as saved ([saga], or null when it is locked elsewhere or
     * missing), and the step it claimed, if any ([next]): the call to make before the next transaction.
     */
    private class Cycle(val saga: SagaRecord?, val next: Work?)

    /** Transaction 1: claims the current step, if there is something to do now. */
    private fun claim(id: Uuid, skipLocked: Boolean, trace: String?): Cycle {
        val r = repository.lock(id, skipLocked) ?: return Cycle(saga = null, next = null)
        return claimLocked(r, trace)
    }

    /** Claims the current step of [current], whose row this transaction has locked, if it is due now. */
    private fun claimLocked(current: SagaRecord, trace: String?): Cycle {
        var r = current
        val now = currentTime()
        if (r.state.isFinished || r.stepStatus == StepStatus.STUCK) return Cycle(r, next = null)

        if (r.stepStatus == StepStatus.IN_PROGRESS) {
            val lockedUntil = checkNotNull(r.lockedUntil) { "Saga ${r.id} is IN_PROGRESS without a lock" }
            if (lockedUntil > now) return Cycle(r, next = null) // a call is in flight elsewhere
            r = lockExpired(r, lockedUntil)                      // the process died during the call
            repository.save(r, now)
            log(r.id, r.stepIndex, null, SagaEvents.LOCK_EXPIRED, r.lastError, now, trace)
        }
        if (r.stepStatus == StepStatus.STUCK || r.nextAttemptAt > now) return Cycle(r, next = null)

        val def = definition(r.type)
        val action = when {
            r.stepStatus == StepStatus.UNKNOWN -> Action.CHECK
            r.state == SagaState.RUNNING -> Action.EXECUTE
            else -> Action.COMPENSATE
        }
        val lockedUntil = now + config.callLock
        val claimed = r.copy(
            stepStatus = StepStatus.IN_PROGRESS,
            attempts = if (action == Action.CHECK) r.attempts else r.attempts + 1,
            lockedUntil = lockedUntil,
            nextAttemptAt = lockedUntil,      // the recovery job looks again when the lock expires
        )
        repository.save(claimed, now)
        val ctx = SagaContext(r.id, r.key, repository.results(r.id))
        return Cycle(claimed, Work(claimed, def.steps[r.stepIndex], action, ctx, trace))
    }

    /**
     * Outside any transaction: the external call. It is cut [recordMargin] before the step's lock
     * expires, so that its outcome is recorded while this engine still owns the step: a call still
     * running after its lock expired could land after another instance has checked it (or re-run a
     * compensation alongside it). The lock's clock started in the claim transaction, before the call.
     */
    private suspend fun perform(work: Work): Outcome {
        val lockedUntil = checkNotNull(work.saga.lockedUntil) { "Claimed saga ${work.saga.id} without a lock" }
        val budget = lockedUntil - currentTime() - recordMargin
        return try {
            withTimeout(budget) { call(work) }
        } catch (e: CancellationException) {
            // Our coroutine was cancelled: rethrow. The step stays IN_PROGRESS; its lock will expire.
            currentCoroutineContext().ensureActive()
            failure(work.action, e)           // a timeout (ours, or the step's own): a failed call
        } catch (e: Throwable) {
            if (e.isFatal()) throw e
            failure(work.action, e)           // an Error too, e.g. a TODO() left in a step
        }
    }

    /** Time kept after a call to record its outcome before the step's lock expires. */
    private val recordMargin: Duration = minOf(config.callLock / 10, 5.seconds)

    private suspend fun call(work: Work): Outcome =
        when (work.action) {
            Action.EXECUTE -> Outcome.Done(work.step.execute(work.ctx))
            Action.CHECK -> when (val c = work.step.check(work.ctx)) {
                is CheckResult.Done -> Outcome.Done(c.ref)
                CheckResult.NotDone -> Outcome.NotDone
                CheckResult.CannotTell -> Outcome.CannotTell
            }
            Action.COMPENSATE -> {
                work.step.compensate(work.ctx)
                Outcome.Compensated
            }
        }

    private fun failure(action: Action, e: Throwable): Outcome {
        val error = e.toString()
        return when (action) {
            Action.EXECUTE -> when (e) {
                is StepRejected -> Outcome.Rejected(error)
                is StepNotSent -> Outcome.NotSent(error)
                else -> Outcome.Unknown(error)
            }
            Action.CHECK -> Outcome.CheckFailed(error)
            Action.COMPENSATE -> Outcome.CompensationFailed(error)
        }
    }

    /**
     * Transaction 2: records the outcome and decides what comes next. While the row is locked, it also
     * claims the next step if it is due and [claimUntil] has not passed: one transaction instead of two.
     * With [claimUntil] null, it only records.
     */
    private fun record(work: Work, outcome: Outcome, claimUntil: Instant?): Cycle {
        val current = repository.lock(work.saga.id) ?: return Cycle(saga = null, next = null)
        val now = currentTime()
        if (!stillClaimedBy(current, work)) {
            log(work, SagaEvents.LATE_RESULT_IGNORED, describe(work, outcome), now)
            return Cycle(current, next = null)
        }
        val def = definition(current.type)
        // Any outcome but a failed check ends the series of failed checks.
        val r = current.copy(lockedUntil = null, checkFailures = 0)

        val updated = when (outcome) {
            is Outcome.Done -> {
                repository.saveResult(r.id, r.stepIndex, work.step.name, outcome.ref, now)
                next(r, def, now)
            }
            is Outcome.Rejected -> compensateFrom(r.copy(lastError = outcome.error), r.stepIndex - 1, now)
            is Outcome.NotSent -> retryLater(r, outcome.error, now)
            is Outcome.Unknown -> r.copy(
                stepStatus = StepStatus.UNKNOWN,
                nextAttemptAt = now + config.settleDelay,
                lastError = outcome.error,
            )
            // Not applied: safe to execute again, or to stop here if the step keeps failing.
            Outcome.NotDone -> if (r.attempts >= config.maxAttempts) {
                r.copy(stepStatus = StepStatus.STUCK, lastError = "Too many attempts: ${r.lastError}")
            } else {
                r.copy(stepStatus = StepStatus.READY, nextAttemptAt = now)
            }
            Outcome.CannotTell -> r.copy(
                stepStatus = StepStatus.STUCK,
                lastError = "Cannot tell whether step '${work.step.name}' was applied",
            )
            // Counted apart from attempts (a lookup outage must not use up the executions), with backoff.
            // Past maxAttempts: STUCK, as when a check cannot tell, so that alerts see it and a human
            // can decide (markDone, abort, or retry once the step is known not to be applied).
            is Outcome.CheckFailed -> {
                val failures = current.checkFailures + 1
                if (failures >= config.maxAttempts) {
                    r.copy(
                        stepStatus = StepStatus.STUCK, checkFailures = failures,
                        lastError = "Check keeps failing, step '${work.step.name}' may have been applied: ${outcome.error}",
                    )
                } else {
                    r.copy(
                        stepStatus = StepStatus.UNKNOWN, checkFailures = failures,
                        nextAttemptAt = now + maxOf(config.checkRetryDelay, config.backoff(failures)),
                        lastError = outcome.error,
                    )
                }
            }
            Outcome.Compensated -> {
                repository.markCompensated(r.id, r.stepIndex, now)
                compensateFrom(r, r.stepIndex - 1, now)
            }
            is Outcome.CompensationFailed -> retryLater(r, outcome.error, now)
        }
        repository.save(updated, now)
        // The new lastError when it changed (it may add "Too many attempts"), else the outcome's own
        // detail: a repeated error is logged every time, not only the first.
        val detail = when (outcome) {
            is Outcome.Done -> outcome.detail
            else -> updated.lastError.takeIf { it != current.lastError } ?: outcome.detail
        }
        // Logged on the step that ran, not on the one the saga moved to.
        log(work, eventOf(work, outcome), detail, now)
        finishIfNeeded(current, updated)
        return if (claimUntil != null && currentTime() < claimUntil) {
            claimLocked(updated, work.trace)
        } else {
            Cycle(updated, next = null)
        }
    }

    /**
     * Fallback transaction, after [record] was rolled back: the saga becomes STUCK on the step that
     * ran, and the outcome (with its reference) is kept in lastError and the log, for a human to apply
     * with [markDone], [retry] or [abort] once the cause is fixed.
     */
    private fun recordFailed(work: Work, outcome: Outcome, error: Throwable): SagaRecord? {
        val current = repository.lock(work.saga.id) ?: return null
        val now = currentTime()
        if (!stillClaimedBy(current, work)) {
            log(work, SagaEvents.LATE_RESULT_IGNORED, describe(work, outcome), now)
            return current
        }
        val stuck = current.copy(
            stepStatus = StepStatus.STUCK,
            lockedUntil = null,
            lastError = "Could not record ${describe(work, outcome)}: $error",
        )
        repository.save(stuck, now)
        log(work, SagaEvents.RECORD_FAILED, stuck.lastError, now)
        return stuck
    }

    /** False when the lock expired during the call and another instance took over. */
    private fun stillClaimedBy(current: SagaRecord, work: Work): Boolean =
        current.stepStatus == StepStatus.IN_PROGRESS &&
            current.stepIndex == work.saga.stepIndex &&
            current.lockedUntil == work.saga.lockedUntil

    // ---- Transitions ------------------------------------------------------------------------

    /** Step done: move on to the next one, or complete the saga. */
    private fun next(r: SagaRecord, def: SagaDefinition, now: Instant): SagaRecord {
        val index = r.stepIndex + 1
        return if (index == def.steps.size) {
            r.copy(state = SagaState.COMPLETED, stepStatus = StepStatus.READY, lastError = null)
        } else {
            r.copy(stepIndex = index, stepStatus = StepStatus.READY, attempts = 0, nextAttemptAt = now, lastError = null)
        }
    }

    /** Undoes the completed steps, from [index] down to the first one. */
    private fun compensateFrom(r: SagaRecord, index: Int, now: Instant): SagaRecord =
        if (index < 0) {
            r.copy(state = SagaState.FAILED, stepStatus = StepStatus.READY)
        } else {
            r.copy(
                state = SagaState.COMPENSATING, stepIndex = index, stepStatus = StepStatus.READY,
                attempts = 0, nextAttemptAt = now,
            )
        }

    private fun retryLater(r: SagaRecord, error: String, now: Instant): SagaRecord =
        if (r.attempts >= config.maxAttempts) {
            r.copy(stepStatus = StepStatus.STUCK, lastError = "Too many attempts: $error")
        } else {
            r.copy(stepStatus = StepStatus.READY, nextAttemptAt = now + config.backoff(r.attempts), lastError = error)
        }

    /**
     * Lock expired. Forward: the call may have left, so the outcome is unknown and will be checked.
     * Compensation: compensations are safe to repeat, so it simply runs again, up to [SagaConfig.maxAttempts].
     */
    private fun lockExpired(r: SagaRecord, since: Instant): SagaRecord =
        if (r.state == SagaState.RUNNING) {
            r.copy(
                stepStatus = StepStatus.UNKNOWN, lockedUntil = null, nextAttemptAt = since + config.settleDelay,
                lastError = "Lock expired during the call",
            )
        } else if (r.attempts >= config.maxAttempts) {
            r.copy(
                stepStatus = StepStatus.STUCK, lockedUntil = null,
                lastError = "Too many attempts: lock expired during the compensation",
            )
        } else {
            r.copy(
                stepStatus = StepStatus.READY, lockedUntil = null, nextAttemptAt = since,
                lastError = "Lock expired during the compensation",
            )
        }

    /** Business effects of the end of the saga, in the same transaction. */
    private fun finishIfNeeded(before: SagaRecord, after: SagaRecord) {
        if (before.state == after.state || !after.state.isFinished) return
        val def = definition(after.type)
        val ctx = SagaContext(after.id, after.key, repository.results(after.id))
        if (after.state == SagaState.COMPLETED) def.onCompleted(ctx) else def.onFailed(ctx)
    }

    private fun log(
        sagaId: Uuid,
        stepIndex: Int,
        stepName: String?,
        event: String,
        detail: String?,
        now: Instant,
        trace: String?,
        actor: String? = null,
    ) {
        repository.log(SagaLogEntry(sagaId, now, stepIndex, stepName, event, detail, actor, trace))
    }

    /** Logs an event on the step that [work] ran. */
    private fun log(work: Work, event: String, detail: String?, now: Instant) =
        log(work.saga.id, work.saga.stepIndex, work.step.name, event, detail, now, work.trace)

    /** Truncated to microseconds: the precision of timestamptz, and record() compares lockedUntil. */
    private fun currentTime(): Instant = clock.now().let {
        Instant.fromEpochSeconds(it.epochSeconds, it.nanosecondsOfSecond / 1_000 * 1_000)
    }

    /** Errors the JVM cannot recover from (out of memory, stack overflow): never caught. */
    private fun Throwable.isFatal() = this is VirtualMachineError

    private fun definition(type: String): SagaDefinition =
        definitions[type] ?: throw IllegalArgumentException("Unknown saga type: $type")
}
