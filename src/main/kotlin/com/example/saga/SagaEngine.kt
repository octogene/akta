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
import kotlin.reflect.KType
import kotlin.reflect.typeOf
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
    definitions: List<SagaDefinition<*>>,
    /** The format of every saga input and step output, e.g. [KotlinxJsonSagaSerializer]. */
    private val serializer: SagaSerializer,
    private val config: SagaConfig = SagaConfig(),
    private val clock: Clock = Clock.System,
    /**
     * Current OpenTelemetry trace id, stored in the saga log. Called on the caller's coroutine
     * (in [create], [run] and the manual actions), never inside [SagaTransactions.inTransaction].
     */
    private val traceId: () -> String? = { null },
) {
    @Suppress("UNCHECKED_CAST")
    private val definitions: Map<String, AnyDefinition> = definitions.associateBy { it.type } as Map<String, AnyDefinition>

    init {
        require(this.definitions.size == definitions.size) { "Duplicate saga types" }
        // Every declared type must be serializable: a missing serializer stops the engine here, at startup,
        // never as a saga stuck in production.
        for (def in definitions) {
            def.inputType?.let { type ->
                require(serializer.supports(type)) { "Saga ${def.type}: cannot serialize its input $type (missing @Serializable?)" }
            }
            for (step in def.steps) step.outputType?.let { type ->
                require(serializer.supports(type)) {
                    "Saga ${def.type}, step ${step.name}: cannot serialize its output $type (missing @Serializable?)"
                }
            }
        }
    }

    // ---- Start and run ----------------------------------------------------------------------

    /**
     * Creates a saga of [definition] with its [input]. Call it INSIDE the transaction that makes the
     * business change (e.g. inserting the item), then call [run] once that transaction is committed.
     *
     * @throws DuplicateSagaException if an unfinished saga already exists for [key].
     * @throws IllegalArgumentException if [definition] is not registered, or the encoded input is over
     *   [SagaConfig.maxDataBytes]. Nothing is written in either case.
     */
    @OptIn(ExperimentalUuidApi::class)
    fun <I> create(definition: SagaDefinition<I>, key: String, input: I, traceparent: String? = null): Uuid {
        require(definitions[definition.type] === definition) { "Saga type ${definition.type} is not registered with this engine" }
        val encoded = definition.inputType?.let { type ->
            serializer.encode(input, type).also { text ->
                val size = text.utf8Size()
                require(size <= config.maxDataBytes) {
                    "The input of saga ${definition.type} is $size bytes, over maxDataBytes (${config.maxDataBytes})"
                }
            }
        }
        val now = currentTime()
        val record = SagaRecord(
            id = Uuid.generateV7(), type = definition.type, key = key, state = SagaState.RUNNING,
            stepIndex = 0, stepStatus = StepStatus.READY, attempts = 0, nextAttemptAt = now,
            lockedUntil = null, lastError = null, traceparent = traceparent, createdAt = now, input = encoded,
        )
        repository.insert(record, now)
        log(record.id, record.stepIndex, null, SagaEvents.CREATED, null, now, trace = traceId())
        return record.id
    }

    /** Creates a saga of [definition], which has no input. See the other [create]. */
    fun create(definition: SagaDefinition<Unit>, key: String, traceparent: String? = null): Uuid =
        create(definition, key, Unit, traceparent)

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
        ManualChange(r.copy(stepStatus = StepStatus.READY, attempts = 0, checkFailures = 0, nextAttemptAt = now, lastError = null))
    }

    /**
     * The stuck step was found done by hand: move on to the next one. [output] is the step's output as
     * the engine's serializer writes it (JSON for [KotlinxJsonSagaSerializer]), checked against the
     * step's output type before it is saved. Null keeps the output already saved for the step (after a
     * `RECORD_FAILED`), or means no output.
     */
    suspend fun markDone(id: Uuid, output: String?, actor: String): SagaRecord =
        manual(id, actor, SagaEvents.MANUAL_DONE) { r, now ->
            require(r.state == SagaState.RUNNING && r.stepStatus == StepStatus.STUCK) { "Saga $id has no stuck forward step" }
            val def = definition(r.type)
            val step = def.stepAt(r.stepIndex)
            val (encoded, decoded) = manualOutput(r, step, output)
            repository.saveResult(r.id, r.stepIndex, step.name, encoded, now)
            ManualChange(next(r, def, now), detail = describe(step, decoded)?.let { "ref=$it" })
        }

    /**
     * Gives up: undoes what was done. Refused while a call is in flight or its outcome is unknown.
     *
     * A stuck step may have been applied (its check could not tell): say so with [currentStepApplied],
     * after looking in the external system. When true, the current step is compensated too, and
     * receives [output] as its own output (checked as in [markDone]). A step that is READY was never applied.
     */
    suspend fun abort(id: Uuid, actor: String, currentStepApplied: Boolean, output: String? = null): SagaRecord =
        manual(id, actor, SagaEvents.MANUAL_ABORT) { r, now ->
            require(r.state == SagaState.RUNNING) { "Saga $id is not running forward" }
            require(r.stepStatus == StepStatus.READY || r.stepStatus == StepStatus.WAITING || r.stepStatus == StepStatus.STUCK) {
                "A call of saga $id may have been applied: wait for its outcome"
            }
            require(!currentStepApplied || r.stepStatus == StepStatus.STUCK) {
                "The current step of saga $id is READY: it was not applied"
            }
            val updated = if (currentStepApplied) {
                val step = definition(r.type).stepAt(r.stepIndex)
                repository.saveResult(r.id, r.stepIndex, step.name, manualOutput(r, step, output).first, now)
                compensateFrom(r, r.stepIndex, now)
            } else {
                compensateFrom(r, r.stepIndex - 1, now)
            }
            ManualChange(updated, detail = "currentStepApplied=$currentStepApplied")
        }

    // ---- Signals and cancellation -----------------------------------------------------------

    /**
     * Delivers the signal [name], with its [value], to the saga [sagaId]: to the step of its definition
     * that waits for that name. One short transaction; call it from a Kafka listener (then commit the
     * offset) or an HTTP handler. Only the first signal of a name counts: a redelivery is ignored.
     *
     * @return [SignalResult.Delivered] (the step was waiting: accepted or rejected by its onSignal),
     *   [SignalResult.Stored] (it does not wait yet: used when it does), [SignalResult.Ignored]
     *   (already received, or too late), or [SignalResult.NoSaga].
     * @throws IllegalArgumentException if no step of the saga waits for [name], or it expects another type.
     */
    suspend inline fun <reified S> signal(sagaId: Uuid, name: String, value: S): SignalResult =
        signal(sagaId, name, value, typeOf<S>())

    @PublishedApi
    internal suspend fun signal(sagaId: Uuid, name: String, value: Any?, type: KType): SignalResult {
        val trace = traceId()
        return transactions.inTransaction { deliver(sagaId, name, value, type, trace) }
    }

    private fun deliver(sagaId: Uuid, name: String, value: Any?, type: KType, trace: String?): SignalResult {
        val r = repository.lock(sagaId) ?: return SignalResult.NoSaga
        val now = currentTime()
        val def = definition(r.type)
        val index = def.steps.indexOfFirst { it is SignalStep<*, *> && it.signal == name }
        require(index >= 0) { "No step of saga type ${r.type} waits for signal '$name'" }
        val step = def.signalStepAt(index)
        require(step.outputType == type) { "Signal '$name' of saga type ${r.type} carries ${step.outputType}, not $type" }
        val encoded = serializer.encode(value, type)
        val size = encoded.utf8Size()
        require(size <= config.maxDataBytes) { "Signal '$name' is $size bytes, over maxDataBytes (${config.maxDataBytes})" }

        fun received(what: String) {
            val detail = listOfNotNull(what, describe(step, value)?.let { "ref=$it" }).joinToString(" ")
            log(r.id, index, step.name, SagaEvents.SIGNAL_RECEIVED, detail, now, trace)
        }
        // Past the step (moved on, compensating or finished): the saga never goes back, so nothing would
        // read the signal. Not stored: its value may hold personal data, kept for nothing until the purge.
        if (r.state != SagaState.RUNNING || r.stepIndex > index) {
            received("ignored: too late")
            return SignalResult.Ignored(r.id)
        }
        // Cancelled while a call is in flight: whatever its outcome, the saga compensates and never waits again.
        if (r.cancelRequestedAt != null) {
            received("ignored: cancelled")
            return SignalResult.Ignored(r.id)
        }
        if (!repository.saveSignal(r.id, name, encoded, now)) {
            received("ignored: already received")
            return SignalResult.Ignored(r.id)
        }
        return if (r.stepIndex == index && r.stepStatus == StepStatus.WAITING) {
            received("delivered")
            applySignal(r, def, step, context(r, def), value, encoded, now)
            SignalResult.Delivered(r.id)
        } else {
            // Not waiting yet (an earlier step runs, or this one is ready, checking or stuck): kept for it.
            received("stored")
            SignalResult.Stored(r.id)
        }
    }

    /** The signal step [step] starts waiting, or consumes at once a signal that arrived before it. */
    private fun enterWait(
        r: SagaRecord,
        def: AnyDefinition,
        step: AnySignalStep,
        ctx: SagaContext<Any?>,
        now: Instant,
        trace: String?,
    ): Cycle {
        val stored = repository.signal(r.id, step.signal)
        if (stored != null) {
            val value = decode(stored, checkNotNull(step.outputType)) { "signal '${step.signal}' of saga ${r.id}" }
            log(r.id, r.stepIndex, step.name, SagaEvents.SIGNAL_RECEIVED, "consumed (stored earlier)", now, trace)
            return claimLocked(applySignal(r, def, step, ctx, value, stored, now), trace)
        }
        val deadline = now + step.timeout
        val waiting = r.copy(stepStatus = StepStatus.WAITING, nextAttemptAt = deadline, attempts = 0, lastError = null)
        repository.save(waiting, now)
        log(r.id, r.stepIndex, step.name, SagaEvents.WAIT_STARTED, "signal '${step.signal}' until $deadline", now, trace)
        return Cycle(waiting, next = null)
    }

    /** A signal for the waiting [step]: its onSignal accepts it (the saga moves on) or rejects it (it compensates). */
    private fun applySignal(
        r: SagaRecord,
        def: AnyDefinition,
        step: AnySignalStep,
        ctx: SagaContext<Any?>,
        value: Any?,
        encoded: String,
        now: Instant,
    ): SagaRecord {
        val updated = try {
            step.accept(ctx, value)
            repository.saveResult(r.id, r.stepIndex, step.name, encoded, now)
            next(r, def, now)
        } catch (e: StepRejected) {
            compensateFrom(r.copy(lastError = e.toString()), r.stepIndex - 1, now)
        }
        repository.save(updated, now)
        finishIfNeeded(r, updated)
        return updated
    }

    /**
     * Abandons the saga: everything done so far is compensated. A saga that is waiting or ready
     * compensates at once. One with a call in flight, or an outcome still to check, compensates as soon
     * as that outcome is recorded, since the call may have been applied and must be undone too. A stuck
     * saga stays stuck until a human retries or aborts it. Refused for a finished saga: undoing it is a
     * new saga.
     */
    suspend fun cancel(id: Uuid, reason: String, actor: String): SagaRecord =
        manual(id, actor, SagaEvents.CANCEL_REQUESTED) { r, now ->
            require(!r.state.isFinished) { "Saga $id is finished: undoing it is a new saga" }
            if (r.state != SagaState.RUNNING) return@manual ManualChange(r, detail = reason)   // already compensating
            val requested = r.copy(cancelRequestedAt = r.cancelRequestedAt ?: now, lastError = "Cancelled: $reason")
            ManualChange(cancelIfRequested(requested, now), detail = reason)
        }

    /** A saga whose cancellation was requested compensates once no call of it is in flight. */
    private fun cancelIfRequested(r: SagaRecord, now: Instant): SagaRecord =
        if (r.cancelRequestedAt != null && r.state == SagaState.RUNNING &&
            (r.stepStatus == StepStatus.READY || r.stepStatus == StepStatus.WAITING)
        ) {
            compensateFrom(r, r.stepIndex - 1, now)
        } else {
            r
        }

    private class ManualChange(val record: SagaRecord, val detail: String? = null)

    private suspend fun manual(
        id: Uuid,
        actor: String,
        event: String,
        change: (SagaRecord, Instant) -> ManualChange,
    ): SagaRecord {
        val trace = traceId()
        transactions.inTransaction {
            val r = repository.lock(id) ?: throw NoSuchElementException("Saga $id not found")
            val now = currentTime()
            val (updated, detail) = change(r, now).let { it.record to it.detail }
            repository.save(updated, now)
            // r: logged on the step the action was taken on
            log(r.id, r.stepIndex, null, event, detail, now, trace, actor)
            finishIfNeeded(r, updated)
        }
        return run(id, deadline = currentTime() + 10.seconds)
    }

    /**
     * The output an operator gives for [step], encoded and decoded, after checking it against the step's
     * output type. Null keeps the output already saved for the step, if any.
     */
    private fun manualOutput(r: SagaRecord, step: AnyStep, output: String?): Pair<String?, Any?> {
        val type = step.outputType
        if (type == null) {
            require(output == null) { "Step '${step.name}' has no output" }
            return null to Unit
        }
        if (output == null) {
            val stored = repository.results(r.id)
            if (step.name in stored) return stored[step.name] to stored[step.name]?.let { decode(it, type) { "the output of step '${step.name}'" } }
            require(type.isMarkedNullable) { "Step '${step.name}' needs an output of type $type" }
            return null to null
        }
        val size = output.utf8Size()
        require(size <= config.maxDataBytes) { "The output is $size bytes, over maxDataBytes (${config.maxDataBytes})" }
        val decoded = try {
            serializer.decode(output, type)
        } catch (e: Exception) {
            throw IllegalArgumentException("Not a valid output of step '${step.name}' ($type): $e", e)
        }
        return output to decoded
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
        val step: AnyStep,
        val action: Action,
        val ctx: SagaContext<Any?>,
        val trace: String?,
    )

    /** [event]: the second half of the logged event (see [SagaEvents]). Stored: never change it. */
    private sealed class Outcome(val event: String, val detail: String? = null) {
        /** [output] is encoded; [logged] is what the step's describe() returned: never the value itself. */
        class Done(val output: String?, logged: String?) : Outcome("Done", logged?.let { "ref=$it" })
        /** The call was made, but its output could not be encoded or is too large: the step must not run again. */
        class OutputUnstorable(val reason: String) : Outcome("OutputUnstorable", reason)
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
        // Decoded before anything is called: data that cannot be read throws here, and the transaction
        // rolls back. resumeDue then postpones the saga (another version of the code may read it).
        val ctx = context(r, def)
        // A cancellation that waited for a call to end: compensate now that none is in flight.
        val cancelled = cancelIfRequested(r, now)
        if (cancelled !== r) {
            repository.save(cancelled, now)
            finishIfNeeded(r, cancelled)
            return claimLocked(cancelled, trace)
        }
        val step = def.stepAt(r.stepIndex)
        if (step is SignalStep<*, *> && r.state == SagaState.RUNNING) {
            @Suppress("UNCHECKED_CAST")
            val signalStep = step as AnySignalStep
            when (r.stepStatus) {
                StepStatus.READY -> return enterWait(r, def, signalStep, ctx, now, trace)
                StepStatus.WAITING -> {                  // its timeout passed without the signal
                    log(r.id, r.stepIndex, step.name, SagaEvents.WAIT_TIMED_OUT, null, now, trace)
                    val noSignal = "No signal '${signalStep.signal}' before the timeout"
                    if (!signalStep.checksOnTimeout) {
                        val stuck = r.copy(stepStatus = StepStatus.STUCK, lastError = noSignal)
                        repository.save(stuck, now)
                        return Cycle(stuck, next = null)
                    }
                    r = r.copy(stepStatus = StepStatus.UNKNOWN, lastError = noSignal)   // the check decides, below
                }
                else -> Unit                              // UNKNOWN: its timeout check, claimed below
            }
        }
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
        return Cycle(claimed, Work(claimed, def.stepAt(r.stepIndex), action, ctx, trace))
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
            Action.EXECUTE -> done(work, work.step.execute(work.ctx))
            Action.CHECK -> when (val c = work.step.check(work.ctx)) {
                is CheckResult.Done -> accepted(work, c.output)
                // A signal step found nothing at its timeout: the signal will not come, the saga gives up.
                CheckResult.NotDone -> (work.step as? SignalStep<*, *>)
                    ?.let { Outcome.Rejected("No signal '${it.signal}' before the timeout, and the check found none") }
                    ?: Outcome.NotDone
                CheckResult.CannotTell -> Outcome.CannotTell
            }
            Action.COMPENSATE -> {
                work.step.compensate(work.ctx, work.ctx.output(work.step.name))
                Outcome.Compensated
            }
        }

    /** A check found [output]. For a signal step, its onSignal decides first, as for a signal received. */
    private fun accepted(work: Work, output: Any?): Outcome {
        val step = work.step
        if (step is SignalStep<*, *>) {
            try {
                @Suppress("UNCHECKED_CAST")
                (step as AnySignalStep).accept(work.ctx, output)
            } catch (e: StepRejected) {
                return Outcome.Rejected(e.toString())
            }
        }
        return done(work, output)
    }

    /**
     * The call returned [output]: encode it now, outside any transaction. The call was made, so if the
     * output cannot be stored the step must not run again: it becomes STUCK for a human.
     */
    private fun done(work: Work, output: Any?): Outcome {
        val type = work.step.outputType ?: return Outcome.Done(output = null, logged = null)
        val encoded = try {
            serializer.encode(output, type)
        } catch (e: Exception) {
            return Outcome.OutputUnstorable("cannot encode it: $e")
        }
        val size = encoded.utf8Size()
        if (size > config.maxDataBytes) {
            return Outcome.OutputUnstorable("it is $size bytes, over maxDataBytes (${config.maxDataBytes})")
        }
        return Outcome.Done(encoded, describe(work.step, output))
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

        val decided = when (outcome) {
            is Outcome.Done -> {
                repository.saveResult(r.id, r.stepIndex, work.step.name, outcome.output, now)
                next(r, def, now)
            }
            is Outcome.OutputUnstorable -> r.copy(
                stepStatus = StepStatus.STUCK,
                lastError = "The call was made, but its output could not be stored (${outcome.reason}): " +
                    "record it with markDone, or undo the step with abort",
            )
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
        // Cancelled while the call was in flight: the call's outcome is recorded, now compensate (it included).
        val updated = cancelIfRequested(decided, now)
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
     * ran, for a human to resolve with [markDone], [retry] or [abort] once the cause is fixed. The
     * outcome is described in lastError and the log. A successful call's output is saved with the
     * step's result, never in lastError (it may hold personal data): `markDone(id, null)` continues with it.
     */
    private fun recordFailed(work: Work, outcome: Outcome, error: Throwable): SagaRecord? {
        val current = repository.lock(work.saga.id) ?: return null
        val now = currentTime()
        if (!stillClaimedBy(current, work)) {
            log(work, SagaEvents.LATE_RESULT_IGNORED, describe(work, outcome), now)
            return current
        }
        if (outcome is Outcome.Done) repository.saveResult(current.id, current.stepIndex, work.step.name, outcome.output, now)
        val kept = if (outcome is Outcome.Done) ". The output is saved: markDone(id, null) continues with it" else ""
        val stuck = current.copy(
            stepStatus = StepStatus.STUCK,
            lockedUntil = null,
            lastError = "Could not record ${describe(work, outcome)}: $error$kept",
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
    /**
     * The current step is done: on to the next one, or the saga is completed. A saga whose cancellation
     * was requested compensates instead, this step included, even when it was the last one.
     */
    private fun next(r: SagaRecord, def: AnyDefinition, now: Instant): SagaRecord {
        if (r.cancelRequestedAt != null && r.state == SagaState.RUNNING) return compensateFrom(r, r.stepIndex, now)
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
        val ctx = context(after, def)
        if (after.state == SagaState.COMPLETED) def.onCompleted(ctx) else def.onFailed(ctx)
    }

    // ---- Saga data --------------------------------------------------------------------------

    /** The saga's input and the outputs of its completed steps, decoded. Throws [SagaDataException]. */
    private fun context(r: SagaRecord, def: AnyDefinition): SagaContext<Any?> {
        val inputType = def.inputType
        val input = if (inputType == null) {
            Unit
        } else {
            val text = r.input ?: throw SagaDataException("Saga ${r.id} has no stored input")
            decode(text, inputType) { "the input of saga ${r.id}" }
        }
        val stored = repository.results(r.id)
        val outputs = def.steps.filter { it.name in stored }.associate { step ->
            val type = step.outputType
            val text = stored[step.name]
            step.name to when {
                type == null -> Unit
                text == null -> null
                else -> decode(text, type) { "the output of step '${step.name}' of saga ${r.id}" }
            }
        }
        return SagaContext(r.id, r.key, input, outputs)
    }

    private fun decode(text: String, type: KType, what: () -> String): Any? =
        try {
            serializer.decode(text, type)
        } catch (e: Exception) {
            throw SagaDataException("Cannot read ${what()} as $type: $e", e)
        }

    /** What saga_log may record about [output]: the step's describe(), never the value. */
    private fun describe(step: AnyStep, output: Any?): String? =
        try {
            step.describe(output)
        } catch (_: Exception) {
            null
        }

    private fun String.utf8Size() = encodeToByteArray().size

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

    private fun definition(type: String): AnyDefinition =
        definitions[type] ?: throw IllegalArgumentException("Unknown saga type: $type")
}

private typealias AnyDefinition = SagaDefinition<Any?>
private typealias AnyStep = SagaStep<Any?, Any?>

private typealias AnySignalStep = SignalStep<Any?, Any?>

/** The engine handles every step through its erased type: each one only ever sees its own values. */
@Suppress("UNCHECKED_CAST")
private fun AnyDefinition.stepAt(index: Int): AnyStep = steps[index] as AnyStep

@Suppress("UNCHECKED_CAST")
private fun AnyDefinition.signalStepAt(index: Int): AnySignalStep = steps[index] as AnySignalStep

/**
 * A saga's input or a step's output cannot be read: it was written by another version of the code, or
 * the serializer's configuration changed. Thrown before any call is made, so nothing is left half done.
 */
class SagaDataException(message: String, cause: Throwable? = null) : IllegalStateException(message, cause)
