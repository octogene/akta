package dev.octogene.akta

import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.NonCancellable
import kotlinx.coroutines.ensureActive
import kotlinx.coroutines.isActive
import kotlinx.coroutines.sync.Semaphore
import kotlinx.coroutines.sync.withPermit
import kotlinx.coroutines.withContext
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
public class SagaEngine(
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
    /** Told about every log entry, after its transaction commits: for metrics and hooks. See [SagaListener]. */
    private val listeners: List<SagaListener> = emptyList(),
) {
    /** Every decision on a saga's row: pure, and tested on its own. */
    private val machine = SagaMachine(config) { type -> this.definitions[type] }

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
            for (step in def.steps) step.idempotency?.let { idempotency ->
                // A single call, which may last callLock, must end while the service still knows the key.
                require(idempotency.keyRetention > config.callLock) {
                    "Saga ${def.type}, step ${step.name}: keyRetention (${idempotency.keyRetention}) must exceed callLock (${config.callLock})"
                }
            }
        }
    }

    // ---- Start and run ----------------------------------------------------------------------

    /**
     * Creates a saga of [definition] with its [input]. Call it INSIDE the transaction that makes the
     * business change (e.g. inserting the item), then call [run] once that transaction is committed.
     *
     * [key] is shared by every saga type: one unfinished saga per key, whatever its type. Prefix keys by
     * what they identify (`order:123`) unless sagas of different types are meant to exclude each other.
     *
     * @throws DuplicateSagaException if an unfinished saga already exists for [key].
     * @throws IllegalArgumentException if [definition] is not registered, [key] is blank, or the encoded input is
     *   over [SagaConfig.maxDataBytes]. Nothing is written in any case.
     */
    @OptIn(ExperimentalUuidApi::class)
    public fun <I> create(definition: SagaDefinition<I>, key: String, input: I, traceparent: String? = null): Uuid {
        require(definitions[definition.type] === definition) { "Saga type ${definition.type} is not registered with this engine" }
        require(key.isNotBlank()) { "A saga's key must not be blank" }
        val encoded = definition.inputType?.let { type ->
            serializer.encode(input, type).also { text ->
                val size = text.utf8Size()
                require(size <= config.maxDataBytes) {
                    "The input of saga ${definition.type} is $size bytes, over maxDataBytes (${config.maxDataBytes})"
                }
            }
        }
        val now = currentTime()
        val saga = machine.created(Uuid.generateV7(), definition.type, key, encoded, traceparent, now)
        repository.insert(saga.record(), now)
        log(saga.id, saga.stepIndex, null, SagaEvents.CREATED, null, now, trace = traceId())
        return saga.id
    }

    /** Creates a saga of [definition], which has no input. See the other [create]. */
    public fun create(definition: SagaDefinition<Unit>, key: String, traceparent: String? = null): Uuid =
        create(definition, key, Unit, traceparent)

    /**
     * Moves the saga forward until it ends, blocks (waiting for a retry, an unknown outcome,
     * a human) or [deadline] passes. Returns its state: an HTTP handler can map it to 201, 422 or 202.
     */
    public suspend fun run(id: Uuid, deadline: Instant, skipLocked: Boolean = false): SagaRecord {
        val trace = traceId()      // here, on the caller's coroutine: transactions may switch threads
        // A step, once claimed, is called and its outcome recorded even if the caller is cancelled (an HTTP
        // client that disconnects, a shutdown): abandoned, it would stay IN_PROGRESS until its lock expires,
        // then count as an unknown outcome, to check or stuck. The caller's cancellation only stops the next
        // claim; run() then throws it. The wait it adds is bounded by callLock. The claim itself is shielded
        // too: a transaction that commits while its caller is cancelled must still lead to the call.
        val caller = currentCoroutineContext()
        caller.ensureActive()
        var cycle: Cycle = if (currentTime() < deadline) {
            withContext(NonCancellable) { claimTx(id, skipLocked, trace) }
        } else {
            Cycle.Rest(saga = null)
        }
        while (true) {
            cycle = when (val current = cycle) {
                is Cycle.Rest -> break
                is Cycle.Call -> withContext(NonCancellable) {
                    val outcome = perform(current.work)
                    // Records this outcome and, in the same transaction, claims the next step if it is due,
                    // unless the caller was cancelled meanwhile.
                    recordTx(current.work, outcome, claimUntil = deadline.takeIf { caller.isActive })
                }
                // The hook runs in a transaction of its own, after the outcome that ended the steps is committed.
                is Cycle.Finish -> withContext(NonCancellable) { finish(id, trace) }
                // A stored signal to consume after an outcome or another signal: in a transaction of its own, so
                // that what onSignal throws never rolls back what was recorded.
                is Cycle.ClaimAgain -> withContext(NonCancellable) { claimTx(id, skipLocked = false, trace) }
            }
        }
        caller.ensureActive()
        // Read again only when no transaction gave us the saga (deadline already passed, row locked elsewhere).
        return cycle.saga?.record() ?: tx { repository.find(id) } ?: error("Saga $id not found")
    }

    /** A claim, in a transaction; if onSignal threw on a stored signal, the saga stops instead, the signal kept. */
    private suspend fun claimTx(id: Uuid, skipLocked: Boolean, trace: String?): Cycle =
        try {
            tx { claim(id, skipLocked, trace) }
        } catch (e: SignalFailed) {
            tx { blockOnStoredSignal(id, e, trace) }
        }

    /**
     * Transaction 2: records [outcome]. While the row is locked, it also claims the next step if it is due and
     * [claimUntil] has not passed: one transaction instead of two. With [claimUntil] null, it only records.
     */
    private suspend fun recordTx(work: Work, outcome: Outcome, claimUntil: Instant?): Cycle =
        try {
            tx {
                val recorded = record(work, outcome) ?: return@tx Cycle.Rest(saga = null)
                if (claimUntil != null && currentTime() < claimUntil) {
                    claimLocked(recorded, work.trace, chained = true)
                } else {
                    Cycle.Rest(recorded)
                }
            }
        } catch (e: Throwable) {
            if (e.isFatal()) throw e
            recordAfterFailure(work, outcome, e)
        }

    /**
     * The transaction that recorded [outcome] (and claimed the next step) was rolled back. First, record
     * the outcome alone: if only the claim of the next step failed, the call's result must not be lost.
     * If that fails too (a constraint, the database…), the saga becomes STUCK with the
     * outcome kept for a human: otherwise the step would loop through lock expiry and check, forever.
     */
    private suspend fun recordAfterFailure(work: Work, outcome: Outcome, error: Throwable): Cycle =
        try {
            Cycle.Rest(tx { record(work, outcome) })
        } catch (e: Throwable) {
            if (e is CancellationException || e.isFatal()) throw e
            // e, not error: what stopped the outcome alone from being recorded. The first failure may have been
            // a passing one (a database error before onSignal ever ran).
            Cycle.Rest(tx { recordFailed(work, outcome, e) })
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
    public suspend fun resumeDue(
        limit: Int = 50,
        budgetPerSaga: Duration = 20.seconds,
        concurrency: Int = 8,
    ): ResumeResult {
        require(concurrency >= 1) { "concurrency must be at least 1" }
        // Only the types this engine knows: during a rolling deploy, old pods leave new types to new pods.
        val trace = traceId()      // here, on the caller's coroutine, for the failures logged below
        val ids = tx { repository.findDue(currentTime(), limit, definitions.keys) }
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
                            postpone(id, e, trace)
                            id to e
                        }
                    }
                }
            }.awaitAll().filterNotNull().toMap()
        }
        return ResumeResult(sagas = ids.size, failures = failures)
    }

    /**
     * After [run] threw in [resumeDue], or after a manual action (a database error, a bug…): moves the saga back in the queue,
     * so that sagas failing on every attempt cannot fill each batch and starve the others. After
     * [SagaConfig.maxAttempts] such failures in a row, the saga becomes STUCK ([StuckReason.RESUME_FAILING]):
     * postponed for ever, it would never reach an operator, and would fill its log.
     * Best effort: if this fails too, the error is already reported by [resumeDue].
     */
    private suspend fun postpone(id: Uuid, error: Throwable, trace: String?) {
        try {
            tx {
                val r = lock(id, skipLocked = true) ?: return@tx
                val now = currentTime()
                val postponed = machine.postpone(r, error.toString(), now) ?: return@tx
                save(postponed, now)
                log(r.id, r.stepIndex, null, SagaEvents.RESUME_FAILED, error.toString(), now, trace)
            }
        } catch (e: Throwable) {
            // The database is probably down: nothing more to do here. Any non-fatal error, as in resumeDue:
            // escaping, it would end the batch (or fail a manual action that is already committed).
            if (e is CancellationException || e.isFatal()) throw e
        }
    }

    /** How long a manual action runs the saga on by default, after committing its change. */
    private companion object {
        val MANUAL_RUN = 10.seconds
        const val MAX_PURGE_BATCH = 10_000
    }

    /** What [resumeDue] did: how many sagas it looked at, and the ones that threw. */
    public data class ResumeResult(val sagas: Int, val failures: Map<Uuid, Throwable>)

    public suspend fun find(id: Uuid): SagaRecord? = tx { repository.find(id) }

    // ---- Reads for operators ----------------------------------------------------------------

    /**
     * Stuck sagas, stuck longest first: what an admin screen lists. Each carries its [SagaRecord.stuckReason],
     * which says what to do. For the next page, pass `StuckCursor.after(lastOfThePage)` as [after].
     */
    public suspend fun findStuck(limit: Int = 100, types: Collection<String>? = null, after: StuckCursor? = null): List<SagaRecord> {
        require(limit >= 1) { "limit must be at least 1" }
        return tx { repository.findStuck(limit, types, after) }
    }

    /**
     * The sagas of a business key, newest first: the unfinished one if any, then the finished ones still kept.
     * What an operator searches by ("why is item 3034… stuck?"); then [history] for one of them.
     */
    public suspend fun findByKey(key: String, limit: Int = 20): List<SagaRecord> {
        require(limit >= 1) { "limit must be at least 1" }
        return tx { repository.findByKey(key, limit) }
    }

    /** Everything that happened to a saga, oldest first. */
    public suspend fun history(sagaId: Uuid): List<SagaLogEntry> = tx { repository.history(sagaId) }

    /** What the request with this trace id did to sagas, oldest first. */
    public suspend fun historyByTrace(traceId: String, limit: Int = 200): List<SagaLogEntry> {
        require(limit >= 1) { "limit must be at least 1" }
        return tx { repository.historyByTrace(traceId, limit) }
    }

    /**
     * Counts and ages of the unfinished sagas, for gauges and alerts: stuck sagas by reason, the oldest
     * unfinished saga, and how far behind the recovery job is (`now - oldestDueAt`). It reads every
     * unfinished saga (one grouped query on PostgreSQL), never the finished ones: sample it about once a
     * minute, not per request.
     */
    public suspend fun stats(types: Collection<String>? = null): SagaStats = tx { repository.stats(currentTime(), types) }

    // ---- Retention --------------------------------------------------------------------------

    /**
     * Deletes the sagas that finished (COMPLETED or FAILED) more than [olderThan] ago, with their results
     * and their log. Unfinished and stuck sagas are never deleted, however old. Call it periodically
     * from the job runner (daily keeps each run small). It works in batches of [batchSize] sagas, one
     * short transaction each, so it never holds locks for long ([batchSize] at most 10,000). Returns the number of
     * sagas deleted.
     */
    public suspend fun purgeFinished(olderThan: Duration = 30.days, batchSize: Int = 1000): Int {
        require(olderThan.isPositive()) { "olderThan must be positive" }
        // A batch deletes by its ids, one bind parameter each: PostgreSQL's driver takes at most 65,535 per statement.
        require(batchSize in 1..MAX_PURGE_BATCH) { "batchSize must be between 1 and $MAX_PURGE_BATCH" }
        val before = currentTime() - olderThan
        var total = 0
        do {
            val deleted = tx { repository.purgeFinished(before, batchSize) }
            total += deleted
        } while (deleted == batchSize)
        return total
    }

    // ---- Manual actions on a stuck saga -----------------------------------------------------
    //
    // Each action commits its change, then runs the saga on for up to runFor (10 s by default), as run()
    // does: a call started before that may take up to callLock more. Duration.ZERO returns at once and
    // leaves the saga to resumeDue, which picks it up within its period. Nothing runs when the saga cannot
    // move on (finished, stuck, a call in flight, not due). The change stays committed whatever happens
    // next: if running the saga on fails (the database, data that cannot be read, a type this engine does
    // not know), the action still returns the saga, postponed and logged as RESUME_FAILED, as in resumeDue.

    /**
     * Retries the stuck step. Forward, this EXECUTES the step again without checking first:
     * when the saga is stuck because a check could not tell, only use it once you have confirmed
     * by hand that the step was not applied (otherwise use [markDone] or [abort]).
     *
     * For an idempotent step, a retry replays with the same key: safe without looking first, while the service
     * remembers the key. [newKey] sends a fresh key instead: only once you have confirmed in the service that
     * the step was not applied (after [StuckReason.KEY_EXPIRED], or when the service keeps answering a failure
     * it remembered for the old key).
     *
     * After [StuckReason.RESUME_FAILING] the step itself was not at fault: it resumes as it was
     * ([SagaRecord.stuckFrom]), its attempts kept. A step that may have been called is checked first (or replayed
     * with its key), not executed blindly; a signal step that was waiting keeps its timeout, or checks if it passed.
     */
    public suspend fun retry(id: Uuid, actor: String, newKey: Boolean = false, runFor: Duration = MANUAL_RUN): SagaRecord =
        manual(id, actor, SagaEvents.MANUAL_RETRY, runFor) { r, now ->
            machine.retry(r, newKey, now) { signal ->
                repository.signal(r.id, signal) != null
            }.let { ManualChange(it.saga, it.detail) }
        }

    /**
     * The stuck step was found done by hand: move on to the next one. [output] is the step's output as
     * the engine's serializer writes it (JSON for [KotlinxJsonSagaSerializer]), checked against the
     * step's output type before it is saved. Null keeps the output already saved for the step (after a
     * `RECORD_FAILED`), or means no output.
     *
     * For a signal step, [output] is the signal's value, delivered by hand: it is stored as the step's signal, and
     * the step's onSignal accepts or rejects it when the saga runs, as for any signal. Null uses the signal the
     * saga already keeps (after its onSignal failed); one it keeps is never replaced by another value.
     */
    public suspend fun markDone(id: Uuid, output: String?, actor: String, runFor: Duration = MANUAL_RUN): SagaRecord =
        manual(id, actor, SagaEvents.MANUAL_DONE, runFor) { r, now ->
            val running = machine.checkMarkDone(r)
            val def = definition(r.type)
            val step = def.stepAt(r.stepIndex)
            if (step is SignalStep<*, *>) return@manual signalByHand(running, step, output, now)
            val (encoded, decoded) = manualOutput(r.id, step, output)
            repository.saveResult(r.id, r.stepIndex, step.name, encoded, now)
            ManualChange(machine.next(running, def.steps.size, now), detail = describe(step, decoded)?.let { "ref=$it" })
        }

    /**
     * Gives up: undoes what was done. Refused while a call is in flight or its outcome is unknown.
     *
     * A stuck step may have been applied (its check could not tell): say so with [currentStepApplied],
     * after looking in the external system. When true, the current step is compensated too, and
     * receives [output] as its own output (checked as in [markDone]). A step that is READY was never applied,
     * nor was one whose refusal is kept (`RECORD_FAILED`): [currentStepApplied] is then refused.
     */
    public suspend fun abort(
        id: Uuid,
        actor: String,
        currentStepApplied: Boolean,
        output: String? = null,
        runFor: Duration = MANUAL_RUN,
    ): SagaRecord =
        manual(id, actor, SagaEvents.MANUAL_ABORT, runFor) { r, now ->
            val saga = machine.checkAbort(r, currentStepApplied)
            if (currentStepApplied && saga !is SagaPhase.Finishing) {
                val step = definition(r.type).stepAt(r.stepIndex)
                repository.saveResult(r.id, r.stepIndex, step.name, manualOutput(r.id, step, output).first, now)
            }
            ManualChange(machine.abort(saga, currentStepApplied, actor, now), detail = "currentStepApplied=$currentStepApplied")
        }

    /**
     * The stuck compensation was done by hand, or there was nothing to undo: the step is marked
     * compensated, and the saga goes on undoing the previous steps (or ends FAILED). Used wrongly, it
     * leaves an effect in the external system, as [markDone] would.
     */
    public suspend fun markCompensated(id: Uuid, actor: String, runFor: Duration = MANUAL_RUN): SagaRecord =
        manual(id, actor, SagaEvents.MANUAL_COMPENSATED, runFor) { r, now ->
            val decision = machine.markCompensated(r, now)
            write(decision.effects, r, now) { definition(r.type).stepAt(r.stepIndex).name }
            ManualChange(decision.saga)
        }

    // ---- Signals and cancellation -----------------------------------------------------------

    /**
     * Delivers the signal [name], with its [value], to the saga [sagaId]: to the step of its definition
     * that waits for that name. One short transaction; call it from a Kafka listener (then commit the
     * offset) or an HTTP handler. Only the first signal of a name counts: a redelivery is ignored.
     *
     * Runs no user code: onSignal decides when the saga next runs, so a bug in it never fails the caller.
     *
     * @return [SignalResult.Delivered] (the step was waiting: woken, due at once; `run` the saga to go on now),
     *   [SignalResult.Stored] (it does not wait yet: used when it does), [SignalResult.Ignored]
     *   (already received, or too late), or [SignalResult.NoSaga].
     * @throws IllegalArgumentException if no step of the saga waits for [name], or it expects another type.
     */
    public suspend inline fun <reified S> signal(sagaId: Uuid, name: String, value: S): SignalResult =
        signal(sagaId, name, value, typeOf<S>())

    @PublishedApi
    internal suspend fun signal(sagaId: Uuid, name: String, value: Any?, type: KType): SignalResult {
        val trace = traceId()
        // No user code runs here: the signal is stored, and its step woken if it waits. onSignal runs when the saga
        // does, so a bug in it never fails the listener.
        return tx { deliver(sagaId, name, value, type, trace) }
    }

    private fun deliver(
        sagaId: Uuid,
        name: String,
        value: Any?,
        type: KType,
        trace: String?,
    ): SignalResult {
        val r = lock(sagaId) ?: return SignalResult.NoSaga
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
        when (machine.arrival(r, index)) {
            Arrival.TOO_LATE -> { received("ignored: too late"); return SignalResult.Ignored(r.id) }
            Arrival.CANCELLED -> { received("ignored: cancelled"); return SignalResult.Ignored(r.id) }
            Arrival.STORE -> Unit
        }
        if (!repository.saveSignal(r.id, name, encoded, now)) {
            received("ignored: already received")
            return SignalResult.Ignored(r.id)
        }
        return when (machine.useOfStored(r, index)) {
            Use.WAKE -> {
                save(machine.woken(r, now), now)
                received("delivered")
                SignalResult.Delivered(r.id)
            }
            Use.KEEP -> {
                received("stored")
                SignalResult.Stored(r.id)
            }
        }
    }

    /** The signal step [step] starts waiting for its signal, until its timeout. */
    private fun startWaiting(r: SagaPhase.Unfinished, step: AnySignalStep, now: Instant, trace: String?): SagaPhase {
        val waiting = machine.waiting(r, step, now)
        save(waiting, now)
        log(r.id, r.stepIndex, step.name, SagaEvents.WAIT_STARTED, "signal '${step.signal}' until ${waiting.due}", now, trace)
        return waiting
    }

    /**
     * The signal of [step] received earlier, [stored], applied now: the saga moves on (or compensates). Saved and
     * logged; the next step is for the caller to claim.
     */
    private fun consumeStored(
        r: SagaPhase.Unfinished,
        def: AnyDefinition,
        step: AnySignalStep,
        ctx: SagaContext<Any?>,
        stored: String,
        now: Instant,
        trace: String?,
    ): SagaPhase {
        val value = decode(stored, checkNotNull(step.outputType)) { "signal '${step.signal}' of saga ${r.id}" }
        val updated = signalDecision(r, def, step, ctx, value, stored, now)
        save(updated, now)
        // Logged after the change, as in deliver(): the event shows the saga as the signal left it.
        val detail = listOfNotNull("consumed (stored earlier)", describe(step, value)?.let { "ref=$it" }).joinToString(" ")
        log(r.id, r.stepIndex, step.name, SagaEvents.SIGNAL_RECEIVED, detail, now, trace)
        return updated
    }

    /** What a signal makes of the saga, not saved yet: onSignal accepts it (next step) or rejects it (compensate). */
    private fun signalDecision(
        r: SagaPhase.Unfinished,
        def: AnyDefinition,
        step: AnySignalStep,
        ctx: SagaContext<Any?>,
        value: Any?,
        encoded: String,
        now: Instant,
    ): SagaPhase =
        try {
            onSignal(step, ctx, value)
            repository.saveResult(r.id, r.stepIndex, step.name, encoded, now)
            machine.next(r, def.steps.size, now)
        } catch (e: StepRejected) {
            machine.refused(r, e.toString(), now)
        }

    /**
     * The step's onSignal, inside a claim transaction, wherever the value comes from (a signal delivered or
     * stored, a value its timeout check found). StepRejected is its answer; anything else is a bug, thrown as
     * [SignalFailed] so that the transaction rolls back whole and the saga stops for a human (RECORD_FAILED),
     * the signal kept.
     */
    private fun onSignal(step: AnySignalStep, ctx: SagaContext<Any?>, value: Any?) {
        try {
            step.accept(ctx, value)
        } catch (e: StepRejected) {
            throw e
        } catch (e: Throwable) {
            if (e.isFatal()) throw e
            throw SignalFailed(step.signal, e)
        }
    }

    /**
     * A signal could not be applied: onSignal threw something other than StepRejected (see [onSignal]).
     */
    private class SignalFailed(val signal: String, cause: Throwable) :
        RuntimeException("onSignal threw on signal '$signal': $cause", cause)

    /**
     * The claim threw [failure]: onSignal failed on a signal stored
     * earlier, as the step started waiting or before a timeout check. The claim rolled back whole: the saga
     * stops, the signal kept.
     */
    private fun blockOnStoredSignal(id: Uuid, failure: SignalFailed, trace: String?): Cycle {
        val r = lock(id) ?: return Cycle.Rest(saga = null)
        val step = definition(r.type).steps[r.stepIndex] as? SignalStep<*, *> ?: return Cycle.Rest(r)
        val now = currentTime()
        val blocked = machine.blockedByStoredSignal(r, failure.message, now) ?: return Cycle.Rest(r)
        save(blocked, now)
        log(r.id, r.stepIndex, step.name, SagaEvents.RECORD_FAILED, blocked.lastError, now, trace)
        return Cycle.Rest(blocked)
    }

    /**
     * Abandons the saga: everything done so far is compensated. A saga that is waiting or ready
     * compensates at once. One with a call in flight, or an outcome still to check, compensates as soon
     * as that outcome is recorded, since the call may have been applied and must be undone too. A stuck
     * saga stays stuck until a human retries or aborts it. Refused for a finished saga: undoing it is a
     * new saga. [reason] is kept in [SagaRecord.cancelReason]; a second cancellation is only logged.
     */
    public suspend fun cancel(id: Uuid, reason: String, actor: String, runFor: Duration = MANUAL_RUN): SagaRecord =
        manual(id, actor, SagaEvents.CANCEL_REQUESTED, runFor) { r, now ->
            machine.cancel(r, reason, now).let { ManualChange(it.saga, it.detail) }
        }

    private class ManualChange(val saga: SagaPhase, val detail: String? = null)

    private suspend fun manual(
        id: Uuid,
        actor: String,
        event: String,
        runFor: Duration,
        change: (SagaPhase, Instant) -> ManualChange,
    ): SagaRecord {
        require(!runFor.isNegative()) { "runFor must not be negative" }
        val trace = traceId()
        val committed = tx {
            val r = lock(id) ?: throw NoSuchElementException("Saga $id not found")
            val now = currentTime()
            val (updated, detail) = change(r, now).let { it.saga to it.detail }
            save(updated, now)
            // r: logged on the step the action was taken on
            log(r.id, r.stepIndex, null, event, detail, now, trace, actor)
            updated
        }
        val now = currentTime()
        if (!runFor.isPositive() || !machine.canMoveOn(committed, now)) return committed.record()
        // The action is committed: a failure from here on is the saga's, not the action's.
        return try {
            run(id, deadline = now + runFor)
        } catch (e: Throwable) {
            if (e is CancellationException || e.isFatal()) throw e
            postpone(id, e, trace)
            try {
                tx { repository.find(id) } ?: committed.record()
            } catch (e: Throwable) {
                if (e is CancellationException || e.isFatal()) throw e
                committed.record()
            }
        }
    }

    /**
     * markDone on a signal step: [output], the signal's value, is stored as its signal (or the one kept is used), and
     * the step is due at once, to consume it through onSignal: a value onSignal refuses is refused, not marked done.
     */
    private fun signalByHand(r: SagaPhase.Running, step: SignalStep<*, *>, output: String?, now: Instant): ManualChange {
        @Suppress("UNCHECKED_CAST")
        step as AnySignalStep
        val kept = repository.signal(r.id, step.signal)
        val given = output?.let { manualOutput(r.id, step, it) }
        require(given != null || kept != null) {
            "Step '${step.name}' needs the value of signal '${step.signal}': none was received"
        }
        if (given != null && kept == null) {
            repository.saveSignal(r.id, step.signal, checkNotNull(given.first), now)
        } else {
            require(given == null || given.first == kept) {
                "Saga ${r.id} keeps the signal '${step.signal}' it received: retry uses it, or abort gives up"
            }
        }
        val value = given?.second ?: decode(checkNotNull(kept), checkNotNull(step.outputType)) { "signal '${step.signal}' of saga ${r.id}" }
        return ManualChange(machine.signalByHand(r, now), detail = listOfNotNull("signal by hand", describe(step, value)?.let { "ref=$it" }).joinToString(" "))
    }

    /**
     * The output an operator gives for [step], encoded and decoded, after checking it against the step's
     * output type. Null keeps the output already saved for the step, if any.
     */
    private fun manualOutput(sagaId: Uuid, step: AnyStep, output: String?): Pair<String?, Any?> {
        val type = step.outputType
        if (type == null) {
            require(output == null) { "Step '${step.name}' has no output" }
            return null to Unit
        }
        if (output == null) {
            val stored = repository.results(sagaId)
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

    private class Work(
        /** The saga as claimed: its call in flight. */
        val saga: SagaPhase.Unfinished,
        val step: AnyStep,
        val action: Action,
        val ctx: SagaContext<Any?>,
        val trace: String?,
        /** An idempotent step whose key was already used: an earlier attempt may have been applied. */
        val replay: Boolean,
        /** The saga as it was before the claim: what it goes back to when the call could not be made. */
        val unclaimed: SagaPhase.Unfinished,
    )

    /** The logged event, e.g. "EXECUTE:Done". */
    private fun eventOf(work: Work, outcome: Outcome): String =
        if (outcome is Outcome.NotCalled) SagaEvents.NOT_CALLED else "${work.action.event}:${outcome.event}"

    /** The event with the outcome's detail, e.g. "EXECUTE:Done ref=STK-1": for LATE_RESULT_IGNORED, RECORD_FAILED. */
    private fun describe(work: Work, outcome: Outcome): String =
        listOfNotNull(eventOf(work, outcome), outcome.detail).joinToString(" ")

    /** What a transaction leaves for [run] to do next, and the saga as it saved it. */
    private sealed interface Cycle {
        /** The saga as saved: null when it is locked elsewhere or missing. */
        val saga: SagaPhase?

        /** Nothing to do now: the saga is finished, stuck, waiting, not due, or locked elsewhere. */
        class Rest(override val saga: SagaPhase?) : Cycle

        /** A call claimed: to make outside any transaction, then record its outcome. */
        class Call(val work: Work) : Cycle {
            override val saga: SagaPhase get() = work.saga
        }

        /** The saga's hook is due, to run in a transaction of its own. */
        class Finish(override val saga: SagaPhase.Finishing) : Cycle

        /** A stored signal is due, to consume in a claim of its own. */
        class ClaimAgain(override val saga: SagaPhase) : Cycle
    }

    /** Transaction 1: claims the current step, if there is something to do now. */
    private fun claim(id: Uuid, skipLocked: Boolean, trace: String?): Cycle {
        val r = lock(id, skipLocked) ?: return Cycle.Rest(saga = null)
        return claimLocked(r, trace)
    }

    /**
     * Claims the call of the current step of [current], whose row this transaction has locked, if it is due now. On the
     * way, each change that calls nothing is saved and logged here: a lock expired, a cancellation, a step with nothing
     * to undo, a signal step that starts waiting, or consumes a signal received earlier.
     *
     * [chained]: this transaction already recorded an outcome or applied a signal, which onSignal must not roll back:
     * a stored signal then waits for a claim of its own ([Cycle.ClaimAgain]).
     */
    private fun claimLocked(current: SagaPhase, trace: String?, chained: Boolean = false): Cycle {
        var saga = current
        var afterChange = chained
        while (true) {
            val now = currentTime()
            var acting = when (val move = machine.move(saga, now)) {
                is Move.Rest -> return Cycle.Rest(move.saga)
                is Move.Finish -> return Cycle.Finish(move.saga)
                is Move.NoSignal -> {
                    save(move.saga, now)
                    log(saga.id, saga.stepIndex, stepName(saga), SagaEvents.WAIT_TIMED_OUT, null, now, trace)
                    return Cycle.Rest(move.saga)
                }
                is Move.LockExpired -> {                         // the process died during the call
                    save(move.saga, now)
                    log(saga.id, saga.stepIndex, null, SagaEvents.LOCK_EXPIRED, move.saga.lastError, now, trace)
                    saga = move.saga
                    continue
                }
                is Move.Cancelled -> {
                    save(move.saga, now)
                    log(saga.id, saga.stepIndex, null, SagaEvents.CANCELLED, move.saga.cancellation?.reason, now, trace)
                    saga = move.saga
                    continue
                }
                is Move.NothingToUndo -> {
                    write(move.effects, saga, now) { stepName(saga) }
                    save(move.saga, now)
                    log(saga.id, saga.stepIndex, stepName(saga), SagaEvents.NOTHING_TO_UNDO, null, now, trace)
                    saga = move.saga
                    continue
                }
                is Move.Act -> move.saga
            }
            val def = definition(acting.type)
            val step = def.stepAt(acting.stepIndex)
            // Decoded before anything is called: data that cannot be read throws here, and the transaction
            // rolls back. resumeDue then postpones the saga (another version of the code may read it).
            val ctx = context(acting, def)
            val signalStep = if (step is SignalStep<*, *> && acting is SagaPhase.Running) def.signalStepAt(acting.stepIndex) else null
            if (signalStep != null && acting.step is StepState.Waiting) {
                // Its timeout passed without the signal: the check decides, claimed below.
                log(acting.id, acting.stepIndex, step.name, SagaEvents.WAIT_TIMED_OUT, null, now, trace)
                acting = machine.timedOut(acting, signalStep)
            } else if (signalStep != null) {
                // READY, to start waiting; or CHECKING, its timeout check to claim. Either way, a signal received
                // earlier answers first (during a check that then failed, it answers what the check looks up).
                val stored = repository.signal(acting.id, signalStep.signal)
                if (stored != null && afterChange) {
                    save(acting, now)
                    return Cycle.ClaimAgain(acting)
                }
                if (stored != null) {
                    saga = consumeStored(acting, def, signalStep, ctx, stored, now, trace)
                    afterChange = true
                    continue
                }
                if (acting.step is StepState.Ready) return Cycle.Rest(startWaiting(acting, signalStep, now, trace))
            }
            return when (val claim = machine.claim(acting, step, now)) {
                is Claim.KeyExpired -> {
                    save(claim.saga, now)
                    log(acting.id, acting.stepIndex, step.name, SagaEvents.KEY_EXPIRED, claim.saga.lastError, now, trace)
                    Cycle.Rest(claim.saga)
                }
                is Claim.Call -> {
                    save(claim.saga, now)
                    val key = machine.keyOf(claim.saga, step.name, claim.action)
                    Cycle.Call(Work(claim.saga, step, claim.action, ctx.forCall(key), trace, claim.replay, unclaimed = acting))
                }
            }
        }
    }

    private fun stepName(saga: SagaPhase): String = definition(saga.type).stepAt(saga.stepIndex).name

    /**
     * Outside any transaction: the external call. It is cut [recordMargin] before the step's lock
     * expires, so that its outcome is recorded while this engine still owns the step: a call still
     * running after its lock expired could land after another instance has checked it (or re-run a
     * compensation alongside it). The lock's clock started in the claim transaction, before the call.
     */
    private suspend fun perform(work: Work): Outcome {
        val lockedUntil = (work.saga.step as? StepState.InFlight)?.lockedUntil ?: error("Claimed saga ${work.saga.id} without a lock")
        val budget = lockedUntil - currentTime() - recordMargin
        // A long pause since the claim (GC, a stalled thread) may leave no time at all. withTimeout would
        // then fail at once, before the step runs, and that would count as a failed call: an attempt
        // used, an outcome to check. Nothing was called, so the claim is undone instead.
        if (budget <= Duration.ZERO) {
            return Outcome.NotCalled("No time left to call before the lock expires (${-budget} late): nothing was called")
        }
        return try {
            withTimeout(budget) { call(work) }
        } catch (e: CancellationException) {
            // run() calls this shielded from its caller's cancellation, so this is a timeout (ours, or the
            // step's own): a failed call. Should anything else cancel it, rethrow: the step stays IN_PROGRESS
            // and its lock will expire.
            currentCoroutineContext().ensureActive()
            failure(work.action, e)
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
                // A signal step's value: onSignal decides on it in record(), inside the transaction.
                is CheckResult.Done -> done(work, c.output).let { done ->
                    if (work.step is SignalStep<*, *> && done is Outcome.Done) Outcome.Found(c.output, done) else done
                }
                // A signal step found nothing at its timeout: the signal will not come, the saga gives up.
                CheckResult.NotDone -> (work.step as? SignalStep<*, *>)
                    ?.let { Outcome.Rejected("No signal '${it.signal}' before the timeout, and the check found none", noSignalFound = true) }
                    ?: Outcome.NotDone
                CheckResult.CannotTell -> Outcome.CannotTell
            }
            Action.COMPENSATE -> {
                work.step.compensate(work.ctx, work.ctx.output(work.step.name))
                Outcome.Compensated
            }
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
            // A check that finds the call was refused (a record of the attempt, REJECTED): the same answer as
            // execute's StepRejected, so the saga compensates. Anything else: the lookup failed, check again.
            Action.CHECK -> if (e is StepRejected) Outcome.Rejected(error) else Outcome.CheckFailed(error)
            Action.COMPENSATE -> Outcome.CompensationFailed(error)
        }
    }

    /**
     * Records the outcome of [work]'s call and decides what comes next: the saga as saved. Null when the outcome is
     * not recorded: the saga is gone, or the call is no longer the one in flight (logged LATE_RESULT_IGNORED).
     */
    private fun record(work: Work, reported: Outcome): SagaPhase? {
        val current = lock(work.saga.id) ?: return null
        val now = currentTime()
        if (!machine.stillClaimed(current, work.saga)) {
            log(work, SagaEvents.LATE_RESULT_IGNORED, describe(work, reported), now)
            return null
        }
        val outcome = reported
        val def = definition(current.type)
        current as SagaPhase.Unfinished                       // still claimed: its call is in flight
        val r = machine.released(current)
        // A signal step's check that found nothing, or could not tell: a signal received meanwhile answers instead.
        val signalStored = { signal: String -> repository.signal(r.id, signal) != null }
        val decided = machine.decide(r, current.checkFailures, outcome, work.step, def.steps.size, work.replay, work.unclaimed, now, signalStored)
            .also { write(it.effects, r, now) { work.step.name } }.saga
        // Cancelled while the call was in flight: the call's outcome is recorded, now compensate (it included).
        val updated = machine.cancelIfRequested(decided, now)
        save(updated, now)
        // The new lastError when it changed (it may add "Too many attempts"), else the outcome's own
        // detail: a repeated error is logged every time, not only the first.
        val detail = when {
            outcome is Outcome.Done || outcome is Outcome.Found -> outcome.detail
            else -> updated.lastError.takeIf { it != current.lastError } ?: outcome.detail
        }
        // Logged on the step that ran, not on the one the saga moved to.
        log(work, eventOf(work, outcome), detail, now)
        return updated
    }

    /**
     * Fallback transaction, after [record] was rolled back: the saga becomes STUCK on the step that
     * ran, for a human to resolve once the cause is fixed (onSignal throwing, a constraint). What the
     * outcome established is kept, and lastError says which action follows from it:
     *
     * - Done: the output is saved with the step's result (never in lastError: it may hold personal
     *   data), and `markDone(id, null)` continues with it.
     * - Compensated: the step is marked undone, and `markCompensated` goes on, without undoing it again.
     * - Rejected: nothing was applied. The refusal is kept as failureReason, which makes markDone refuse
     *   the saga (it would turn a refusal into a success) and abort keep it as the reason.
     * - Any other outcome (only a database failure can stop those, no user code runs): look in the external
     *   system, then markDone, retry or abort.
     */
    private fun recordFailed(work: Work, outcome: Outcome, error: Throwable): SagaPhase? {
        val current = lock(work.saga.id) ?: return null
        val now = currentTime()
        if (!machine.stillClaimed(current, work.saga)) {
            log(work, SagaEvents.LATE_RESULT_IGNORED, describe(work, outcome), now)
            return current
        }
        current as SagaPhase.Unfinished                       // still claimed: its call is in flight
        val decision = machine.recordFailed(
            current, work.step, work.action, outcome, describe(work, outcome), error, onSignalFailed = error is SignalFailed, now,
        ) { signal -> repository.signal(current.id, signal) != null }
        write(decision.effects, current, now) { work.step.name }
        val blocked = decision.saga
        save(blocked, now)
        log(work, SagaEvents.RECORD_FAILED, blocked.lastError, now)
        return blocked
    }

    // ---- Transitions ------------------------------------------------------------------------

    /** What a decision writes besides the row, for the current step of [r] (its name is only read to save a result). */
    private fun write(effects: List<Effect>, r: SagaPhase, now: Instant, stepName: () -> String) {
        for (effect in effects) when (effect) {
            is Effect.SaveResult -> repository.saveResult(r.id, r.stepIndex, stepName(), effect.output, now)
            is Effect.SaveSignal -> repository.saveSignal(r.id, effect.signal, effect.value, now)
            Effect.MarkCompensated -> repository.markCompensated(r.id, r.stepIndex, now)
        }
    }

    // ---- Finishing: the hook, in a transaction of its own ---------------------------------------------------

    /**
     * Runs the hook of a finishing saga, in a transaction that also ends it: COMPLETED or FAILED. The outcome that
     * ended its steps is already committed, so a hook that throws loses nothing: it is tried again after backoff, then
     * the saga stops (HOOK_FAILED). A transaction that fails for another reason counts the same.
     */
    private suspend fun finish(id: Uuid, trace: String?): Cycle =
        try {
            tx { finishLocked(id, trace) }
        } catch (e: Throwable) {
            if (e is CancellationException || e.isFatal()) throw e
            Cycle.Rest(tx { hookFailedLocked(id, e, trace) })
        }

    private fun finishLocked(id: Uuid, trace: String?): Cycle {
        val r = lock(id) ?: return Cycle.Rest(saga = null)
        val now = currentTime()
        if (r !is SagaPhase.Finishing || r.step !is StepState.Ready || r.due > now) return Cycle.Rest(r)
        val def = definition(r.type)
        val ended = machine.finished(r)
        save(ended, now)
        log(r.id, r.stepIndex, null, SagaEvents.FINISHED, ended.state.name, now, trace)
        // Written before the hook, not when the transaction ends: the hook may read the saga table, or create the next
        // saga for the same key, which the unique index on unfinished sagas would refuse while this row read FINISHING.
        txEvents.get()?.let(::writeUnsaved)
        val ctx = context(ended, def)
        if (ended.state == SagaState.COMPLETED) def.onCompleted(ctx) else def.onFailed(ctx)
        return Cycle.Rest(ended)
    }

    private fun hookFailedLocked(id: Uuid, error: Throwable, trace: String?): SagaPhase? {
        val r = lock(id) ?: return null
        // Committed after all (the failure came after the commit), or finished elsewhere: nothing to count.
        if (r !is SagaPhase.Finishing || r.step !is StepState.Ready) return r
        val now = currentTime()
        val failed = machine.hookFailed(r, error.toString(), now)
        save(failed, now)
        log(r.id, r.stepIndex, null, SagaEvents.HOOK_FAILED, failed.lastError, now, trace)
        return failed
    }

    // ---- Saga data --------------------------------------------------------------------------

    /** The saga's input and the outputs of its completed steps, decoded. Throws [SagaDataException]. */
    private fun context(r: SagaPhase, def: AnyDefinition): SagaContext<Any?> {
        val inputType = def.inputType
        val input = if (inputType == null) {
            Unit
        } else {
            val text = r.identity.input ?: throw SagaDataException("Saga ${r.id} has no stored input")
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
        return SagaContext(r.id, r.identity.key, input, outputs)
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

    /**
     * Free text (an error message, a reason, what describe() returned) as a database can keep it: PostgreSQL
     * refuses U+0000, which an exception quoting a binary response may carry. Refused, the text would prevent
     * the outcome it describes from being recorded at all, RECORD_FAILED included.
     */
    private fun String.storable(): String = replace('\u0000', '\uFFFD')

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
        val entry = SagaLogEntry(sagaId, now, stepIndex, stepName, event, detail?.storable(), actor, trace)
        repository.log(entry)
        // Reported after commit, with the saga as this transaction last read or wrote it. None from create(),
        // which runs in the caller's transaction.
        val pending = txEvents.get() ?: return
        val r = pending.latest[sagaId] ?: return
        pending.events += SagaEvent(entry, r.type, r.state, r.stepStatus, r.stuckReason)
    }

    // ---- Transactions, and the events they report after commit ---------------------------------

    /**
     * The transaction running on this thread: the sagas it touched as it last read or wrote them, its log
     * entries, and the sagas it changed, written once each when its block ends (see [save]).
     */
    private class PendingEvents {
        val latest = HashMap<Uuid, SagaRecord>()
        val events = ArrayList<SagaEvent>()
        val unsaved = LinkedHashMap<Uuid, Pair<SagaRecord, Instant>>()
    }

    /**
     * Set while the engine's transaction block runs. The block is plain blocking code on one thread
     * ([SagaTransactions.inTransaction] takes a non-suspending block), so a thread local is safe here.
     */
    private val txEvents = ThreadLocal<PendingEvents?>()

    /** Writes the sagas the transaction changed and has not written yet, once each. */
    private fun writeUnsaved(pending: PendingEvents) {
        for ((record, at) in pending.unsaved.values) repository.save(record, at)
        pending.unsaved.clear()
    }

    /** One transaction; its events go to the listeners once it has committed. A retried attempt starts afresh. */
    private suspend fun <T> tx(block: () -> T): T {
        var committed: PendingEvents? = null
        val result = transactions.inTransaction {
            val pending = PendingEvents()
            committed = pending
            val outer = txEvents.get()
            txEvents.set(pending)
            try {
                block().also {
                    // Each saga the block changed, written once: a step recorded and the next one claimed in the
                    // same transaction is one UPDATE, not two (each is a new row version, a dead one to vacuum).
                    writeUnsaved(pending)
                }
            } finally {
                // remove(), not set(null): pooled threads are left exactly as they were found.
                if (outer == null) txEvents.remove() else txEvents.set(outer)
            }
        }
        committed?.let { emit(it.events) }
        return result
    }

    private fun emit(events: List<SagaEvent>) {
        for (event in events) for (listener in listeners) {
            try {
                listener.onEvent(event)
            } catch (e: Throwable) {
                if (e.isFatal()) throw e        // anything else: ignored, the saga is not affected
            }
        }
    }

    /** Locks the saga's row and reads it as a phase: a row the engine must never have written throws here. */
    private fun lock(id: Uuid, skipLocked: Boolean = false): SagaPhase? =
        repository.lock(id, skipLocked)?.also { txEvents.get()?.latest?.put(it.id, it) }?.phase()

    /**
     * Saves [saga], as its row, when the transaction's block ends, so that a saga changed several times in one transaction is
     * written once. Nothing in a transaction reads a saga row back after changing it: each works on the
     * record it locked. Outside the engine's transactions (none today), it writes at once.
     */
    private fun save(saga: SagaPhase, now: Instant) {
        val row = saga.record()
        val r = row.copy(
            lastError = row.lastError?.storable(),
            failureReason = row.failureReason?.storable(),
            cancelReason = row.cancelReason?.storable(),
        )
        val pending = txEvents.get()
        if (pending == null) {
            repository.save(r, now)
            return
        }
        pending.unsaved[r.id] = r to now
        pending.latest[r.id] = r
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
public class SagaDataException(message: String, cause: Throwable? = null) : IllegalStateException(message, cause)
