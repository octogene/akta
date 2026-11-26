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
    private val machine = SagaMachine(config) { type -> this.definitions[type]?.hooks ?: Hooks(completed = true, failed = true) }

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
     * @throws IllegalArgumentException if [definition] is not registered, or the encoded input is over
     *   [SagaConfig.maxDataBytes]. Nothing is written in either case.
     */
    @OptIn(ExperimentalUuidApi::class)
    public fun <I> create(definition: SagaDefinition<I>, key: String, input: I, traceparent: String? = null): Uuid {
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
        val record = machine.created(Uuid.generateV7(), definition.type, key, encoded, traceparent, now)
        repository.insert(record, now)
        log(record.id, record.stepIndex, null, SagaEvents.CREATED, null, now, trace = traceId())
        return record.id
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
        var cycle = if (currentTime() < deadline) {
            withContext(NonCancellable) { claimTx(id, skipLocked, trace) }
        } else {
            Cycle(saga = null, next = null)
        }
        while (true) {
            // The hook runs in a transaction of its own, after the outcome that ended the steps is committed.
            if (cycle.finish) {
                cycle = withContext(NonCancellable) { finish(id, trace) }
                continue
            }
            // A stored signal to consume after an outcome or another signal: in a transaction of its own, so that
            // what onSignal throws never rolls back what was recorded.
            if (cycle.claimAgain) {
                cycle = withContext(NonCancellable) { claimTx(id, skipLocked = false, trace) }
                continue
            }
            val work = cycle.next ?: break
            cycle = withContext(NonCancellable) {
                val outcome = perform(work)
                // Records this outcome and, in the same transaction, claims the next step if it is due,
                // unless the caller was cancelled meanwhile.
                val claimUntil = deadline.takeIf { caller.isActive }
                try {
                    tx { record(work, outcome, claimUntil) }
                } catch (e: Throwable) {
                    if (e.isFatal()) throw e
                    recordAfterFailure(work, outcome, e)
                }
            }
        }
        caller.ensureActive()
        // Read again only when no transaction gave us the saga (deadline already passed, row locked elsewhere).
        return cycle.saga ?: tx { repository.find(id) } ?: error("Saga $id not found")
    }

    /** A claim, in a transaction; if onSignal threw on a stored signal, the saga stops instead, the signal kept. */
    private suspend fun claimTx(id: Uuid, skipLocked: Boolean, trace: String?): Cycle =
        try {
            tx { claim(id, skipLocked, trace) }
        } catch (e: SignalFailed) {
            tx { blockOnStoredSignal(id, e, trace) }
        }

    /**
     * The transaction that recorded [outcome] (and claimed the next step) was rolled back. First, record
     * the outcome alone: if only the claim of the next step failed, the call's result must not be lost.
     * If that fails too (a constraint, the database…), the saga becomes STUCK with the
     * outcome kept for a human: otherwise the step would loop through lock expiry and check, forever.
     */
    private suspend fun recordAfterFailure(work: Work, outcome: Outcome, error: Throwable): Cycle =
        try {
            Cycle(tx { record(work, outcome, claimUntil = null) }.saga, next = null)
        } catch (e: Throwable) {
            if (e is CancellationException || e.isFatal()) throw e
            // e, not error: what stopped the outcome alone from being recorded. The first failure may have been
            // a passing one (a database error before onSignal ever ran).
            Cycle(tx { recordFailed(work, outcome, e) }, next = null)
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
     * short transaction each, so it never holds locks for long. Returns the number of sagas deleted.
     */
    public suspend fun purgeFinished(olderThan: Duration = 30.days, batchSize: Int = 1000): Int {
        require(olderThan.isPositive()) { "olderThan must be positive" }
        require(batchSize >= 1) { "batchSize must be at least 1" }
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
            machine.retry(r, newKey, now, stepOf = { definition(r.type).stepAt(r.stepIndex) }) { signal ->
                repository.signal(r.id, signal) != null
            }.let { ManualChange(it.record, it.detail) }
        }

    /**
     * The stuck step was found done by hand: move on to the next one. [output] is the step's output as
     * the engine's serializer writes it (JSON for [KotlinxJsonSagaSerializer]), checked against the
     * step's output type before it is saved. Null keeps the output already saved for the step (after a
     * `RECORD_FAILED`), or means no output.
     */
    public suspend fun markDone(id: Uuid, output: String?, actor: String, runFor: Duration = MANUAL_RUN): SagaRecord =
        manual(id, actor, SagaEvents.MANUAL_DONE, runFor) { r, now ->
            machine.checkMarkDone(r)
            val def = definition(r.type)
            val step = def.stepAt(r.stepIndex)
            val (encoded, decoded) = manualOutput(r, step, output)
            repository.saveResult(r.id, r.stepIndex, step.name, encoded, now)
            ManualChange(machine.next(r, def.steps.size, now), detail = describe(step, decoded)?.let { "ref=$it" })
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
            machine.checkAbort(r, currentStepApplied)
            if (currentStepApplied && r.state != SagaState.FINISHING) {
                val step = definition(r.type).stepAt(r.stepIndex)
                repository.saveResult(r.id, r.stepIndex, step.name, manualOutput(r, step, output).first, now)
            }
            ManualChange(machine.abort(r, currentStepApplied, actor, now), detail = "currentStepApplied=$currentStepApplied")
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
            ManualChange(decision.record)
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

    /** The signal step [step] starts waiting, or consumes at once a signal that arrived before it. */
    private fun enterWait(
        r: SagaRecord,
        def: AnyDefinition,
        step: AnySignalStep,
        ctx: SagaContext<Any?>,
        now: Instant,
        trace: String?,
        chained: Boolean,
    ): Cycle {
        consumeStored(r, def, step, ctx, now, trace, chained)?.let { return it }
        val waiting = machine.waiting(r, step, now)
        save(waiting, now)
        log(r.id, r.stepIndex, step.name, SagaEvents.WAIT_STARTED, "signal '${step.signal}' until ${waiting.nextAttemptAt}", now, trace)
        return Cycle(waiting, next = null)
    }

    /**
     * The signal of [step] received earlier and stored, if any, applied now: the saga moves on (or compensates),
     * and the next step is claimed. Null when none was received. [chained]: this transaction already recorded an
     * outcome or applied a signal, which onSignal must not roll back: the signal waits for a claim of its own.
     */
    private fun consumeStored(
        r: SagaRecord,
        def: AnyDefinition,
        step: AnySignalStep,
        ctx: SagaContext<Any?>,
        now: Instant,
        trace: String?,
        chained: Boolean,
    ): Cycle? {
        val stored = repository.signal(r.id, step.signal) ?: return null
        if (chained) {
            save(r, now)
            return Cycle(r, next = null, claimAgain = true)
        }
        val value = decode(stored, checkNotNull(step.outputType)) { "signal '${step.signal}' of saga ${r.id}" }
        val updated = applySignal(r, def, step, ctx, value, stored, now)
        // Logged after the change, as in deliver(): the event shows the saga as the signal left it.
        val detail = listOfNotNull("consumed (stored earlier)", describe(step, value)?.let { "ref=$it" }).joinToString(" ")
        log(r.id, r.stepIndex, step.name, SagaEvents.SIGNAL_RECEIVED, detail, now, trace)
        return claimLocked(updated, trace, chained = true)
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
        val updated = signalDecision(r, def, step, ctx, value, encoded, now)
        save(updated, now)
        return updated
    }

    /** What a signal makes of the saga, not saved yet: onSignal accepts it (next step) or rejects it (compensate). */
    private fun signalDecision(
        r: SagaRecord,
        def: AnyDefinition,
        step: AnySignalStep,
        ctx: SagaContext<Any?>,
        value: Any?,
        encoded: String,
        now: Instant,
    ): SagaRecord =
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
        val r = lock(id) ?: return Cycle(saga = null, next = null)
        val step = definition(r.type).steps[r.stepIndex] as? SignalStep<*, *>
        val now = currentTime()
        if (step == null || !machine.blocksOnStoredSignal(r, now)) return Cycle(r, next = null)
        val blocked = machine.blockedBy(r, failure.message, now)
        save(blocked, now)
        log(r.id, r.stepIndex, step.name, SagaEvents.RECORD_FAILED, blocked.lastError, now, trace)
        return Cycle(blocked, next = null)
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
            machine.cancel(r, reason, now).let { ManualChange(it.record, it.detail) }
        }

    private class ManualChange(val record: SagaRecord, val detail: String? = null)

    private suspend fun manual(
        id: Uuid,
        actor: String,
        event: String,
        runFor: Duration,
        change: (SagaRecord, Instant) -> ManualChange,
    ): SagaRecord {
        require(!runFor.isNegative()) { "runFor must not be negative" }
        val trace = traceId()
        val committed = tx {
            val r = lock(id) ?: throw NoSuchElementException("Saga $id not found")
            val now = currentTime()
            val (updated, detail) = change(r, now).let { it.record to it.detail }
            save(updated, now)
            // r: logged on the step the action was taken on
            log(r.id, r.stepIndex, null, event, detail, now, trace, actor)
            updated
        }
        val now = currentTime()
        if (!runFor.isPositive() || !machine.canMoveOn(committed, now)) return committed
        // The action is committed: a failure from here on is the saga's, not the action's.
        return try {
            run(id, deadline = now + runFor)
        } catch (e: Throwable) {
            if (e is CancellationException || e.isFatal()) throw e
            postpone(id, e, trace)
            try {
                tx { repository.find(id) } ?: committed
            } catch (e: Throwable) {
                if (e is CancellationException || e.isFatal()) throw e
                committed
            }
        }
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

    private class Work(
        val saga: SagaRecord,
        val step: AnyStep,
        val action: Action,
        val ctx: SagaContext<Any?>,
        val trace: String?,
        /** An idempotent step whose key was already used: an earlier attempt may have been applied. */
        val replay: Boolean,
        /** The saga as it was before the claim: what it goes back to when the call could not be made. */
        val unclaimed: SagaRecord,
    )

    /** The logged event, e.g. "EXECUTE:Done". */
    private fun eventOf(work: Work, outcome: Outcome): String =
        if (outcome is Outcome.NotCalled) SagaEvents.NOT_CALLED else "${work.action.event}:${outcome.event}"

    /** The event with the outcome's detail, e.g. "EXECUTE:Done ref=STK-1": for LATE_RESULT_IGNORED, RECORD_FAILED. */
    private fun describe(work: Work, outcome: Outcome): String =
        listOfNotNull(eventOf(work, outcome), outcome.detail).joinToString(" ")

    /**
     * What a transaction leaves: the saga as saved ([saga], or null when it is locked elsewhere or
     * missing), and the step it claimed, if any ([next]): the call to make before the next transaction. [finish]: the
     * saga's hook is due instead, to run in a transaction of its own. [claimAgain]: a stored signal is due, to consume
     * in a claim of its own.
     */
    private class Cycle(val saga: SagaRecord?, val next: Work?, val finish: Boolean = false, val claimAgain: Boolean = false)

    /** Transaction 1: claims the current step, if there is something to do now. */
    private fun claim(id: Uuid, skipLocked: Boolean, trace: String?): Cycle {
        val r = lock(id, skipLocked) ?: return Cycle(saga = null, next = null)
        return claimLocked(r, trace)
    }

    /**
     * Claims the current step of [current], whose row this transaction has locked, if it is due now. First the moves
     * that call nothing (a lock expired, a cancellation, a step with nothing to undo), each saved and logged here,
     * until the saga rests or its step is to be acted on; then the step's data is read and the call claimed.
     */
    private fun claimLocked(current: SagaRecord, trace: String?, chained: Boolean = false): Cycle {
        var r = current
        var now = currentTime()
        lateinit var def: AnyDefinition
        while (true) {
            when (val move = machine.move(r, now, definitionOf = { definition(r.type).also { def = it } }, stepOf = { def.stepAt(r.stepIndex) }) {
                definition(r.type).stepAt(r.stepIndex).idempotency != null
            }) {
                is Move.Rest -> return Cycle(move.record, next = null)
                is Move.Act -> { r = move.record; break }
                is Move.LockExpired -> {                         // the process died during the call: on, at the same time
                    r = move.record
                    save(r, now)
                    log(r.id, r.stepIndex, null, SagaEvents.LOCK_EXPIRED, r.lastError, now, trace)
                }
                is Move.Cancelled -> {
                    save(move.record, now)
                    log(r.id, r.stepIndex, null, SagaEvents.CANCELLED, move.record.cancelReason, now, trace)
                    r = move.record
                    now = currentTime()
                }
                is Move.NothingToUndo -> {
                    write(move.effects, r, now) { def.stepAt(r.stepIndex).name }
                    save(move.record, now)
                    log(r.id, r.stepIndex, def.stepAt(r.stepIndex).name, SagaEvents.NOTHING_TO_UNDO, null, now, trace)
                    r = move.record
                    now = currentTime()
                }
                is Move.Finish -> return Cycle(move.record, next = null, finish = true)
                is Move.NoSignal -> {
                    save(move.record, now)
                    log(r.id, r.stepIndex, def.stepAt(r.stepIndex).name, SagaEvents.WAIT_TIMED_OUT, null, now, trace)
                    return Cycle(move.record, next = null)
                }
            }
        }
        val step = def.stepAt(r.stepIndex)
        val signalStep = (step as? SignalStep<*, *>)?.takeIf { r.state == SagaState.RUNNING }
        // Decoded before anything is called: data that cannot be read throws here, and the transaction
        // rolls back. resumeDue then postpones the saga (another version of the code may read it).
        val ctx = context(r, def)
        if (signalStep != null) {
            @Suppress("UNCHECKED_CAST")
            signalStep as AnySignalStep
            when (r.stepStatus) {
                StepStatus.READY -> return enterWait(r, def, signalStep, ctx, now, trace, chained)
                StepStatus.WAITING -> {                  // its timeout passed without the signal: the check decides
                    log(r.id, r.stepIndex, step.name, SagaEvents.WAIT_TIMED_OUT, null, now, trace)
                    r = machine.timedOut(r, signalStep)
                }
                // CHECKING: its timeout check, claimed below. Unless a signal arrived during a check that then
                // failed: it was stored, and answers what the check would look up again.
                else -> consumeStored(r, def, signalStep, ctx, now, trace, chained)?.let { return it }
            }
        }
        return when (val claim = machine.claim(r, step, now)) {
            is Claim.KeyExpired -> {
                save(claim.record, now)
                log(r.id, r.stepIndex, step.name, SagaEvents.KEY_EXPIRED, claim.record.lastError, now, trace)
                Cycle(claim.record, next = null)
            }
            is Claim.Call -> {
                save(claim.record, now)
                val key = machine.keyOf(claim.record, step.name, claim.action)
                Cycle(claim.record, Work(claim.record, step, claim.action, ctx.forCall(key), trace, claim.replay, unclaimed = r))
            }
        }
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
     * Transaction 2: records the outcome and decides what comes next. While the row is locked, it also
     * claims the next step if it is due and [claimUntil] has not passed: one transaction instead of two.
     * With [claimUntil] null, it only records.
     */
    private fun record(work: Work, reported: Outcome, claimUntil: Instant?): Cycle {
        val current = lock(work.saga.id) ?: return Cycle(saga = null, next = null)
        val now = currentTime()
        if (!machine.stillClaimed(current, work.saga)) {
            log(work, SagaEvents.LATE_RESULT_IGNORED, describe(work, reported), now)
            return Cycle(current, next = null)
        }
        val outcome = reported
        val def = definition(current.type)
        val r = machine.released(current)
        // A signal step's check that found nothing, or could not tell: a signal received meanwhile answers instead.
        val signalStored = { signal: String -> repository.signal(r.id, signal) != null }
        val decided = machine.decide(r, current.checkFailures, outcome, work.step, def.steps.size, work.replay, work.unclaimed, now, signalStored)
            .also { write(it.effects, r, now) { work.step.name } }.record
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
        return if (claimUntil != null && currentTime() < claimUntil) {
            claimLocked(updated, work.trace, chained = true)
        } else {
            Cycle(updated, next = null)
        }
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
    private fun recordFailed(work: Work, outcome: Outcome, error: Throwable): SagaRecord? {
        val current = lock(work.saga.id) ?: return null
        val now = currentTime()
        if (!machine.stillClaimed(current, work.saga)) {
            log(work, SagaEvents.LATE_RESULT_IGNORED, describe(work, outcome), now)
            return current
        }
        val decision = machine.recordFailed(
            current, work.step, work.action, outcome, describe(work, outcome), error, onSignalFailed = error is SignalFailed, now,
        ) { signal -> repository.signal(current.id, signal) != null }
        write(decision.effects, current, now) { work.step.name }
        val blocked = decision.record
        save(blocked, now)
        log(work, SagaEvents.RECORD_FAILED, blocked.lastError, now)
        return blocked
    }

    // ---- Transitions ------------------------------------------------------------------------

    /** What a decision writes besides the row, for the current step of [r] (its name is only read to save a result). */
    private fun write(effects: List<Effect>, r: SagaRecord, now: Instant, stepName: () -> String) {
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
            Cycle(tx { hookFailedLocked(id, e, trace) }, next = null)
        }

    private fun finishLocked(id: Uuid, trace: String?): Cycle {
        val r = lock(id) ?: return Cycle(saga = null, next = null)
        val now = currentTime()
        if (r.state != SagaState.FINISHING || r.stepStatus != StepStatus.READY || r.nextAttemptAt > now) return Cycle(r, next = null)
        val def = definition(r.type)
        val ended = machine.finished(r)
        save(ended, now)
        log(r.id, r.stepIndex, null, SagaEvents.FINISHED, ended.state.name, now, trace)
        // Written before the hook, not when the transaction ends: the hook may read the saga table, or create the next
        // saga for the same key, which the unique index on unfinished sagas would refuse while this row read FINISHING.
        txEvents.get()?.let(::writeUnsaved)
        val ctx = context(ended, def)
        if (ended.state == SagaState.COMPLETED) def.onCompleted(ctx) else def.onFailed(ctx)
        return Cycle(ended, next = null)
    }

    private fun hookFailedLocked(id: Uuid, error: Throwable, trace: String?): SagaRecord? {
        val r = lock(id) ?: return null
        // Committed after all (the failure came after the commit), or finished elsewhere: nothing to count.
        if (r.state != SagaState.FINISHING || r.stepStatus != StepStatus.READY) return r
        val now = currentTime()
        val failed = machine.hookFailed(r, error.toString(), now)
        save(failed, now)
        log(r.id, r.stepIndex, null, SagaEvents.HOOK_FAILED, failed.lastError, now, trace)
        return failed
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

    private fun lock(id: Uuid, skipLocked: Boolean = false): SagaRecord? =
        repository.lock(id, skipLocked)?.also { txEvents.get()?.latest?.put(it.id, it) }

    /**
     * Saves [record] when the transaction's block ends, so that a saga changed several times in one transaction is
     * written once. Nothing in a transaction reads a saga row back after changing it: each works on the
     * record it locked. Outside the engine's transactions (none today), it writes at once.
     */
    private fun save(record: SagaRecord, now: Instant) {
        val r = record.copy(
            lastError = record.lastError?.storable(),
            failureReason = record.failureReason?.storable(),
            cancelReason = record.cancelReason?.storable(),
        )
        check((r.stepStatus == StepStatus.STUCK) == (r.stuckReason != null && r.stuckAt != null)) {
            "Saga ${r.id}: ${r.stepStatus} with stuck reason ${r.stuckReason}"
        }
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
