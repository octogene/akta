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

/*
 * How the engine is put together, for whoever works on it:
 *
 * - SagaEngine (this file): the public API, and the cycle of each step: claim, call, record, finish.
 * - SagaMachine: every decision on a saga, pure. It takes a SagaPhase and returns the SagaPhase it becomes.
 * - SagaPhase: a saga's row, typed, so that a combination the engine must never write cannot be built.
 * - SagaStore: the transactions, the row read as a phase and saved back, the log, and listener events.
 * - SagaSignals: signals, delivered by signal() and consumed through onSignal when the saga runs.
 * - SagaManualActions: retry, markDone, abort, markCompensated and cancel.
 */

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
    /** Transactions, and what is read and written in them. */
    private val store = SagaStore(transactions, repository, listeners, clock)

    /** Every decision on a saga's row: pure, and tested on its own. */
    private val machine = SagaMachine(config) { type -> this.definitions[type] }

    @Suppress("UNCHECKED_CAST")
    private val definitions: Map<String, AnyDefinition> = definitions.associateBy { it.type } as Map<String, AnyDefinition>

    private val signals = SagaSignals(store, machine, serializer, config, ::definition)

    private val manualActions = SagaManualActions(
        store, machine, serializer, config, traceId, ::definition,
        runOn = { id, deadline -> run(id, deadline) },
        postpone = ::postpone,
    )

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
        val now = store.now()
        val saga = machine.created(Uuid.generateV7(), definition.type, key, encoded, traceparent, now)
        repository.insert(saga.record(), now)
        store.log(saga.id, saga.stepIndex, null, SagaEvents.CREATED, null, now, trace = traceId())
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
        val trace = traceId()      // read here, on the caller's coroutine: transactions may switch threads
        // Once a step is claimed, it is called and its outcome recorded even if the caller is cancelled (an HTTP
        // client disconnects, the pod shuts down). Abandoned, the step would stay IN_PROGRESS until its lock
        // expires, then count as an unknown outcome. So each transaction and call below runs NonCancellable, the
        // claim included: once its transaction commits, the call must follow. The caller's cancellation only stops
        // the next claim, and run() rethrows it at the end, at most callLock later.
        val caller = currentCoroutineContext()
        caller.ensureActive()
        var cycle: Cycle = if (store.now() < deadline) {
            withContext(NonCancellable) { claimTx(id, skipLocked, trace) }
        } else {
            Cycle.Rest(saga = null)
        }
        while (true) {
            cycle = when (val current = cycle) {
                is Cycle.Rest -> break
                is Cycle.Call -> withContext(NonCancellable) {
                    val outcome = perform(current.work)
                    // Record the outcome. In the same transaction, claim the next step if it is due, unless the
                    // caller was cancelled meanwhile.
                    recordTx(current.work, outcome, claimUntil = deadline.takeIf { caller.isActive })
                }
                // The hook runs in a transaction of its own, once the outcome that ended the steps is committed.
                is Cycle.Finish -> withContext(NonCancellable) { finish(id, trace) }
                // A stored signal is due after an outcome or another signal. It gets a transaction of its own, so
                // that onSignal throwing cannot roll back what was just recorded.
                is Cycle.ClaimAgain -> withContext(NonCancellable) { claimTx(id, skipLocked = false, trace) }
            }
        }
        caller.ensureActive()
        // Read the saga again only if no transaction returned it (deadline already passed, row locked elsewhere,
        // a late result ignored).
        return cycle.saga?.record() ?: store.tx { repository.find(id) } ?: error("Saga $id not found")
    }

    /**
     * Transaction 1: claims the saga's next call. If onSignal threw on a signal stored earlier, the claim rolls back
     * and a second transaction stops the saga instead, keeping the signal.
     */
    private suspend fun claimTx(id: Uuid, skipLocked: Boolean, trace: String?): Cycle =
        try {
            store.tx { claim(id, skipLocked, trace) }
        } catch (e: SignalFailed) {
            Cycle.Rest(store.tx { signals.blockOnStoredSignal(id, e, trace) })
        }

    /**
     * Transaction 2: records [outcome]. While the row is locked, it also claims the next step, if it is due and
     * [claimUntil] has not passed: one transaction instead of two. With [claimUntil] null, it only records.
     */
    private suspend fun recordTx(work: Work, outcome: Outcome, claimUntil: Instant?): Cycle =
        try {
            store.tx {
                val recorded = record(work, outcome) ?: return@tx Cycle.Rest(saga = null)
                if (claimUntil != null && store.now() < claimUntil) {
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
     * Called when the transaction that recorded [outcome] (and claimed the next step) was rolled back.
     *
     * It first records the outcome alone: if only the claim of the next step failed, the call's result is kept.
     * If that fails too (a constraint, the database), [recordFailed] stops the saga (STUCK) and keeps the outcome
     * for a human. Without that, the step would cycle through lock expiry and check forever.
     */
    private suspend fun recordAfterFailure(work: Work, outcome: Outcome, error: Throwable): Cycle =
        try {
            Cycle.Rest(store.tx { record(work, outcome) })
        } catch (e: Throwable) {
            if (e is CancellationException || e.isFatal()) throw e
            // Report e, not error: e is what stopped the outcome alone from being recorded. The first failure may
            // have been a passing one (a database error before onSignal ever ran).
            Cycle.Rest(store.tx { recordFailed(work, outcome, e) })
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
        val ids = store.tx { repository.findDue(store.now(), limit, definitions.keys) }
        val permits = Semaphore(concurrency)
        val failures = coroutineScope {
            ids.map { id ->
                async {
                    permits.withPermit {
                        try {
                            run(id, deadline = store.now() + budgetPerSaga, skipLocked = true)
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
     * Called when [run] threw, in [resumeDue] or after a manual action (a database error, a bug). It moves the saga
     * back in the queue, so that sagas failing on every attempt cannot fill each batch and starve the others.
     *
     * After [SagaConfig.maxAttempts] such failures in a row, the saga becomes STUCK ([StuckReason.RESUME_FAILING]).
     * Postponed forever, it would never reach an operator, and would fill its log. Best effort: if this fails
     * too, the error is already reported by [resumeDue].
     */
    private suspend fun postpone(id: Uuid, error: Throwable, trace: String?) {
        try {
            store.tx {
                val saga = store.lock(id, skipLocked = true) ?: return@tx
                val now = store.now()
                val postponed = machine.postpone(saga, error.toString(), now) ?: return@tx
                store.save(postponed, now)
                store.log(saga.id, saga.stepIndex, null, SagaEvents.RESUME_FAILED, error.toString(), now, trace)
            }
        } catch (e: Throwable) {
            // The database is probably down: nothing more to do. Swallow any non-fatal error, as resumeDue does:
            // escaping, it would end the batch, or fail a manual action that is already committed.
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

    public suspend fun find(id: Uuid): SagaRecord? = store.tx { repository.find(id) }

    // ---- Reads for operators ----------------------------------------------------------------

    /**
     * Stuck sagas, stuck longest first: what an admin screen lists. Each carries its [SagaRecord.stuckReason],
     * which says what to do. For the next page, pass `StuckCursor.after(lastOfThePage)` as [after].
     */
    public suspend fun findStuck(limit: Int = 100, types: Collection<String>? = null, after: StuckCursor? = null): List<SagaRecord> {
        require(limit >= 1) { "limit must be at least 1" }
        return store.tx { repository.findStuck(limit, types, after) }
    }

    /**
     * The sagas of a business key, newest first: the unfinished one if any, then the finished ones still kept.
     * What an operator searches by ("why is item 3034… stuck?"); then [history] for one of them.
     */
    public suspend fun findByKey(key: String, limit: Int = 20): List<SagaRecord> {
        require(limit >= 1) { "limit must be at least 1" }
        return store.tx { repository.findByKey(key, limit) }
    }

    /** Everything that happened to a saga, oldest first. */
    public suspend fun history(sagaId: Uuid): List<SagaLogEntry> = store.tx { repository.history(sagaId) }

    /** What the request with this trace id did to sagas, oldest first. */
    public suspend fun historyByTrace(traceId: String, limit: Int = 200): List<SagaLogEntry> {
        require(limit >= 1) { "limit must be at least 1" }
        return store.tx { repository.historyByTrace(traceId, limit) }
    }

    /**
     * Counts and ages of the unfinished sagas, for gauges and alerts: stuck sagas by reason, the oldest
     * unfinished saga, and how far behind the recovery job is (`now - oldestDueAt`). It reads every
     * unfinished saga (one grouped query on PostgreSQL), never the finished ones: sample it about once a
     * minute, not per request.
     */
    public suspend fun stats(types: Collection<String>? = null): SagaStats = store.tx { repository.stats(store.now(), types) }

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
        val before = store.now() - olderThan
        var total = 0
        do {
            val deleted = store.tx { repository.purgeFinished(before, batchSize) }
            total += deleted
        } while (deleted == batchSize)
        return total
    }

    // ---- Manual actions on a stuck saga -----------------------------------------------------
    //
    // Each action commits its change, then runs the saga on for up to runFor (10 s by default). See
    // SagaManualActions, which does the work, for what happens when the saga cannot move on, or fails to.

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
        manualActions.retry(id, actor, newKey, runFor)

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
        manualActions.markDone(id, output, actor, runFor)

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
        manualActions.abort(id, actor, currentStepApplied, output, runFor)

    /**
     * The stuck compensation was done by hand, or there was nothing to undo: the step is marked
     * compensated, and the saga goes on undoing the previous steps (or ends FAILED). Used wrongly, it
     * leaves an effect in the external system, as [markDone] would.
     */
    public suspend fun markCompensated(id: Uuid, actor: String, runFor: Duration = MANUAL_RUN): SagaRecord =
        manualActions.markCompensated(id, actor, runFor)

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
        return store.tx { signals.deliver(sagaId, name, value, type, trace) }
    }

    /**
     * Abandons the saga: everything done so far is compensated. A saga that is waiting or ready
     * compensates at once. One with a call in flight, or an outcome still to check, compensates as soon
     * as that outcome is recorded, since the call may have been applied and must be undone too. A stuck
     * saga stays stuck until a human retries or aborts it. Refused for a finished saga: undoing it is a
     * new saga. [reason] is kept in [SagaRecord.cancelReason]; a second cancellation is only logged.
     */
    public suspend fun cancel(id: Uuid, reason: String, actor: String, runFor: Duration = MANUAL_RUN): SagaRecord =
        manualActions.cancel(id, reason, actor, runFor)

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
        val saga = store.lock(id, skipLocked) ?: return Cycle.Rest(saga = null)
        return claimLocked(saga, trace)
    }

    /**
     * Claims the call of [current]'s step, if one is due now. The caller's transaction has locked the row.
     *
     * First it applies every change that needs no call, saving and logging each one: an expired lock, a
     * cancellation, a step with nothing to undo, a signal step that starts waiting, a signal that arrived earlier.
     * After each change it looks at the saga again, until the saga rests or a call is claimed.
     *
     * [chained] is true when this transaction already recorded an outcome or applied a signal. A stored signal then
     * waits for a transaction of its own ([Cycle.ClaimAgain]), so that onSignal throwing cannot roll that back.
     */
    private fun claimLocked(current: SagaPhase, trace: String?, chained: Boolean = false): Cycle {
        var saga = current
        var afterChange = chained
        while (true) {
            val now = store.now()
            var acting = when (val move = machine.move(saga, now)) {
                is Move.Rest -> return Cycle.Rest(move.saga)
                is Move.Finish -> return Cycle.Finish(move.saga)
                is Move.NoSignal -> {
                    store.save(move.saga, now)
                    store.log(saga.id, saga.stepIndex, stepName(saga), SagaEvents.WAIT_TIMED_OUT, null, now, trace)
                    return Cycle.Rest(move.saga)
                }
                is Move.LockExpired -> {                         // the process died during the call
                    store.save(move.saga, now)
                    store.log(saga.id, saga.stepIndex, null, SagaEvents.LOCK_EXPIRED, move.saga.lastError, now, trace)
                    saga = move.saga
                    continue
                }
                is Move.Cancelled -> {
                    store.save(move.saga, now)
                    store.log(saga.id, saga.stepIndex, null, SagaEvents.CANCELLED, move.saga.cancellation?.reason, now, trace)
                    saga = move.saga
                    continue
                }
                is Move.NothingToUndo -> {
                    store.write(move.effects, saga, now) { stepName(saga) }
                    store.save(move.saga, now)
                    store.log(saga.id, saga.stepIndex, stepName(saga), SagaEvents.NOTHING_TO_UNDO, null, now, trace)
                    saga = move.saga
                    continue
                }
                is Move.Act -> move.saga
            }
            val def = definition(acting.type)
            val step = def.stepAt(acting.stepIndex)
            // Decoded before any call. Data that cannot be read throws here and rolls the transaction back; resumeDue
            // then postpones the saga, since another version of the code may be able to read it.
            val ctx = context(acting, def)
            val signalStep = if (step is SignalStep<*, *> && acting is SagaPhase.Running) def.signalStepAt(acting.stepIndex) else null
            if (signalStep != null && acting.step is StepState.Waiting) {
                // The timeout passed without the signal: the step's timeout check decides, claimed below.
                store.log(acting.id, acting.stepIndex, step.name, SagaEvents.WAIT_TIMED_OUT, null, now, trace)
                acting = machine.timedOut(acting, signalStep)
            } else if (signalStep != null) {
                // READY (about to wait) or CHECKING (a timeout check to claim). Either way, a signal that already
                // arrived answers first: it is what the check would look up.
                val stored = repository.signal(acting.id, signalStep.signal)
                if (stored != null && afterChange) {
                    store.save(acting, now)
                    return Cycle.ClaimAgain(acting)
                }
                if (stored != null) {
                    saga = signals.consumeStored(acting, def, signalStep, ctx, stored, now, trace)
                    afterChange = true
                    continue
                }
                if (acting.step is StepState.Ready) return Cycle.Rest(signals.startWaiting(acting, signalStep, now, trace))
            }
            return when (val claim = machine.claim(acting, step, now)) {
                is Claim.KeyExpired -> {
                    store.save(claim.saga, now)
                    store.log(acting.id, acting.stepIndex, step.name, SagaEvents.KEY_EXPIRED, claim.saga.lastError, now, trace)
                    Cycle.Rest(claim.saga)
                }
                is Claim.Call -> {
                    store.save(claim.saga, now)
                    val key = machine.keyOf(claim.saga, step.name, claim.action)
                    Cycle.Call(Work(claim.saga, step, claim.action, ctx.forCall(key), trace, claim.replay, unclaimed = acting))
                }
            }
        }
    }

    private fun stepName(saga: SagaPhase): String = definition(saga.type).stepAt(saga.stepIndex).name

    /**
     * The external call, outside any transaction. It is cut off [recordMargin] before the step's lock expires, so
     * that its outcome is recorded while this engine still owns the step. A call that ran past its lock could land
     * after another instance already checked the step, or ran its compensation again. The lock was taken in the
     * claim transaction, so its clock is already running.
     */
    private suspend fun perform(work: Work): Outcome {
        val lockedUntil = (work.saga.step as? StepState.InFlight)?.lockedUntil ?: error("Claimed saga ${work.saga.id} without a lock")
        val budget = lockedUntil - store.now() - recordMargin
        // A long pause since the claim (GC, a stalled thread) can leave no time at all. withTimeout would then fail
        // before the step runs, and count as a failed call (an attempt used, an outcome to check). Nothing was
        // called, so the claim is undone instead.
        if (budget <= Duration.ZERO) {
            return Outcome.NotCalled("No time left to call before the lock expires (${-budget} late): nothing was called")
        }
        return try {
            withTimeout(budget) { call(work) }
        } catch (e: CancellationException) {
            // run() shields this from its caller's cancellation, so this is a timeout, ours or the step's own: a
            // failed call. If anything else cancelled it, rethrow: the step stays IN_PROGRESS until its lock expires.
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
                // A signal step's check found the signal's value: record() stores it as the signal, and onSignal
                // decides on it at the step's next claim, as for a signal delivered.
                is CheckResult.Done -> done(work, c.output).let { done ->
                    if (work.step is SignalStep<*, *> && done is Outcome.Done) Outcome.Found(done) else done
                }
                // A signal step's check found nothing at its timeout: the signal will not come, so the saga gives up
                // (unless a signal arrived meanwhile: record() looks).
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
     * The call returned [output]: encode it now, outside any transaction. The call was made, so if its output
     * cannot be stored, the step must not run again: it becomes STUCK (OUTPUT_UNSTORABLE) for a human.
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
            // A check that finds the call was refused (it throws StepRejected) answers as execute would: the saga
            // compensates. Any other error means the lookup failed: check again later.
            Action.CHECK -> if (e is StepRejected) Outcome.Rejected(error) else Outcome.CheckFailed(error)
            Action.COMPENSATE -> Outcome.CompensationFailed(error)
        }
    }

    /**
     * Records the outcome of [work]'s call and decides what comes next. Returns the saga as saved, or null when
     * nothing was recorded: the saga is gone, or this call is no longer the one in flight (logged
     * LATE_RESULT_IGNORED).
     */
    private fun record(work: Work, outcome: Outcome): SagaPhase? {
        val current = store.lock(work.saga.id) ?: return null
        val now = store.now()
        if (!machine.stillClaimed(current, work.saga)) {
            log(work, SagaEvents.LATE_RESULT_IGNORED, describe(work, outcome), now)
            return null
        }
        current as SagaPhase.Unfinished                       // still claimed: its call is in flight
        // For a signal step's check that found nothing or could not tell: a signal received meanwhile answers instead.
        val signalStored = { signal: String -> repository.signal(current.id, signal) != null }
        val decided = machine.decide(current, outcome, work.replay, work.unclaimed, now, signalStored)
            .also { store.write(it.effects, current, now) { work.step.name } }.saga
        // Cancelled while the call was in flight: the outcome is recorded, and the saga now compensates, this step
        // included if it was applied.
        val updated = machine.cancelIfRequested(decided, now)
        store.save(updated, now)
        // Log the new lastError if it changed (it may add "Too many attempts"), else the outcome's own detail, so
        // that a repeated error is logged every time, not only the first.
        val detail = when {
            outcome is Outcome.Done || outcome is Outcome.Found -> outcome.detail
            else -> updated.lastError.takeIf { it != current.lastError } ?: outcome.detail
        }
        // Logged on the step that ran, not on the one the saga moved to.
        log(work, eventOf(work, outcome), detail, now)
        return updated
    }

    /**
     * Last resort, when even the outcome alone could not be recorded (see [recordAfterFailure]). The saga becomes
     * STUCK (RECORD_FAILED) on the step that ran, for a human to resolve once the cause is fixed. What the outcome
     * established is kept, and lastError says which action follows from it:
     *
     * - Done: the output is saved with the step's result (never in lastError: it may hold personal
     *   data), and `markDone(id, null)` continues with it.
     * - Compensated: the step is marked undone, and `markCompensated` goes on, without undoing it again.
     * - Rejected: nothing was applied. The refusal is kept as failureReason, which makes markDone refuse
     *   the saga (it would turn a refusal into a success) and abort keep it as the reason.
     * - Any other outcome (only a database failure can stop those, no user code runs): look in the external
     *   system, then markDone, retry or abort.
     *
     * Returns null, as [record] does, when nothing was recorded: the saga is gone, or the call is no longer ours.
     */
    private fun recordFailed(work: Work, outcome: Outcome, error: Throwable): SagaPhase? {
        val current = store.lock(work.saga.id) ?: return null
        val now = store.now()
        if (!machine.stillClaimed(current, work.saga)) {
            log(work, SagaEvents.LATE_RESULT_IGNORED, describe(work, outcome), now)
            return null
        }
        current as SagaPhase.Unfinished                       // still claimed: its call is in flight
        val decision = machine.recordFailed(
            current, work.step, work.action, outcome, describe(work, outcome), error, onSignalFailed = error is SignalFailed, now,
        ) { signal -> repository.signal(current.id, signal) != null }
        store.write(decision.effects, current, now) { work.step.name }
        val blocked = decision.saga
        store.save(blocked, now)
        log(work, SagaEvents.RECORD_FAILED, blocked.lastError, now)
        return blocked
    }

    // ---- Finishing: the hook, in a transaction of its own ---------------------------------------------------

    /**
     * Runs a finishing saga's hook (onCompleted or onFailed) in a transaction that also ends the saga: COMPLETED or
     * FAILED. The outcome that ended its steps is already committed, so a hook that throws loses nothing. It is
     * retried after backoff, and after [SagaConfig.maxAttempts] failures the saga stops (HOOK_FAILED). A transaction
     * that fails for any other reason counts the same.
     */
    private suspend fun finish(id: Uuid, trace: String?): Cycle =
        try {
            store.tx { finishLocked(id, trace) }
        } catch (e: Throwable) {
            if (e is CancellationException || e.isFatal()) throw e
            Cycle.Rest(store.tx { hookFailedLocked(id, e, trace) })
        }

    private fun finishLocked(id: Uuid, trace: String?): Cycle {
        val saga = store.lock(id) ?: return Cycle.Rest(saga = null)
        val now = store.now()
        if (saga !is SagaPhase.Finishing || saga.step !is StepState.Ready || saga.due > now) return Cycle.Rest(saga)
        val def = definition(saga.type)
        val ended = machine.finished(saga)
        store.save(ended, now)
        store.log(saga.id, saga.stepIndex, null, SagaEvents.FINISHED, ended.state.name, now, trace)
        // Write the row now, before the hook, not at commit. The hook may read the saga table, or create the next saga
        // for the same key, which the unique index on unfinished sagas refuses while this row still says FINISHING.
        store.flush()
        val ctx = context(ended, def)
        if (ended.state == SagaState.COMPLETED) def.onCompleted(ctx) else def.onFailed(ctx)
        return Cycle.Rest(ended)
    }

    private fun hookFailedLocked(id: Uuid, error: Throwable, trace: String?): SagaPhase? {
        val saga = store.lock(id) ?: return null
        // The transaction committed after all (it failed after the commit), or another instance finished the saga:
        // nothing to count.
        if (saga !is SagaPhase.Finishing || saga.step !is StepState.Ready) return saga
        val now = store.now()
        val failed = machine.hookFailed(saga, error.toString(), now)
        store.save(failed, now)
        store.log(saga.id, saga.stepIndex, null, SagaEvents.HOOK_FAILED, failed.lastError, now, trace)
        return failed
    }

    // ---- Saga data --------------------------------------------------------------------------

    /** The saga's input and the outputs of its completed steps, decoded. Throws [SagaDataException]. */
    private fun context(saga: SagaPhase, def: AnyDefinition): SagaContext<Any?> {
        val inputType = def.inputType
        val input = if (inputType == null) {
            Unit
        } else {
            val text = saga.identity.input ?: throw SagaDataException("Saga ${saga.id} has no stored input")
            serializer.decodeSaved(text, inputType) { "the input of saga ${saga.id}" }
        }
        val stored = repository.results(saga.id)
        val outputs = def.steps.filter { it.name in stored }.associate { step ->
            val type = step.outputType
            val text = stored[step.name]
            step.name to when {
                type == null -> Unit
                text == null -> null
                else -> serializer.decodeSaved(text, type) { "the output of step '${step.name}' of saga ${saga.id}" }
            }
        }
        return SagaContext(saga.id, saga.identity.key, input, outputs)
    }

    /** Logs an event on the step that [work] ran. */
    private fun log(work: Work, event: String, detail: String?, now: Instant) =
        store.log(work.saga.id, work.saga.stepIndex, work.step.name, event, detail, now, work.trace)

    private fun definition(type: String): AnyDefinition =
        definitions[type] ?: throw IllegalArgumentException("Unknown saga type: $type")
}
