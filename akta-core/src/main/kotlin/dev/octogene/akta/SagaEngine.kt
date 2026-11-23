package dev.octogene.akta

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
    /** Told about every log entry, after its transaction commits: for metrics and hooks. See [SagaListener]. */
    private val listeners: List<SagaListener> = emptyList(),
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
            tx { claim(id, skipLocked, trace) }
        } else {
            Cycle(saga = null, next = null)
        }
        while (true) {
            val work = cycle.next ?: break
            val outcome = perform(work)
            cycle = try {
                // Records this outcome and, in the same transaction, claims the next step if it is due.
                tx { record(work, outcome, claimUntil = deadline) }
            } catch (e: Throwable) {
                if (e is CancellationException || e.isFatal()) throw e
                recordAfterFailure(work, outcome, e)
            }
        }
        // Read again only when no transaction gave us the saga (deadline already passed, row locked elsewhere).
        return cycle.saga ?: tx { repository.find(id) } ?: error("Saga $id not found")
    }

    /**
     * The transaction that recorded [outcome] (and claimed the next step) was rolled back. First, record
     * the outcome alone: if only the claim of the next step failed, the call's result must not be lost.
     * If that fails too (a throwing onCompleted/onFailed, a constraint…), the saga becomes STUCK with the
     * outcome kept for a human: otherwise the step would loop through lock expiry and check, forever.
     */
    private suspend fun recordAfterFailure(work: Work, outcome: Outcome, error: Throwable): Cycle =
        try {
            Cycle(tx { record(work, outcome, claimUntil = null) }.saga, next = null)
        } catch (e: Throwable) {
            if (e is CancellationException || e.isFatal()) throw e
            Cycle(tx { recordFailed(work, outcome, error) }, next = null)
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
            tx {
                val r = lock(id, skipLocked = true) ?: return@tx
                val now = currentTime()
                val inFlight = r.stepStatus == StepStatus.IN_PROGRESS && r.lockedUntil?.let { it > now } == true
                if (r.state.isFinished || r.stepStatus == StepStatus.STUCK || inFlight) return@tx
                // lastError keeps the step's own error: the resume failure goes to the log only.
                val postponed = r.copy(nextAttemptAt = maxOf(r.nextAttemptAt, now + config.resumeFailureDelay))
                save(postponed, now)
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

    suspend fun find(id: Uuid): SagaRecord? = tx { repository.find(id) }

    // ---- Reads for operators ----------------------------------------------------------------

    /**
     * Stuck sagas, stuck longest first: what an admin screen lists. Each carries its [SagaRecord.stuckReason],
     * which says what to do. For the next page, pass `StuckCursor.after(lastOfThePage)` as [after].
     */
    suspend fun findStuck(limit: Int = 100, types: Collection<String>? = null, after: StuckCursor? = null): List<SagaRecord> {
        require(limit >= 1) { "limit must be at least 1" }
        return tx { repository.findStuck(limit, types, after) }
    }

    /** Everything that happened to a saga, oldest first. */
    suspend fun history(sagaId: Uuid): List<SagaLogEntry> = tx { repository.history(sagaId) }

    /** What the request with this trace id did to sagas, oldest first. */
    suspend fun historyByTrace(traceId: String, limit: Int = 200): List<SagaLogEntry> {
        require(limit >= 1) { "limit must be at least 1" }
        return tx { repository.historyByTrace(traceId, limit) }
    }

    /**
     * Counts and ages of the unfinished sagas, for gauges and alerts: stuck sagas by reason, the oldest
     * unfinished saga, and how far behind the recovery job is (`now - oldestDueAt`). Sample it about once
     * a minute; it reads partial indexes only.
     */
    suspend fun stats(types: Collection<String>? = null): SagaStats = tx { repository.stats(currentTime(), types) }

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
            val deleted = tx { repository.purgeFinished(before, batchSize) }
            total += deleted
        } while (deleted == batchSize)
        return total
    }

    // ---- Manual actions on a stuck saga -----------------------------------------------------

    /**
     * Retries the stuck step. Forward, this EXECUTES the step again without checking first:
     * when the saga is stuck because a check could not tell, only use it once you have confirmed
     * by hand that the step was not applied (otherwise use [markDone] or [abort]).
     *
     * For an idempotent step, a retry replays with the same key: safe without looking first, while the service
     * remembers the key. [newKey] sends a fresh key instead: only once you have confirmed in the service that
     * the step was not applied (after [StuckReason.KEY_EXPIRED], or when the service keeps answering a failure
     * it remembered for the old key).
     */
    suspend fun retry(id: Uuid, actor: String, newKey: Boolean = false): SagaRecord =
        manual(id, actor, SagaEvents.MANUAL_RETRY) { r, now ->
            require(r.stepStatus == StepStatus.STUCK) { "Saga $id is not stuck" }
            require(newKey || r.stuckReason != StuckReason.KEY_EXPIRED) {
                "The idempotency key of saga $id may be forgotten by its service: confirm the step was not applied, " +
                    "then retry with newKey = true (or markDone, or abort)"
            }
            val generation = if (newKey) r.keyGeneration + 1 else r.keyGeneration
            // An idempotent step whose key was used may be applied: it is replayed, and stays UNKNOWN meanwhile.
            val idempotent = r.state == SagaState.RUNNING && definition(r.type).stepAt(r.stepIndex).idempotency != null
            val replay = idempotent && !newKey && r.keySince != null
            ManualChange(
                r.copy(
                    stepStatus = if (replay) StepStatus.UNKNOWN else StepStatus.READY, attempts = 0, checkFailures = 0, nextAttemptAt = now, lastError = null,
                    stuckReason = null, stuckAt = null,
                    keySince = if (newKey) null else r.keySince, keyGeneration = generation,
                ),
                detail = if (newKey) "newKey: generation $generation" else null,
            )
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
                "The current step of saga $id is ${r.stepStatus}: it was not applied"
            }
            val updated = if (currentStepApplied) {
                val step = definition(r.type).stepAt(r.stepIndex)
                repository.saveResult(r.id, r.stepIndex, step.name, manualOutput(r, step, output).first, now)
                startCompensation(r, r.stepIndex, "Aborted by $actor", now)
            } else {
                startCompensation(r, r.stepIndex - 1, "Aborted by $actor", now)
            }
            ManualChange(updated, detail = "currentStepApplied=$currentStepApplied")
        }

    /**
     * The stuck compensation was done by hand, or there was nothing to undo: the step is marked
     * compensated, and the saga goes on undoing the previous steps (or ends FAILED). Used wrongly, it
     * leaves an effect in the external system, as [markDone] would.
     */
    suspend fun markCompensated(id: Uuid, actor: String): SagaRecord =
        manual(id, actor, SagaEvents.MANUAL_COMPENSATED) { r, now ->
            require(r.state == SagaState.COMPENSATING && r.stepStatus == StepStatus.STUCK) { "Saga $id has no stuck compensation" }
            repository.markCompensated(r.id, r.stepIndex, now)
            ManualChange(compensateFrom(r, r.stepIndex - 1, now))
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
        return tx { deliver(sagaId, name, value, type, trace) }
    }

    private fun deliver(sagaId: Uuid, name: String, value: Any?, type: KType, trace: String?): SignalResult {
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
        // WAITING, or UNKNOWN: its timeout passed and a check failed, to be tried again. No call is in flight
        // either way (a check in flight is IN_PROGRESS), so the signal answers now what the check looks up.
        val listening = r.stepStatus == StepStatus.WAITING || r.stepStatus == StepStatus.UNKNOWN
        return if (r.stepIndex == index && listening) {
            applySignal(r, def, step, context(r, def), value, encoded, now)
            received("delivered")
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
        consumeStored(r, def, step, ctx, now, trace)?.let { return it }
        val deadline = now + step.timeout
        val waiting = r.copy(stepStatus = StepStatus.WAITING, nextAttemptAt = deadline, attempts = 0, lastError = null)
        save(waiting, now)
        log(r.id, r.stepIndex, step.name, SagaEvents.WAIT_STARTED, "signal '${step.signal}' until $deadline", now, trace)
        return Cycle(waiting, next = null)
    }

    /**
     * The signal of [step] received earlier and stored, if any, applied now: the saga moves on (or compensates),
     * and the next step is claimed. Null when none was received.
     */
    private fun consumeStored(
        r: SagaRecord,
        def: AnyDefinition,
        step: AnySignalStep,
        ctx: SagaContext<Any?>,
        now: Instant,
        trace: String?,
    ): Cycle? {
        val stored = repository.signal(r.id, step.signal) ?: return null
        val value = decode(stored, checkNotNull(step.outputType)) { "signal '${step.signal}' of saga ${r.id}" }
        val updated = applySignal(r, def, step, ctx, value, stored, now)
        // Logged after the change, as in deliver(): the event shows the saga as the signal left it.
        val detail = listOfNotNull("consumed (stored earlier)", describe(step, value)?.let { "ref=$it" }).joinToString(" ")
        log(r.id, r.stepIndex, step.name, SagaEvents.SIGNAL_RECEIVED, detail, now, trace)
        return claimLocked(updated, trace)
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
            startCompensation(r.copy(lastError = e.toString()), r.stepIndex - 1, e.toString(), now)
        }
        save(updated, now)
        finishIfNeeded(r, updated)
        return updated
    }

    /**
     * Abandons the saga: everything done so far is compensated. A saga that is waiting or ready
     * compensates at once. One with a call in flight, or an outcome still to check, compensates as soon
     * as that outcome is recorded, since the call may have been applied and must be undone too. A stuck
     * saga stays stuck until a human retries or aborts it. Refused for a finished saga: undoing it is a
     * new saga. [reason] is kept in [SagaRecord.cancelReason]; a second cancellation is only logged.
     */
    suspend fun cancel(id: Uuid, reason: String, actor: String): SagaRecord =
        manual(id, actor, SagaEvents.CANCEL_REQUESTED) { r, now ->
            require(!r.state.isFinished) { "Saga $id is finished: undoing it is a new saga" }
            if (r.state != SagaState.RUNNING) return@manual ManualChange(r, detail = reason)   // already compensating
            // The first cancellation counts: a second one is only logged.
            val requested = if (r.cancelRequestedAt != null) r else r.copy(cancelRequestedAt = now, cancelReason = reason)
            ManualChange(cancelIfRequested(requested, now), detail = reason)
        }

    /** A saga whose cancellation was requested compensates once no call of it is in flight. */
    private fun cancelIfRequested(r: SagaRecord, now: Instant): SagaRecord =
        if (r.cancelRequestedAt != null && r.state == SagaState.RUNNING &&
            (r.stepStatus == StepStatus.READY || r.stepStatus == StepStatus.WAITING)
        ) {
            startCompensation(r, r.stepIndex - 1, cancelled(r), now)
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
        tx {
            val r = lock(id) ?: throw NoSuchElementException("Saga $id not found")
            val now = currentTime()
            val (updated, detail) = change(r, now).let { it.record to it.detail }
            save(updated, now)
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
        /** An idempotent step whose key was already used: an earlier attempt may have been applied. */
        val replay: Boolean,
        /** The saga as it was before the claim: what it goes back to when the call could not be made. */
        val unclaimed: SagaRecord,
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
        /** No time was left to call before the lock expires: nothing was called. Logged as NOT_CALLED. */
        class NotCalled(reason: String) : Outcome("NotCalled", reason)
    }

    /** The logged event, e.g. "EXECUTE:Done". */
    private fun eventOf(work: Work, outcome: Outcome): String =
        if (outcome is Outcome.NotCalled) SagaEvents.NOT_CALLED else "${work.action.event}:${outcome.event}"

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
        val r = lock(id, skipLocked) ?: return Cycle(saga = null, next = null)
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
            r = lockExpired(r, lockedUntil, now)                 // the process died during the call
            save(r, now)
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
            save(cancelled, now)
            log(r.id, r.stepIndex, null, SagaEvents.CANCELLED, cancelled.cancelReason, now, trace)
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
                    val noSignal = "No signal '${signalStep.signal}' before the timeout"
                    if (!signalStep.checksOnTimeout) {
                        val blocked = stuck(r, StuckReason.NO_SIGNAL, noSignal, now)
                        save(blocked, now)
                        log(r.id, r.stepIndex, step.name, SagaEvents.WAIT_TIMED_OUT, null, now, trace)
                        return Cycle(blocked, next = null)
                    }
                    log(r.id, r.stepIndex, step.name, SagaEvents.WAIT_TIMED_OUT, null, now, trace)
                    r = r.copy(stepStatus = StepStatus.UNKNOWN, lastError = noSignal)   // the check decides, below
                }
                // UNKNOWN: its timeout check, claimed below. Unless a signal arrived during a check that then
                // failed: it was stored, and answers what the check would look up again.
                else -> consumeStored(r, def, signalStep, ctx, now, trace)?.let { return it }
            }
        }
        val idempotency = step.idempotency.takeIf { r.state == SagaState.RUNNING }
        val action = when {
            // An idempotent step's unknown outcome is replayed with the same key, never checked.
            r.stepStatus == StepStatus.UNKNOWN && idempotency == null -> Action.CHECK
            r.state == SagaState.RUNNING -> Action.EXECUTE
            else -> Action.COMPENSATE
        }
        var keySince = r.keySince
        val replay = idempotency != null && keySince != null
        if (idempotency != null) {
            val since = keySince ?: now
            // The whole call must end while the service still knows the key: past that, a replay could duplicate.
            if (now + config.callLock >= since + idempotency.keyRetention) {
                val expired = stuck(
                    r, StuckReason.KEY_EXPIRED,
                    "The idempotency key of step '${step.name}', first used at $since, may be forgotten by its " +
                        "service (kept ${idempotency.keyRetention}): a replay could duplicate",
                    now,
                )
                save(expired, now)
                log(r.id, r.stepIndex, step.name, SagaEvents.KEY_EXPIRED, expired.lastError, now, trace)
                return Cycle(expired, next = null)
            }
            keySince = since
        }
        val lockedUntil = now + config.callLock
        val claimed = r.copy(
            stepStatus = StepStatus.IN_PROGRESS,
            attempts = if (action == Action.CHECK) r.attempts else r.attempts + 1,
            lockedUntil = lockedUntil,
            nextAttemptAt = lockedUntil,      // the recovery job looks again when the lock expires
            keySince = keySince,
        )
        save(claimed, now)
        return Cycle(claimed, Work(claimed, step, action, ctx.forCall(keyOf(claimed, step, action)), trace, replay, unclaimed = r))
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
        val current = lock(work.saga.id) ?: return Cycle(saga = null, next = null)
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
            is Outcome.OutputUnstorable -> stuck(
                r, StuckReason.OUTPUT_UNSTORABLE,
                "The call was made, but its output could not be stored (${outcome.reason}): " +
                    "record it with markDone, or undo the step with abort",
                now,
            )
            is Outcome.Rejected -> startCompensation(r.copy(lastError = outcome.error), r.stepIndex - 1, outcome.error, now)
            // A replay that never left says nothing of the earlier attempt, which may be applied: still UNKNOWN.
            // A first attempt that never left did not use the key: its window has not started (keySince cleared).
            is Outcome.NotSent -> if (work.replay) {
                replayLater(r, "${outcome.error} (an earlier attempt may be applied)", now)
            } else {
                retryLater(r.copy(keySince = null), outcome.error, now)
            }
            is Outcome.Unknown -> when {
                work.step.idempotency == null -> r.copy(
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
            Outcome.CannotTell -> stuck(r, StuckReason.CANNOT_TELL, "Cannot tell whether step '${work.step.name}' was applied", now)
            // Counted apart from attempts (a lookup outage must not use up the executions), with backoff.
            // Past maxAttempts: STUCK, as when a check cannot tell, so that alerts see it and a human
            // can decide (markDone, abort, or retry once the step is known not to be applied).
            is Outcome.CheckFailed -> {
                val failures = current.checkFailures + 1
                if (failures >= config.maxAttempts) {
                    stuck(
                        r, StuckReason.CHECK_FAILING,
                        "Check keeps failing, step '${work.step.name}' may have been applied: ${outcome.error}", now,
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
                repository.markCompensated(r.id, r.stepIndex, now)
                compensateFrom(r, r.stepIndex - 1, now)
            }
            is Outcome.CompensationFailed -> retryLater(r, outcome.error, now)
            // What the claim changed goes back, due at once: no attempt used, the idempotency key not used.
            // The rest is kept: a cancellation requested meanwhile, for one.
            is Outcome.NotCalled -> work.unclaimed.let { before ->
                r.copy(
                    stepStatus = before.stepStatus, attempts = before.attempts, checkFailures = before.checkFailures,
                    keySince = before.keySince, nextAttemptAt = now,
                )
            }
        }
        // Cancelled while the call was in flight: the call's outcome is recorded, now compensate (it included).
        val updated = cancelIfRequested(decided, now)
        save(updated, now)
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
        val current = lock(work.saga.id) ?: return null
        val now = currentTime()
        if (!stillClaimedBy(current, work)) {
            log(work, SagaEvents.LATE_RESULT_IGNORED, describe(work, outcome), now)
            return current
        }
        if (outcome is Outcome.Done) repository.saveResult(current.id, current.stepIndex, work.step.name, outcome.output, now)
        val kept = if (outcome is Outcome.Done) ". The output is saved: markDone(id, null) continues with it" else ""
        val blocked = stuck(current, StuckReason.RECORD_FAILED, "Could not record ${describe(work, outcome)}: $error$kept", now)
            .copy(lockedUntil = null)
        save(blocked, now)
        log(work, SagaEvents.RECORD_FAILED, blocked.lastError, now)
        return blocked
    }

    /** False when the lock expired during the call and another instance took over. */
    private fun stillClaimedBy(current: SagaRecord, work: Work): Boolean =
        current.stepStatus == StepStatus.IN_PROGRESS &&
            current.stepIndex == work.saga.stepIndex &&
            current.lockedUntil == work.saga.lockedUntil

    // ---- Transitions ------------------------------------------------------------------------

    /**
     * Step done: move on to the next one, or complete the saga. A saga whose cancellation was requested
     * compensates instead, this step included, even when it was the last one.
     */
    private fun next(r: SagaRecord, def: AnyDefinition, now: Instant): SagaRecord {
        if (r.cancelRequestedAt != null && r.state == SagaState.RUNNING) return startCompensation(r, r.stepIndex, cancelled(r), now)
        val index = r.stepIndex + 1
        return if (index == def.steps.size) {
            r.copy(
                state = SagaState.COMPLETED, stepStatus = StepStatus.READY, lastError = null, stuckReason = null, stuckAt = null,
                keySince = null, keyGeneration = 0, checkFailures = 0,
            )
        } else {
            r.copy(
                stepIndex = index, stepStatus = StepStatus.READY, attempts = 0, checkFailures = 0, nextAttemptAt = now,
                lastError = null, stuckReason = null, stuckAt = null, keySince = null, keyGeneration = 0,
            )
        }
    }

    /**
     * Starts undoing the completed steps, from [index] down to the first one. [reason] is kept as the
     * saga's failureReason, which nothing changes afterwards: lastError may then be overwritten by a
     * compensation that fails and is retried.
     */
    private fun startCompensation(r: SagaRecord, index: Int, reason: String, now: Instant): SagaRecord {
        check(r.state == SagaState.RUNNING) { "Saga ${r.id} is already ${r.state}" }
        return compensateFrom(r.copy(failureReason = reason), index, now)
    }

    private fun cancelled(r: SagaRecord) = "Cancelled: ${r.cancelReason}"

    /** Undoes the completed steps, from [index] down to the first one. */
    private fun compensateFrom(r: SagaRecord, index: Int, now: Instant): SagaRecord =
        if (index < 0) {
            r.copy(
                state = SagaState.FAILED, stepStatus = StepStatus.READY, checkFailures = 0, stuckReason = null, stuckAt = null,
                keySince = null, keyGeneration = 0,
            )
        } else {
            r.copy(
                state = SagaState.COMPENSATING, stepIndex = index, stepStatus = StepStatus.READY, attempts = 0, checkFailures = 0,
                nextAttemptAt = now, stuckReason = null, stuckAt = null, keySince = null, keyGeneration = 0,
            )
        }

    /**
     * An idempotent step that may have been applied: it stays UNKNOWN (abort is refused, cancel waits) and is
     * replayed with the same key after backoff, until a replay answers. No settle delay: the service answers
     * a replay with the first result.
     */
    private fun replayLater(r: SagaRecord, error: String, now: Instant): SagaRecord =
        if (r.attempts >= config.maxAttempts) {
            stuck(r, StuckReason.TOO_MANY_ATTEMPTS, "Too many attempts: $error", now)
        } else {
            r.copy(stepStatus = StepStatus.UNKNOWN, nextAttemptAt = now + config.backoff(r.attempts), lastError = "$error; retrying with the same key")
        }

    private fun retryLater(r: SagaRecord, error: String, now: Instant): SagaRecord =
        if (r.attempts >= config.maxAttempts) {
            stuck(r, StuckReason.TOO_MANY_ATTEMPTS, "Too many attempts: $error", now)
        } else {
            r.copy(stepStatus = StepStatus.READY, nextAttemptAt = now + config.backoff(r.attempts), lastError = error)
        }

    /**
     * Lock expired. Forward: the call may have left, so the outcome is unknown and will be checked.
     * Compensation: compensations are safe to repeat, so it simply runs again, up to [SagaConfig.maxAttempts].
     */
    private fun lockExpired(r: SagaRecord, since: Instant, now: Instant): SagaRecord =
        if (r.state == SagaState.RUNNING && definition(r.type).stepAt(r.stepIndex).idempotency != null) {
            // Idempotent: replayed with the same key from the lock's expiry, as after an unknown outcome.
            if (r.attempts >= config.maxAttempts) {
                stuck(r, StuckReason.TOO_MANY_ATTEMPTS, "Too many attempts: lock expired during the call", now).copy(lockedUntil = null)
            } else {
                r.copy(
                    stepStatus = StepStatus.UNKNOWN, lockedUntil = null, nextAttemptAt = since,
                    lastError = "Lock expired during the call; retrying with the same key",
                )
            }
        } else if (r.state == SagaState.RUNNING) {
            r.copy(
                stepStatus = StepStatus.UNKNOWN, lockedUntil = null, nextAttemptAt = since + config.settleDelay,
                lastError = "Lock expired during the call",
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
    private fun keyOf(r: SagaRecord, step: AnyStep, action: Action): String {
        val base = "${r.id}/${step.name}/${if (action == Action.COMPENSATE) "compensate" else "execute"}"
        return if (r.keyGeneration > 0) "$base/${r.keyGeneration}" else base
    }

    /** The saga stops for a human, for [reason]. Leaving STUCK clears both (see [next], [compensateFrom], [retry]). */
    private fun stuck(r: SagaRecord, reason: StuckReason, error: String?, now: Instant): SagaRecord =
        r.copy(stepStatus = StepStatus.STUCK, stuckReason = reason, stuckAt = now, lastError = error)

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
        val entry = SagaLogEntry(sagaId, now, stepIndex, stepName, event, detail, actor, trace)
        repository.log(entry)
        // Reported after commit, with the saga as this transaction last read or wrote it. None from create(),
        // which runs in the caller's transaction.
        val pending = txEvents.get() ?: return
        val r = pending.latest[sagaId] ?: return
        pending.events += SagaEvent(entry, r.type, r.state, r.stepStatus, r.stuckReason)
    }

    // ---- Transactions, and the events they report after commit ---------------------------------

    /** The log entries of the transaction running on this thread, and the sagas it touched. */
    private class PendingEvents {
        val latest = HashMap<Uuid, SagaRecord>()
        val events = ArrayList<SagaEvent>()
    }

    /**
     * Set while the engine's transaction block runs. The block is plain blocking code on one thread
     * ([SagaTransactions.inTransaction] takes a non-suspending block), so a thread local is safe here.
     */
    private val txEvents = ThreadLocal<PendingEvents?>()

    /** One transaction; its events go to the listeners once it has committed. A retried attempt starts afresh. */
    private suspend fun <T> tx(block: () -> T): T {
        var committed: PendingEvents? = null
        val result = transactions.inTransaction {
            val pending = PendingEvents()
            committed = pending
            val outer = txEvents.get()
            txEvents.set(pending)
            try {
                block()
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

    private fun save(r: SagaRecord, now: Instant) {
        check((r.stepStatus == StepStatus.STUCK) == (r.stuckReason != null && r.stuckAt != null)) {
            "Saga ${r.id}: ${r.stepStatus} with stuck reason ${r.stuckReason}"
        }
        repository.save(r, now)
        txEvents.get()?.latest?.put(r.id, r)
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
