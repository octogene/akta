package dev.octogene.akta

import kotlin.coroutines.cancellation.CancellationException
import kotlin.time.Duration
import kotlin.time.Instant
import kotlin.uuid.Uuid

/**
 * The manual actions on a saga, documented on [SagaEngine]: [retry], [markDone], [abort], [markCompensated], [cancel].
 *
 * Each action commits its change, then runs the saga on for up to runFor (10 s by default), as [SagaEngine.run]
 * does: a call started before that may take up to callLock more. Duration.ZERO returns at once and leaves the saga to
 * resumeDue, which picks it up within its period. Nothing runs when the saga cannot move on (finished, stuck, a call
 * in flight, not due). The change stays committed whatever happens next: if running the saga on fails (the
 * database, data that cannot be read, a type this engine does not know), the action still returns the saga,
 * postponed and logged as RESUME_FAILED, as in resumeDue.
 */
internal class SagaManualActions(
    private val store: SagaStore,
    private val machine: SagaMachine,
    private val serializer: SagaSerializer,
    private val config: SagaConfig,
    private val traceId: () -> String?,
    private val definition: (type: String) -> AnyDefinition,
    /** Runs the saga on, as [SagaEngine.run]. */
    private val runOn: suspend (id: Uuid, deadline: Instant) -> SagaRecord,
    /** After [runOn] threw: the saga moved back in the queue, as in [SagaEngine.resumeDue]. */
    private val postpone: suspend (id: Uuid, error: Throwable, trace: String?) -> Unit,
) {
    private val repository: SagaRepository get() = store.repository

    /** See [SagaEngine.retry]. */
    suspend fun retry(id: Uuid, actor: String, newKey: Boolean, runFor: Duration): SagaRecord =
        manual(id, actor, SagaEvents.MANUAL_RETRY, runFor) { r, now ->
            machine.retry(r, newKey, now) { signal ->
                repository.signal(r.id, signal) != null
            }.let { Change(it.saga, it.detail) }
        }

    /** See [SagaEngine.markDone]. */
    suspend fun markDone(id: Uuid, output: String?, actor: String, runFor: Duration): SagaRecord =
        manual(id, actor, SagaEvents.MANUAL_DONE, runFor) { r, now ->
            val running = machine.checkMarkDone(r)
            val def = definition(r.type)
            val step = def.stepAt(r.stepIndex)
            if (step is SignalStep<*, *>) return@manual signalByHand(running, def.signalStepAt(r.stepIndex), output, now)
            val (encoded, decoded) = manualOutput(r.id, step, output)
            repository.saveResult(r.id, r.stepIndex, step.name, encoded, now)
            Change(machine.next(running, def.steps.size, now), detail = describe(step, decoded)?.let { "ref=$it" })
        }

    /** See [SagaEngine.abort]. */
    suspend fun abort(id: Uuid, actor: String, currentStepApplied: Boolean, output: String?, runFor: Duration): SagaRecord =
        manual(id, actor, SagaEvents.MANUAL_ABORT, runFor) { r, now ->
            val saga = machine.checkAbort(r, currentStepApplied)
            if (currentStepApplied && saga !is SagaPhase.Finishing) {
                val step = definition(r.type).stepAt(r.stepIndex)
                repository.saveResult(r.id, r.stepIndex, step.name, manualOutput(r.id, step, output).first, now)
            }
            Change(machine.abort(saga, currentStepApplied, actor, now), detail = "currentStepApplied=$currentStepApplied")
        }

    /** See [SagaEngine.markCompensated]. */
    suspend fun markCompensated(id: Uuid, actor: String, runFor: Duration): SagaRecord =
        manual(id, actor, SagaEvents.MANUAL_COMPENSATED, runFor) { r, now ->
            val decision = machine.markCompensated(r, now)
            store.write(decision.effects, r, now) { definition(r.type).stepAt(r.stepIndex).name }
            Change(decision.saga)
        }

    /** See [SagaEngine.cancel]. */
    suspend fun cancel(id: Uuid, reason: String, actor: String, runFor: Duration): SagaRecord =
        manual(id, actor, SagaEvents.CANCEL_REQUESTED, runFor) { r, now ->
            machine.cancel(r, reason, now).let { Change(it.saga, it.detail) }
        }

    /** What an action makes of the saga, and what to log about it. */
    private class Change(val saga: SagaPhase, val detail: String? = null)

    /**
     * One action: [change] decides on the locked saga, in a transaction that saves it and logs [event]; then the saga
     * is run on for up to [runFor].
     */
    private suspend fun manual(
        id: Uuid,
        actor: String,
        event: String,
        runFor: Duration,
        change: (SagaPhase, Instant) -> Change,
    ): SagaRecord {
        require(!runFor.isNegative()) { "runFor must not be negative" }
        val trace = traceId()
        val committed = store.tx {
            val r = store.lock(id) ?: throw NoSuchElementException("Saga $id not found")
            val now = store.now()
            val (updated, detail) = change(r, now).let { it.saga to it.detail }
            store.save(updated, now)
            // r: logged on the step the action was taken on
            store.log(r.id, r.stepIndex, null, event, detail, now, trace, actor)
            updated
        }
        val now = store.now()
        if (!runFor.isPositive() || !machine.canMoveOn(committed, now)) return committed.record()
        // The action is committed: a failure from here on is the saga's, not the action's.
        return try {
            runOn(id, now + runFor)
        } catch (e: Throwable) {
            if (e is CancellationException || e.isFatal()) throw e
            postpone(id, e, trace)
            try {
                store.tx { repository.find(id) } ?: committed.record()
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
    private fun signalByHand(r: SagaPhase.Running, step: AnySignalStep, output: String?, now: Instant): Change {
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
        val value = given?.second
            ?: serializer.decodeSaved(checkNotNull(kept), checkNotNull(step.outputType)) { "signal '${step.signal}' of saga ${r.id}" }
        val detail = listOfNotNull("signal by hand", describe(step, value)?.let { "ref=$it" }).joinToString(" ")
        return Change(machine.signalByHand(r, now), detail)
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
            if (step.name in stored) {
                return stored[step.name] to stored[step.name]?.let { serializer.decodeSaved(it, type) { "the output of step '${step.name}'" } }
            }
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
}
