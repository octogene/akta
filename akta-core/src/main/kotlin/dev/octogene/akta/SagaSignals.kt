package dev.octogene.akta

import kotlin.reflect.KType
import kotlin.time.Instant
import kotlin.uuid.Uuid

/**
 * A saga's signals: delivered by [SagaEngine.signal] (stored, and the step woken if it waits), then consumed when the
 * saga runs, through the step's onSignal. Each function runs inside one of the engine's transactions.
 */
internal class SagaSignals(
    private val store: SagaStore,
    private val machine: SagaMachine,
    private val serializer: SagaSerializer,
    private val config: SagaConfig,
    private val definition: (type: String) -> AnyDefinition,
) {
    private val repository: SagaRepository get() = store.repository

    /** [SagaEngine.signal], in its transaction: stored, and its step woken if it waits. Runs no user code. */
    fun deliver(
        sagaId: Uuid,
        name: String,
        value: Any?,
        type: KType,
        trace: String?,
    ): SignalResult {
        val r = store.lock(sagaId) ?: return SignalResult.NoSaga
        val now = store.now()
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
            store.log(r.id, index, step.name, SagaEvents.SIGNAL_RECEIVED, detail, now, trace)
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
                store.save(machine.woken(r, now), now)
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
    fun startWaiting(r: SagaPhase.Unfinished, step: AnySignalStep, now: Instant, trace: String?): SagaPhase {
        val waiting = machine.waiting(r, step, now)
        store.save(waiting, now)
        store.log(r.id, r.stepIndex, step.name, SagaEvents.WAIT_STARTED, "signal '${step.signal}' until ${waiting.due}", now, trace)
        return waiting
    }

    /**
     * The signal of [step] received earlier, [stored], applied now: the saga moves on (or compensates). Saved and
     * logged; the next step is for the caller to claim.
     */
    fun consumeStored(
        r: SagaPhase.Unfinished,
        def: AnyDefinition,
        step: AnySignalStep,
        ctx: SagaContext<Any?>,
        stored: String,
        now: Instant,
        trace: String?,
    ): SagaPhase {
        val value = serializer.decodeSaved(stored, checkNotNull(step.outputType)) { "signal '${step.signal}' of saga ${r.id}" }
        val updated = signalDecision(r, def, step, ctx, value, stored, now)
        store.save(updated, now)
        // Logged after the change, as in deliver(): the event shows the saga as the signal left it.
        val detail = listOfNotNull("consumed (stored earlier)", describe(step, value)?.let { "ref=$it" }).joinToString(" ")
        store.log(r.id, r.stepIndex, step.name, SagaEvents.SIGNAL_RECEIVED, detail, now, trace)
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
     * The claim threw [failure]: onSignal failed on a signal stored
     * earlier, as the step started waiting or before a timeout check. The claim rolled back whole: the saga
     * stops, the signal kept.
     */
    fun blockOnStoredSignal(id: Uuid, failure: SignalFailed, trace: String?): SagaPhase? {
        val r = store.lock(id) ?: return null
        val step = definition(r.type).steps[r.stepIndex] as? SignalStep<*, *> ?: return r
        val now = store.now()
        val blocked = machine.blockedByStoredSignal(r, failure.message, now) ?: return r
        store.save(blocked, now)
        store.log(r.id, r.stepIndex, step.name, SagaEvents.RECORD_FAILED, blocked.lastError, now, trace)
        return blocked
    }
}

/**
 * A signal could not be applied: onSignal threw something other than StepRejected (see [SagaSignals.onSignal]).
 */
internal class SignalFailed(val signal: String, cause: Throwable) :
    RuntimeException("onSignal threw on signal '$signal': $cause", cause)
