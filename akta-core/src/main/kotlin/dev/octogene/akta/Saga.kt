package dev.octogene.akta

import java.security.MessageDigest
import kotlin.reflect.KType
import kotlin.time.Duration
import kotlin.uuid.Uuid

/*
 * Public API for defining sagas.
 *
 * A saga is an ordered list of steps. Each step calls an external system. The engine records
 * where every saga stands and decides what to do next from the outcome of each step. It knows
 * nothing about the business meaning of the steps.
 *
 * A saga may carry an input of type I, given once to create and read by every step. Each step may
 * return an output of its own type O, stored with its result and read by the steps after it, by its
 * own compensation and by the hooks. Both are stored as text by the engine's SagaSerializer.
 */

/**
 * Thrown by a step to say "the external system refused, nothing was applied".
 * The engine undoes the previous steps (compensation).
 */
class StepRejected(message: String, cause: Throwable? = null) : Exception(message, cause)

/**
 * Thrown by a step to say "the request never left" (connection refused, DNS failure,
 * connect timeout, rate limited…). The engine retries later, with no risk of a duplicate.
 */
class StepNotSent(message: String, cause: Throwable? = null) : Exception(message, cause)

// Any other exception means the outcome is UNKNOWN: the call may have been applied.

/** A step's answer to "were you already done?", asked after an unknown outcome. */
sealed interface CheckResult<out O> {
    /** The effect was found. [output] is what the call would have returned: it is stored as the step's output. */
    data class Done<out O>(val output: O) : CheckResult<O>
    /** The effect was not found: the engine executes the step again. */
    data object NotDone : CheckResult<Nothing>
    /** Cannot decide (no lookup available, several candidates…): a human must step in. */
    data object CannotTell : CheckResult<Nothing>
}

/** Typed handle on a step, to read its output: `requireResult(hold)`. */
class StepRef<out O>(val name: String) {
    override fun toString() = "StepRef($name)"
}

/** What a step sees: the saga's business key, its input, and the outputs of the steps done so far. */
class SagaContext<out I>(
    val sagaId: Uuid,
    /** Business key given at creation, e.g. the id of the item being set up. */
    val key: String,
    /** The input given at creation. Fixed for the whole life of the saga. */
    val input: I,
    private val outputs: Map<String, Any?>,
) {
    /** Output of [step], or null if it has not completed (or was compensated). */
    @Suppress("UNCHECKED_CAST")
    fun <O> result(step: StepRef<O>): O? = outputs[step.name] as O?

    /** Output of [step], which must have completed. */
    @Suppress("UNCHECKED_CAST")
    fun <O> requireResult(step: StepRef<O>): O {
        check(step.name in outputs) { "Step '${step.name}' has no result in saga $sagaId" }
        return outputs[step.name] as O
    }

    internal fun output(stepName: String): Any? = outputs[stepName]

    private var callKey: String? = null

    /**
     * The idempotency key of this call, for a service that accepts one: the same for every attempt of
     * the call, on every pod, for the life of the saga (`<saga id>/<step name>/execute`, or `/compensate`
     * for an undo; a check gets its step's execute key). Only inside execute, check and compensate.
     */
    val idempotencyKey: String
        get() = callKey ?: throw IllegalStateException("No idempotency key outside a step's execute, check or compensate")

    /** [idempotencyKey] as a name-based UUID (version 5), for a service that wants a UUID. Stable as the key. */
    val idempotencyUuid: Uuid
        get() = nameUuid(sagaId, idempotencyKey)

    /** This context, for one call with [key]. */
    internal fun forCall(key: String): SagaContext<I> =
        SagaContext(sagaId, this.key, input, outputs).also { it.callKey = key }
}

/** RFC 9562 version 5: SHA-1 of the namespace's bytes and the name, with the version and variant bits set. */
private fun nameUuid(namespace: Uuid, name: String): Uuid {
    val digest = MessageDigest.getInstance("SHA-1").run {
        update(namespace.toByteArray())
        digest(name.encodeToByteArray())
    }
    val bytes = digest.copyOf(16)
    bytes[6] = ((bytes[6].toInt() and 0x0f) or 0x50).toByte()
    bytes[8] = ((bytes[8].toInt() and 0x3f) or 0x80).toByte()
    return Uuid.fromByteArray(bytes)
}

/**
 * A step whose service accepts an idempotency key: replaying a call with the same key returns the first
 * result instead of applying it twice. The engine then replays it after an unknown outcome, instead of
 * waiting and checking. [keyRetention] is how long the service remembers a key: the engine never sends a
 * key it may have outlived (the step becomes stuck with [StuckReason.KEY_EXPIRED]).
 */
data class Idempotency(val keyRetention: Duration) {
    init {
        require(keyRetention.isPositive()) { "keyRetention must be positive" }
    }
}

/**
 * One step of a saga with input [I], whose call returns an [O]. The DSL (`step<O>(…) { … }`) builds
 * these; a hand-written step implements the interface and declares [outputType] as `typeOf<O>()`.
 */
interface SagaStep<I, O> {
    /** Unique within its saga. Stored in the database: do not rename a step that has running sagas. */
    val name: String

    /**
     * The type of the output, for the engine's serializer. Null when the step has no output (O is Unit):
     * `typeOf<Unit>()` is refused, since it would store `{}` for a step that returns nothing.
     */
    val outputType: KType?

    /**
     * Set when the step's service accepts an idempotency key, which the step sends from
     * [SagaContext.idempotencyKey]. Default: none, an unknown outcome is checked.
     */
    val idempotency: Idempotency? get() = null

    /**
     * Performs the call and returns its output.
     * Throw [StepRejected] or [StepNotSent] when you know nothing was applied.
     */
    suspend fun execute(ctx: SagaContext<I>): O

    /**
     * Undoes the step, given its own output ([own], null when it stored none). MUST be safe to run
     * more than once (e.g. "move back to stock if still there", "delete if present"): on any error
     * the engine simply runs it again. Default: nothing to undo.
     */
    suspend fun compensate(ctx: SagaContext<I>, own: O?) {}

    /**
     * Called after an unknown outcome, once the settle delay has passed: was the step applied?
     * Throw [StepRejected] when the external system shows the call was refused (a record of the attempt,
     * rejected): the saga compensates, as after a refusal from [execute]. Any other exception means the
     * lookup failed: it is tried again, then the saga stops (CHECK_FAILING). Default: cannot tell, the saga
     * waits for a human.
     */
    suspend fun check(ctx: SagaContext<I>): CheckResult<O> = CheckResult.CannotTell

    /**
     * What `saga_log` records about an output: an identifier, never personal data. Default: nothing.
     * The value itself is never logged.
     */
    fun describe(output: O): String? = null

    /**
     * Whether the step has anything to undo. False: compensation passes over it without calling
     * [compensate], in the same transaction (logged as NOTHING_TO_UNDO). Default: true. The DSL sets it
     * from whether `compensate { … }` was declared.
     */
    val compensates: Boolean get() = true

    /** A handle to read this step's output from the steps after it. */
    val ref: StepRef<O> get() = StepRef(name)
}

/**
 * A step that makes no call: it waits for a signal named [signal], delivered from outside through
 * [SagaEngine.signal] (a Kafka listener, an HTTP call). The signal's value, of type [S], becomes the
 * step's output. While it waits, the saga is only a row that is not due: no thread, connection or lock.
 *
 * At [timeout], the step's [check] decides, if [checksOnTimeout]: the value it finds means accepted,
 * NotDone means rejected (the saga compensates), CannotTell means stuck. A signal received while the
 * check runs, or after it failed, is used instead of NotDone or CannotTell: the timeout is a fallback
 * for a lost event, not a deadline (a business deadline belongs in [accept]). Without a check, a timeout
 * makes the saga stuck. The DSL (`awaitSignal<S>(…) { … }`) builds these.
 */
interface SignalStep<I, S> : SagaStep<I, S> {
    /** The name a listener signals: it describes the event (`payment`), not the step. Unique in a saga. */
    val signal: String

    /** How long to wait, from the moment the step starts waiting. */
    val timeout: Duration

    /** Whether [check] asks the external system at the timeout. False: a timeout makes the saga stuck. */
    val checksOnTimeout: Boolean

    /**
     * Decides on a received value: return to accept it, throw [StepRejected] to fail the saga and
     * compensate. Runs inside the engine's transaction, wherever the value comes from (a signal delivered or
     * stored, a value [check] found): no external calls. Anything else it throws is a bug: the saga stops
     * (RECORD_FAILED) with the signal kept, until fixed and retried. Default: accept.
     */
    fun accept(ctx: SagaContext<I>, value: S) {}

    /** A signal step makes no call, so it has nothing to undo. Override with [compensate] if yours has. */
    override val compensates: Boolean get() = false

    /** A signal step makes no call. */
    override suspend fun execute(ctx: SagaContext<I>): S =
        throw UnsupportedOperationException("Signal step '$name' makes no call: it waits for signal '$signal'")
}

/** What [SagaEngine.signal] did with a signal. */
sealed interface SignalResult {
    /** The step was waiting for it: accepted (the saga moves on) or rejected (it compensates). */
    data class Delivered(val sagaId: Uuid) : SignalResult
    /** The step does not wait yet (or its timeout check is in flight): kept, used when it waits. */
    data class Stored(val sagaId: Uuid) : SignalResult
    /** A signal of that name was already received, or the step is done, or the saga finished: nothing changes. */
    data class Ignored(val sagaId: Uuid) : SignalResult
    /** No saga with that id. */
    data object NoSaga : SignalResult
}

class SagaDefinition<I>(
    /** Saga type, stored in the database (e.g. "ITEM_SETUP"). */
    val type: String,
    inputType: KType?,
    val steps: List<SagaStep<I, *>>,
    /** Runs INSIDE the transaction that completes the saga. Must not call external systems. */
    val onCompleted: SagaContext<I>.() -> Unit = {},
    /** Runs INSIDE the transaction that ends the saga after compensation. Must not call external systems. */
    val onFailed: SagaContext<I>.() -> Unit = {},
) {
    /**
     * The type of the input, for the engine's serializer. Null when the saga has no input (I is Unit):
     * `typeOf<Unit>()` becomes null, so a saga without input never stores `{}`.
     */
    val inputType: KType? = inputType?.unlessUnit()

    init {
        require(type.isNotBlank()) { "Saga type must not be blank" }
        require(steps.isNotEmpty()) { "Saga $type has no steps" }
        val duplicates = steps.groupBy { it.name }.filterValues { it.size > 1 }.keys
        require(duplicates.isEmpty()) { "Duplicate step names in saga $type: $duplicates" }
        val signals = steps.filterIsInstance<SignalStep<*, *>>()
        val duplicateSignals = signals.groupBy { it.signal }.filterValues { it.size > 1 }.keys
        require(duplicateSignals.isEmpty()) { "Signal names used by several steps in saga $type: $duplicateSignals" }
        signals.forEach { require(it.outputType != null) { "Signal step '${it.name}' of saga $type needs a value type" } }
        signals.forEach { require(it.idempotency == null) { "Signal step '${it.name}' of saga $type makes no call: it cannot be idempotent" } }
        steps.filter { it !is SignalStep<*, *> }.forEach {
            require(it.outputType?.unlessUnit() == it.outputType) {
                "Step '${it.name}' of saga $type declares Unit as its output type: declare null, a step without output stores nothing"
            }
        }
        // The name is part of the idempotency key, which services limit in length.
        steps.forEach { require(it.name.length <= MAX_STEP_NAME) { "Step name '${it.name}' of saga $type is over $MAX_STEP_NAME characters" } }
    }
}

private const val MAX_STEP_NAME = 100

/** Null for `Unit`, which carries no data: nothing to store, rather than `{}`. `Unit?` is kept. */
@PublishedApi
internal fun KType.unlessUnit(): KType? = takeUnless { it.classifier == Unit::class && !it.isMarkedNullable }
