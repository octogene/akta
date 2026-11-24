package dev.octogene.akta

import kotlin.reflect.KType
import kotlin.reflect.typeOf
import kotlin.time.Duration

/*
 * Kotlin DSL on top of SagaDefinition / SagaStep. It builds exactly the same objects:
 * the engine does not know whether a saga was declared with the DSL or by hand.
 *
 *   @Serializable data class BookingRequest(val customerId: String, val start: LocalDate, val end: LocalDate)
 *   @Serializable data class Hold(val id: String, val warehouse: String)
 *
 *   val booking = saga<BookingRequest>("BOOKING") {
 *       val hold = step<Hold>("hold-item") {
 *           execute { stockClient.hold(key, until = input.end) }            // returns a Hold
 *           compensate { own -> own?.let { stockClient.release(it.id) } }   // its own output
 *           checkByLookup { stockClient.findHolds(key, input.start) }       // a List<Hold>
 *           logAs { it.id }                                                  // what saga_log shows
 *       }
 *       step<String>("booking") {
 *           execute { bookingClient.book(input.customerId, requireResult(hold).warehouse) }
 *       }
 *       onCompleted { bookings.confirm(key, input.start) }
 *   }
 *
 * Without a type argument there is no data: `saga("X") { … }` has no input, `step("x") { … }` no output.
 */

@DslMarker
annotation class SagaDslMarker

/** Builds a saga without input. */
@JvmName("sagaWithoutInput")      // same JVM signature as the typed saga<I> below, once erased
fun saga(type: String, block: SagaBuilder<Unit>.() -> Unit): SagaDefinition<Unit> =
    SagaBuilder<Unit>(type, inputType = null).apply(block).build()

/**
 * Builds a saga whose input is an [I]: given to `create`, read by every step as `input`.
 * `saga<Unit>` is the same as `saga` without a type argument: no input is stored.
 */
inline fun <reified I> saga(type: String, noinline block: SagaBuilder<I>.() -> Unit): SagaDefinition<I> =
    SagaBuilder<I>(type, typeOf<I>().unlessUnit()).apply(block).build()

@SagaDslMarker
class SagaBuilder<I> @PublishedApi internal constructor(
    private val type: String,
    private val inputType: KType?,
) {
    private val steps = mutableListOf<SagaStep<I, *>>()
    private var onCompleted: SagaContext<I>.() -> Unit = {}
    private var onFailed: SagaContext<I>.() -> Unit = {}

    /** Declares the next step, without output. Steps run in declaration order. */
    @JvmName("stepWithoutOutput")  // same JVM signature as the typed step<O> below, once erased
    fun step(name: String, block: StepBuilder<I, Unit>.() -> Unit): StepRef<Unit> = addStep(name, null, block)

    /**
     * Declares the next step, whose call returns an [O]. Steps run in declaration order.
     * `step<Unit>` is the same as `step` without a type argument: no output is stored.
     */
    inline fun <reified O> step(name: String, noinline block: StepBuilder<I, O>.() -> Unit): StepRef<O> =
        addStep(name, typeOf<O>().unlessUnit(), block)

    @PublishedApi
    internal fun <O> addStep(name: String, outputType: KType?, block: StepBuilder<I, O>.() -> Unit): StepRef<O> {
        steps += StepBuilder<I, O>(name, outputType).apply(block).build()
        return StepRef(name)
    }

    /**
     * Declares the next step as a wait for the signal named [signal], whose value is an [S]: it makes no
     * call, and the value becomes its output. After [timeout], its check decides (`check` or
     * `checkByLookup` in [block]); without one, the saga becomes stuck. Returns a `StepRef<S>`.
     */
    inline fun <reified S> awaitSignal(
        name: String,
        signal: String,
        timeout: Duration,
        noinline block: SignalStepBuilder<I, S>.() -> Unit = {},
    ): StepRef<S> = addSignalStep(name, signal, timeout, typeOf<S>(), block)

    @PublishedApi
    internal fun <S> addSignalStep(
        name: String,
        signal: String,
        timeout: Duration,
        valueType: KType,
        block: SignalStepBuilder<I, S>.() -> Unit,
    ): StepRef<S> {
        steps += SignalStepBuilder<I, S>(name, signal, timeout, valueType).apply(block).build()
        return StepRef(name)
    }

    /** Runs INSIDE the transaction that completes the saga. No external calls; the saga's row is written after it. */
    fun onCompleted(block: SagaContext<I>.() -> Unit) {
        onCompleted = block
    }

    /** Runs INSIDE the transaction that ends the saga after compensation. No external calls; the saga's row is written after it. */
    fun onFailed(block: SagaContext<I>.() -> Unit) {
        onFailed = block
    }

    @PublishedApi
    internal fun build() = SagaDefinition(type, inputType, steps.toList(), onCompleted, onFailed)
}

@SagaDslMarker
class StepBuilder<I, O> @PublishedApi internal constructor(
    private val name: String,
    private val outputType: KType?,
) {
    private var executeBlock: (suspend SagaContext<I>.() -> O)? = null
    private var compensateBlock: (suspend SagaContext<I>.(own: O?) -> Unit)? = null
    private var checkBlock: suspend SagaContext<I>.() -> CheckResult<O> = { CheckResult.CannotTell }
    private var describeBlock: ((O) -> String?)? = null
    private var idempotency: Idempotency? = null

    /**
     * The step's service accepts an idempotency key, sent from `idempotencyKey`, and remembers it for
     * [keyRetention] (what the service documents; take its minimum). After an unknown outcome, the call is
     * replayed with the same key instead of checked. Confirm the service's guarantees first: see
     * "Before marking a step idempotent" in https://octogene.github.io/akta/use-cases/idempotent-services/
     */
    fun idempotent(keyRetention: Duration) {
        idempotency = Idempotency(keyRetention)
    }

    /**
     * The call. Returns the step's output.
     * Throw [StepRejected] or [StepNotSent] when you know nothing was applied.
     */
    fun execute(block: suspend SagaContext<I>.() -> O) {
        check(executeBlock == null) { "execute already declared for step '$name'" }
        executeBlock = block
    }

    /** Undo. Receives this step's own output. Must be safe to run more than once. */
    fun compensate(block: suspend SagaContext<I>.(own: O?) -> Unit) {
        compensateBlock = block
    }

    /**
     * After an unknown outcome: was the step applied? [CheckResult.Done] carries the output found. Throw
     * [StepRejected] when the external system shows the call was refused: the saga compensates.
     */
    fun check(block: suspend SagaContext<I>.() -> CheckResult<O>) {
        checkBlock = block
    }

    /**
     * Shortcut for the usual check: look the effect up in the external system.
     * No match → not done; one match → done (it becomes the step's output); several → cannot tell.
     */
    fun checkByLookup(lookup: suspend SagaContext<I>.() -> List<O>) {
        checkBlock = {
            val found = lookup()
            when (found.size) {
                0 -> CheckResult.NotDone
                1 -> CheckResult.Done(found.single())
                else -> CheckResult.CannotTell
            }
        }
    }

    /**
     * What `saga_log` records about the output: an identifier, never personal data.
     * Default: the output itself for a `String` output, nothing for any other type.
     */
    fun logAs(block: (O) -> String?) {
        describeBlock = block
    }

    @PublishedApi
    internal fun build(): SagaStep<I, O> {
        val executeBlock = requireNotNull(executeBlock) { "Step '$name' has no execute block" }
        val compensateBlock = compensateBlock
        val checkBlock = checkBlock
        val describeBlock = describeBlock ?: if (outputType?.classifier == String::class) {
            { output: O -> output as String? }
        } else {
            { _: O -> null }
        }
        val stepName = name
        val type = outputType
        val stepIdempotency = idempotency
        return object : SagaStep<I, O> {
            override val name = stepName
            override val outputType = type
            override val idempotency = stepIdempotency
            override suspend fun execute(ctx: SagaContext<I>): O = ctx.executeBlock()
            override val compensates = compensateBlock != null
            override suspend fun compensate(ctx: SagaContext<I>, own: O?) { compensateBlock?.invoke(ctx, own) }
            override suspend fun check(ctx: SagaContext<I>): CheckResult<O> = ctx.checkBlock()
            override fun describe(output: O): String? = describeBlock(output)
            override fun toString() = "SagaStep($stepName)"
        }
    }
}

@SagaDslMarker
class SignalStepBuilder<I, S> @PublishedApi internal constructor(
    private val name: String,
    private val signal: String,
    private val timeout: Duration,
    private val valueType: KType,
) {
    private var acceptBlock: SagaContext<I>.(S) -> Unit = {}
    private var checkBlock: (suspend SagaContext<I>.() -> CheckResult<S>)? = null
    private var describeBlock: ((S) -> String?)? = null

    /**
     * Decides on a received value: return to accept it, throw [StepRejected] to fail the saga and
     * compensate. Runs inside the engine's transaction, for a signal delivered or stored and for a value the
     * timeout check found: no external calls. Anything else it throws is a bug: the transaction rolls back
     * whole, and the saga stops (RECORD_FAILED) with the signal kept, until the bug is fixed and it is retried.
     */
    fun onSignal(block: SagaContext<I>.(signal: S) -> Unit) {
        acceptBlock = block
    }

    /**
     * At the timeout, asks the external system: [CheckResult.Done] accepts its value, NotDone rejects. A signal
     * received while it runs is used instead of NotDone or CannotTell.
     */
    fun check(block: suspend SagaContext<I>.() -> CheckResult<S>) {
        checkBlock = block
    }

    /** At the timeout, looks the value up: none means rejected, one means accepted, several means cannot tell. */
    fun checkByLookup(lookup: suspend SagaContext<I>.() -> List<S>) {
        checkBlock = {
            val found = lookup()
            when (found.size) {
                0 -> CheckResult.NotDone
                1 -> CheckResult.Done(found.single())
                else -> CheckResult.CannotTell
            }
        }
    }

    /** What `saga_log` records about the value: an identifier, never personal data. Default: nothing. */
    fun logAs(block: (S) -> String?) {
        describeBlock = block
    }

    @PublishedApi
    internal fun build(): SignalStep<I, S> {
        require(timeout.isPositive()) { "Signal step '$name' needs a positive timeout" }
        val acceptBlock = acceptBlock
        val checkBlock = checkBlock
        val describeBlock = describeBlock ?: { _: S -> null }
        val stepName = name
        val signalName = signal
        val stepTimeout = timeout
        val type = valueType
        return object : SignalStep<I, S> {
            override val name = stepName
            override val signal = signalName
            override val timeout = stepTimeout
            override val outputType = type
            override val checksOnTimeout = checkBlock != null
            override fun accept(ctx: SagaContext<I>, value: S) = ctx.acceptBlock(value)
            override suspend fun check(ctx: SagaContext<I>): CheckResult<S> =
                checkBlock?.let { ctx.it() } ?: CheckResult.CannotTell
            override fun describe(output: S): String? = describeBlock(output)
            override fun toString() = "SignalStep($stepName, signal=$signalName)"
        }
    }
}
