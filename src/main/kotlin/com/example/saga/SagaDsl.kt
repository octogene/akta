package com.example.saga

import kotlin.reflect.KType
import kotlin.reflect.typeOf

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

/** Builds a saga whose input is an [I]: given to `create`, read by every step as `input`. */
inline fun <reified I> saga(type: String, noinline block: SagaBuilder<I>.() -> Unit): SagaDefinition<I> =
    SagaBuilder<I>(type, typeOf<I>()).apply(block).build()

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

    /** Declares the next step, whose call returns an [O]. Steps run in declaration order. */
    inline fun <reified O> step(name: String, noinline block: StepBuilder<I, O>.() -> Unit): StepRef<O> =
        addStep(name, typeOf<O>(), block)

    @PublishedApi
    internal fun <O> addStep(name: String, outputType: KType?, block: StepBuilder<I, O>.() -> Unit): StepRef<O> {
        steps += StepBuilder<I, O>(name, outputType).apply(block).build()
        return StepRef(name)
    }

    /** Runs INSIDE the transaction that completes the saga. No external calls. */
    fun onCompleted(block: SagaContext<I>.() -> Unit) {
        onCompleted = block
    }

    /** Runs INSIDE the transaction that ends the saga after compensation. No external calls. */
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
    private var compensateBlock: suspend SagaContext<I>.(own: O?) -> Unit = {}
    private var checkBlock: suspend SagaContext<I>.() -> CheckResult<O> = { CheckResult.CannotTell }
    private var describeBlock: ((O) -> String?)? = null

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

    /** After an unknown outcome: was the step applied? [CheckResult.Done] carries the output found. */
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
        return object : SagaStep<I, O> {
            override val name = stepName
            override val outputType = type
            override suspend fun execute(ctx: SagaContext<I>): O = ctx.executeBlock()
            override suspend fun compensate(ctx: SagaContext<I>, own: O?) = ctx.compensateBlock(own)
            override suspend fun check(ctx: SagaContext<I>): CheckResult<O> = ctx.checkBlock()
            override fun describe(output: O): String? = describeBlock(output)
            override fun toString() = "SagaStep($stepName)"
        }
    }
}
