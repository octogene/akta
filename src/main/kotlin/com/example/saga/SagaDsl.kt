package com.example.saga

/*
 * Kotlin DSL on top of SagaDefinition / SagaStep. It builds exactly the same objects:
 * the engine does not know whether a saga was declared with the DSL or by hand.
 *
 *   val itemSetup = saga("ITEM_SETUP") {
 *       val stock = step("stock") {
 *           execute { stockClient.moveToTestStock(key) }            // returns the stock id
 *           compensate { stockId -> stockClient.revert(stockId!!) }  // receives this step's own result
 *           checkByLookup { stockClient.findByEpc(key) }
 *       }
 *       step("booking") {
 *           execute { bookingClient.createResource(key, stockId = requireResult(stock)) }
 *           checkByLookup { bookingClient.findByEpc(key) }
 *       }
 *       onCompleted { items.markAvailable(key) }
 *       onFailed { items.markSetupFailed(key) }
 *   }
 */

@DslMarker
annotation class SagaDslMarker

/** Builds a saga definition. */
fun saga(type: String, block: SagaBuilder.() -> Unit): SagaDefinition = SagaBuilder(type).apply(block).build()

/** Handle on a declared step, to read its result in a type-safe way: `result(stock)`. */
class StepRef internal constructor(val name: String)

/** Reference returned by [step], or null if it returned none or has not run. */
fun SagaContext.result(step: StepRef): String? = result(step.name)

/** Like [result], for a step that always returns a reference. */
fun SagaContext.requireResult(step: StepRef): String =
    result(step.name) ?: error("Step '${step.name}' has no result in saga $sagaId")

@SagaDslMarker
class SagaBuilder internal constructor(private val type: String) {
    private val steps = mutableListOf<SagaStep>()
    private var onCompleted: (SagaContext) -> Unit = {}
    private var onFailed: (SagaContext) -> Unit = {}

    /** Declares the next step. Steps run in declaration order. */
    fun step(name: String, block: StepBuilder.() -> Unit): StepRef {
        steps += StepBuilder(name).apply(block).build()
        return StepRef(name)
    }

    /** Runs INSIDE the transaction that completes the saga. No external calls. */
    fun onCompleted(block: SagaContext.() -> Unit) {
        onCompleted = block
    }

    /** Runs INSIDE the transaction that ends the saga after compensation. No external calls. */
    fun onFailed(block: SagaContext.() -> Unit) {
        onFailed = block
    }

    internal fun build() = SagaDefinition(type, steps.toList(), onCompleted, onFailed)
}

@SagaDslMarker
class StepBuilder internal constructor(private val name: String) {
    private var executeBlock: (suspend SagaContext.() -> String?)? = null
    private var compensateBlock: suspend SagaContext.(ownResult: String?) -> Unit = {}
    private var checkBlock: suspend SagaContext.() -> CheckResult = { CheckResult.CannotTell }

    /**
     * The call. Return the external reference to keep, or null.
     * Throw [StepRejected] or [StepNotSent] when you know nothing was applied.
     */
    fun execute(block: suspend SagaContext.() -> String?) {
        check(executeBlock == null) { "execute already declared for step '$name'" }
        executeBlock = block
    }

    /** Undo. Receives the reference this step returned. Must be safe to run more than once. */
    fun compensate(block: suspend SagaContext.(ownResult: String?) -> Unit) {
        compensateBlock = block
    }

    /** After an unknown outcome: was the step applied? */
    fun check(block: suspend SagaContext.() -> CheckResult) {
        checkBlock = block
    }

    /**
     * Shortcut for the usual check: look the effect up in the external system.
     * No match → not done; one match → done (its reference is kept); several → cannot tell.
     */
    fun checkByLookup(lookup: suspend SagaContext.() -> List<String>) {
        checkBlock = {
            val found = lookup()
            when (found.size) {
                0 -> CheckResult.NotDone
                1 -> CheckResult.Done(found.single())
                else -> CheckResult.CannotTell
            }
        }
    }

    internal fun build(): SagaStep {
        val executeBlock = requireNotNull(executeBlock) { "Step '$name' has no execute block" }
        val compensateBlock = compensateBlock
        val checkBlock = checkBlock
        val stepName = name
        return object : SagaStep {
            override val name = stepName
            override suspend fun execute(ctx: SagaContext): String? = ctx.executeBlock()
            override suspend fun compensate(ctx: SagaContext) = ctx.compensateBlock(ctx.result(stepName))
            override suspend fun check(ctx: SagaContext): CheckResult = ctx.checkBlock()
            override fun toString() = "SagaStep($stepName)"
        }
    }
}
