package com.example.saga

import kotlin.reflect.KType
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
}

/**
 * One step of a saga with input [I], whose call returns an [O]. The DSL (`step<O>(…) { … }`) builds
 * these; a hand-written step implements the interface and declares [outputType] as `typeOf<O>()`.
 */
interface SagaStep<I, O> {
    /** Unique within its saga. Stored in the database: do not rename a step that has running sagas. */
    val name: String

    /** The type of the output, for the engine's serializer. Null when the step has no output (O is Unit). */
    val outputType: KType?

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
     * Default: cannot tell, the saga waits for a human.
     */
    suspend fun check(ctx: SagaContext<I>): CheckResult<O> = CheckResult.CannotTell

    /**
     * What `saga_log` records about an output: an identifier, never personal data. Default: nothing.
     * The value itself is never logged.
     */
    fun describe(output: O): String? = null

    /** A handle to read this step's output from the steps after it. */
    val ref: StepRef<O> get() = StepRef(name)
}

class SagaDefinition<I>(
    /** Saga type, stored in the database (e.g. "ITEM_SETUP"). */
    val type: String,
    /** The type of the input, for the engine's serializer. Null when the saga has no input (I is Unit). */
    val inputType: KType?,
    val steps: List<SagaStep<I, *>>,
    /** Runs INSIDE the transaction that completes the saga. Must not call external systems. */
    val onCompleted: SagaContext<I>.() -> Unit = {},
    /** Runs INSIDE the transaction that ends the saga after compensation. Must not call external systems. */
    val onFailed: SagaContext<I>.() -> Unit = {},
) {
    init {
        require(type.isNotBlank()) { "Saga type must not be blank" }
        require(steps.isNotEmpty()) { "Saga $type has no steps" }
        val duplicates = steps.groupBy { it.name }.filterValues { it.size > 1 }.keys
        require(duplicates.isEmpty()) { "Duplicate step names in saga $type: $duplicates" }
    }
}
