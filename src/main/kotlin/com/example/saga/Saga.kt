package com.example.saga

import kotlin.uuid.Uuid

/*
 * Public API for defining sagas.
 *
 * A saga is an ordered list of steps. Each step calls an external system. The engine records
 * where every saga stands and decides what to do next from the outcome of each step. It knows
 * nothing about the business meaning of the steps.
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
sealed interface CheckResult {
    /** The effect was found. [ref] is the external reference to keep. */
    data class Done(val ref: String?) : CheckResult
    /** The effect was not found: the engine executes the step again. */
    data object NotDone : CheckResult
    /** Cannot decide (no lookup available, several candidates…): a human must step in. */
    data object CannotTell : CheckResult
}

/** What a step sees: the saga's business key and the references returned by earlier steps. */
class SagaContext(
    val sagaId: Uuid,
    /** Business key given at creation, e.g. the id of the item being set up. */
    val key: String,
    private val results: Map<String, String?>,
) {
    /** Reference returned by the step named [stepName], or null if it returned none or has not run. */
    fun result(stepName: String): String? = results[stepName]
}

interface SagaStep {
    /** Unique within its saga. Stored in the database: do not rename a step that has running sagas. */
    val name: String

    /**
     * Performs the call. Returns an external reference to keep (or null).
     * Throw [StepRejected] or [StepNotSent] when you know nothing was applied.
     */
    suspend fun execute(ctx: SagaContext): String?

    /**
     * Undoes the step. MUST be safe to run more than once (e.g. "move back to stock if still
     * there", "delete if present"): on any error the engine simply runs it again.
     * Default: nothing to undo.
     */
    suspend fun compensate(ctx: SagaContext) {}

    /**
     * Called after an unknown outcome, once the settle delay has passed: was the step applied?
     * Default: cannot tell, the saga waits for a human.
     */
    suspend fun check(ctx: SagaContext): CheckResult = CheckResult.CannotTell
}

class SagaDefinition(
    /** Saga type, stored in the database (e.g. "ITEM_SETUP"). */
    val type: String,
    val steps: List<SagaStep>,
    /** Runs INSIDE the transaction that completes the saga. Must not call external systems. */
    val onCompleted: (SagaContext) -> Unit = {},
    /** Runs INSIDE the transaction that ends the saga after compensation. Must not call external systems. */
    val onFailed: (SagaContext) -> Unit = {},
) {
    init {
        require(type.isNotBlank()) { "Saga type must not be blank" }
        require(steps.isNotEmpty()) { "Saga $type has no steps" }
        require(steps.map { it.name }.toSet().size == steps.size) { "Duplicate step names in saga $type" }
    }
}
