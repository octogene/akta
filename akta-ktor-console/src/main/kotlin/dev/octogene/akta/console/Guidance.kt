package dev.octogene.akta.console

import dev.octogene.akta.ManualAction
import dev.octogene.akta.SagaRecord
import dev.octogene.akta.SagaState
import dev.octogene.akta.SagaStepProgress
import dev.octogene.akta.StepProgress
import dev.octogene.akta.StuckReason

/*
 * What the console says to an operator: what happened to a stuck saga, what to check, and what each action does.
 * The words of docs/operations/stuck-sagas.md, shortened for a screen.
 */

/** What happened, and what to check before acting, for a saga stuck for [reason] on the step [step]. */
internal fun guidance(reason: StuckReason, step: String): Pair<String, String> = when (reason) {
    StuckReason.CANNOT_TELL ->
        "The check could not tell whether '$step' was applied." to
            "Look in the external system. Applied: mark it done, with its output. Not applied: retry."
    StuckReason.CHECK_FAILING ->
        "The check of '$step' kept failing: the step may have been applied." to
            "Once the lookup works, look yourself. Applied: mark it done. Not applied: retry."
    StuckReason.TOO_MANY_ATTEMPTS ->
        "'$step' kept failing, up to the attempts allowed." to
            "Fix the cause, then retry. A compensation you did by hand: mark it compensated."
    StuckReason.OUTPUT_UNSTORABLE ->
        "'$step' was called, but its output could not be stored (it cannot be encoded, or is too large)." to
            "Mark it done with an output that fits, or abort with the current step applied."
    StuckReason.RECORD_FAILED ->
        "The outcome of '$step' could not be recorded. What it established is kept." to
            "The last error says which action follows, once the cause is fixed."
    StuckReason.NO_SIGNAL ->
        "No signal arrived for '$step' before its timeout, and the step has no check." to
            "Deliver the value by hand with mark done (onSignal still decides), or abort."
    StuckReason.KEY_EXPIRED ->
        "The service of '$step' may have forgotten its idempotency key: sending it again could duplicate the call." to
            "Look in the service. Applied: mark it done. Not applied: retry with a new key."
    StuckReason.RESUME_FAILING ->
        "The saga itself could not be run, again and again: a deployment to finish, data that cannot be read." to
            "Fix the cause, then retry: the step resumes as it was, checked first if it may have been called."
    StuckReason.HOOK_FAILED ->
        "Every step is done (or undone), but the saga's onCompleted or onFailed kept throwing." to
            "Fix the hook, then retry: it runs the hook alone, never a step."
}

/** The path segment of [action]'s form, under `saga/{id}/`. */
internal val ManualAction.slug: String
    get() = when (this) {
        ManualAction.RETRY -> "retry"
        ManualAction.RETRY_WITH_NEW_KEY -> "retry-new-key"
        ManualAction.MARK_DONE -> "mark-done"
        ManualAction.ABORT -> "abort"
        ManualAction.ABORT_CURRENT_STEP_APPLIED -> "abort-applied"
        ManualAction.MARK_COMPENSATED -> "mark-compensated"
        ManualAction.CANCEL -> "cancel"
    }

internal fun actionOf(slug: String): ManualAction? = ManualAction.entries.firstOrNull { it.slug == slug }

internal val ManualAction.label: String
    get() = when (this) {
        ManualAction.RETRY -> "Retry"
        ManualAction.RETRY_WITH_NEW_KEY -> "Retry with a new key"
        ManualAction.MARK_DONE -> "Mark done"
        ManualAction.ABORT -> "Abort"
        ManualAction.ABORT_CURRENT_STEP_APPLIED -> "Abort, the current step applied"
        ManualAction.MARK_COMPENSATED -> "Mark compensated"
        ManualAction.CANCEL -> "Cancel"
    }

/** What [action] does to [saga], said before it is done: the line under its button, and its confirmation. */
internal fun consequence(action: ManualAction, saga: SagaRecord, steps: List<SagaStepProgress>?): String {
    val current = steps?.firstOrNull { it.progress == StepProgress.CURRENT }?.name ?: "the current step"
    val done = steps.orEmpty().filter { it.progress == StepProgress.DONE }.map { it.name }.reversed()
    fun undoes(vararg first: String) = (first.toList() + done).let { if (it.isEmpty()) "nothing to undo" else "undoes ${it.joinToString(", ")}" }
    val unknown = saga.stuckReason == StuckReason.CANNOT_TELL || saga.stuckReason == StuckReason.CHECK_FAILING
    return when (action) {
        ManualAction.RETRY -> when {
            saga.state == SagaState.FINISHING -> "Runs the hook again, alone."
            saga.state == SagaState.COMPENSATING -> "Runs the undo of '$current' again."
            unknown -> "Calls '$current' again WITHOUT checking: only if you confirmed it was not applied."
            else -> "Runs '$current' again, as it was (replayed with its key if it has one)."
        }
        ManualAction.RETRY_WITH_NEW_KEY -> "Calls '$current' with a fresh key, as a new request: only if you confirmed it was not applied."
        ManualAction.MARK_DONE -> "The saga goes on as if '$current' had succeeded: only if you confirmed it was applied."
        ManualAction.ABORT -> if (saga.state == SagaState.FINISHING) {
            "Gives up: ${undoes()}."
        } else {
            "Gives up, '$current' taken as not applied: ${undoes()}."
        }
        ManualAction.ABORT_CURRENT_STEP_APPLIED -> "Gives up, '$current' taken as applied: ${undoes(current)}."
        ManualAction.MARK_COMPENSATED -> "The undo of '$current' is taken as done (by hand, or nothing to undo): the saga undoes the steps before."
        ManualAction.CANCEL -> "Abandons the saga: it undoes what it did, once no call is in flight."
    }
}
