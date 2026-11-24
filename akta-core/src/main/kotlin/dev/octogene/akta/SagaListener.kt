package dev.octogene.akta

/**
 * Told what the engine wrote to `saga_log`, once the transaction that wrote it has committed: for
 * counters (retries, compensations, lock expirations…) and application hooks (a notification when a
 * saga gets stuck). Pass listeners to [SagaEngine].
 *
 * - Called after commit only: a change that rolls back is never reported.
 * - At most once: a crash, or a connection lost after the commit, loses the events of that transaction.
 *   `saga_log` stays the record.
 * - A listener that throws is ignored: the saga is not affected. Keep it quick and non-blocking.
 * - [SagaEngine.create] runs in the caller's transaction, whose commit the engine cannot see: creations
 *   are not reported. Count them in the application.
 */
public fun interface SagaListener {
    public fun onEvent(event: SagaEvent)
}

/**
 * One entry of `saga_log`, with the saga as it stood when the entry was written. It carries what the log
 * carries (ids, event names, the refs from `logAs`, error messages), never an input, output or signal value.
 */
public data class SagaEvent(
    val entry: SagaLogEntry,
    val sagaType: String,
    val state: SagaState,
    val stepStatus: StepStatus,
    /** Set when the saga is stuck. */
    val stuckReason: StuckReason?,
)
