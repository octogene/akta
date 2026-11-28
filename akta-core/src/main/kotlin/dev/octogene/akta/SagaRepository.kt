package dev.octogene.akta

import kotlin.time.Instant
import kotlin.uuid.Uuid

/**
 * Storage for sagas. Every method is called INSIDE a transaction opened by [SagaTransactions].
 * The saga row is the lock of the whole saga: steps are never locked on their own.
 *
 * A saga's `lastError` and `failureReason`, and a log entry's `detail`, are kept up to [MAX_TEXT] characters:
 * longer text is cut.
 */
public interface SagaRepository {
    public companion object {
        /** The longest `lastError`, `failureReason` or log `detail` a repository keeps, in characters. */
        public const val MAX_TEXT: Int = 2000
    }

    /** Fails if another unfinished saga exists for the same key. */
    public fun insert(record: SagaRecord, now: Instant)

    /** Reads the saga and locks it until the end of the transaction. With [skipLocked], returns null if it is already locked. */
    public fun lock(id: Uuid, skipLocked: Boolean = false): SagaRecord?

    public fun find(id: Uuid): SagaRecord?

    public fun save(record: SagaRecord, now: Instant)

    /** Unfinished, non-stuck sagas of the given [types] whose `nextAttemptAt` has passed, oldest first. */
    public fun findDue(now: Instant, limit: Int, types: Collection<String>): List<Uuid>

    /** Encoded outputs of the completed steps that were not compensated, by step name. */
    public fun results(sagaId: Uuid): Map<String, String?>

    /** Every saved step result of the saga, undone ones included, by step index. */
    public fun stepResults(sagaId: Uuid): List<StepResultRecord>

    /** Saves a completed step's encoded output, replacing one already saved for that step. */
    public fun saveResult(sagaId: Uuid, stepIndex: Int, stepName: String, output: String?, now: Instant)

    public fun markCompensated(sagaId: Uuid, stepIndex: Int, now: Instant)

    public fun log(entry: SagaLogEntry)

    /**
     * Deletes up to [limit] finished sagas (COMPLETED or FAILED) last updated before [before], oldest first,
     * with their results, signals and log entries. Returns how many sagas were deleted.
     */
    public fun purgeFinished(before: Instant, limit: Int): Int

    /**
     * Stores the first signal named [name] received for the saga, [value] encoded. Returns false, storing
     * nothing, when one of that name was already received: a redelivered event changes nothing.
     */
    public fun saveSignal(sagaId: Uuid, name: String, value: String, now: Instant): Boolean

    /** The encoded value of the signal named [name] received for the saga, if any. */
    public fun signal(sagaId: Uuid, name: String): String?

    /** Stuck sagas of [types] (null: all), by `stuckAt` then id, after [after]. */
    public fun findStuck(limit: Int, types: Collection<String>?, after: StuckCursor?): List<SagaRecord>

    /**
     * The sagas of business [key]: the unfinished one first (at most one), whatever its createdAt, then the finished
     * ones newest first (by createdAt, then id). Pods' clocks may disagree: createdAt alone could put a finished saga
     * before the one that holds the key.
     */
    public fun findByKey(key: String, limit: Int): List<SagaRecord>

    /** The log of a saga, oldest first; entries of the same instant in the order they were written. */
    public fun history(sagaId: Uuid): List<SagaLogEntry>

    /**
     * The latest [limit] entries of a saga's log, oldest first, as [history] orders them. The default reads the whole
     * log: a database implementation should read only those entries.
     */
    public fun recentHistory(sagaId: Uuid, limit: Int): List<SagaLogEntry> = history(sagaId).takeLast(limit)

    /** The log entries of a trace, oldest first; entries of the same instant in the order they were written. */
    public fun historyByTrace(traceId: String, limit: Int): List<SagaLogEntry>

    /** Counts and ages of the unfinished sagas of [types] (null: all). */
    public fun stats(now: Instant, types: Collection<String>?): SagaStats
}

/** Opens a short transaction. External calls are never made inside [inTransaction]. */
public interface SagaTransactions {
    public suspend fun <T> inTransaction(block: () -> T): T
}

/**
 * An unfinished saga already holds [key]. [existingSagaId] is that saga, for an endpoint that answers with it
 * (an idempotent POST, say): null in the rare case where it finished between the insert and the lookup.
 */
public class DuplicateSagaException(
    public val key: String,
    public val existingSagaId: Uuid? = null,
    cause: Throwable? = null,
) : IllegalStateException(
    "An unfinished saga already exists for key $key" + (existingSagaId?.let { " (saga $it)" } ?: ""),
    cause,
)
