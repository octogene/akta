package com.example.saga

import kotlin.time.Instant
import kotlin.uuid.Uuid

/**
 * Storage for sagas. Every method is called INSIDE a transaction opened by [SagaTransactions].
 * The saga row is the lock of the whole saga: steps are never locked on their own.
 */
interface SagaRepository {
    /** Fails if another unfinished saga exists for the same key. */
    fun insert(record: SagaRecord, now: Instant)

    /** Reads the saga and locks it until the end of the transaction. With [skipLocked], returns null if it is already locked. */
    fun lock(id: Uuid, skipLocked: Boolean = false): SagaRecord?

    fun find(id: Uuid): SagaRecord?

    fun save(record: SagaRecord, now: Instant)

    /** Unfinished, non-stuck sagas of the given [types] whose `nextAttemptAt` has passed, oldest first. */
    fun findDue(now: Instant, limit: Int, types: Collection<String>): List<Uuid>

    /** Encoded outputs of the completed steps that were not compensated, by step name. */
    fun results(sagaId: Uuid): Map<String, String?>

    /** Saves a completed step's encoded output, replacing one already saved for that step. */
    fun saveResult(sagaId: Uuid, stepIndex: Int, stepName: String, output: String?, now: Instant)

    fun markCompensated(sagaId: Uuid, stepIndex: Int, now: Instant)

    fun log(entry: SagaLogEntry)

    /**
     * Deletes up to [limit] finished sagas (COMPLETED or FAILED) last updated before [before], oldest first,
     * with their results and log entries. Returns how many sagas were deleted.
     */
    fun purgeFinished(before: Instant, limit: Int): Int
}

/** Opens a short transaction. External calls are never made inside [inTransaction]. */
interface SagaTransactions {
    suspend fun <T> inTransaction(block: () -> T): T
}

class DuplicateSagaException(key: String, cause: Throwable? = null) :
    IllegalStateException("An unfinished saga already exists for key $key", cause)
