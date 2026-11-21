package com.example.saga.memory

import com.example.saga.DuplicateSagaException
import com.example.saga.SagaLogEntry
import com.example.saga.SagaRecord
import com.example.saga.SagaRepository
import com.example.saga.SagaTransactions
import com.example.saga.StepStatus
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock
import kotlin.time.Instant
import kotlin.uuid.Uuid

/**
 * In-memory repository, FOR TESTS ONLY: lets you test your sagas and steps without a database.
 * Use it with [InMemorySagaTransactions] built on the same repository, so a failed transaction is rolled back.
 */
class InMemorySagaRepository : SagaRepository {
    private val sagas = linkedMapOf<Uuid, SagaRecord>()
    private val results = mutableListOf<StoredResult>()
    private val logEntries = mutableListOf<SagaLogEntry>()
    private val updatedAt = hashMapOf<Uuid, Instant>()

    data class StoredResult(
        val sagaId: Uuid,
        val stepIndex: Int,
        val stepName: String,
        val output: String?,
        val doneAt: Instant,
        val compensatedAt: Instant? = null,
    )

    /** Full history, for assertions. */
    val log: List<SagaLogEntry> get() = logEntries.toList()

    fun storedResults(sagaId: Uuid): List<StoredResult> = results.filter { it.sagaId == sagaId }

    override fun insert(record: SagaRecord, now: Instant) {
        if (sagas.values.any { it.key == record.key && !it.state.isFinished }) throw DuplicateSagaException(record.key)
        sagas[record.id] = record
        updatedAt[record.id] = now
    }

    override fun lock(id: Uuid, skipLocked: Boolean): SagaRecord? = sagas[id]

    override fun find(id: Uuid): SagaRecord? = sagas[id]

    override fun save(record: SagaRecord, now: Instant) {
        sagas[record.id] = record
        updatedAt[record.id] = now
    }

    override fun findDue(now: Instant, limit: Int, types: Collection<String>): List<Uuid> =
        sagas.values
            .filter { it.type in types && !it.state.isFinished && it.stepStatus != StepStatus.STUCK && it.nextAttemptAt <= now }
            .sortedBy { it.nextAttemptAt }
            .take(limit)
            .map { it.id }

    override fun results(sagaId: Uuid): Map<String, String?> =
        results.filter { it.sagaId == sagaId && it.compensatedAt == null }.associate { it.stepName to it.output }

    override fun saveResult(sagaId: Uuid, stepIndex: Int, stepName: String, output: String?, now: Instant) {
        results.removeAll { it.sagaId == sagaId && it.stepIndex == stepIndex }
        results += StoredResult(sagaId, stepIndex, stepName, output, now)
    }

    override fun markCompensated(sagaId: Uuid, stepIndex: Int, now: Instant) {
        val i = results.indexOfFirst { it.sagaId == sagaId && it.stepIndex == stepIndex }
        if (i >= 0) results[i] = results[i].copy(compensatedAt = now)
    }

    override fun log(entry: SagaLogEntry) {
        logEntries += entry
    }

    override fun purgeFinished(before: Instant, limit: Int): Int {
        val ids = sagas.values
            .filter { it.state.isFinished && updatedAt.getValue(it.id) < before }
            .sortedBy { updatedAt.getValue(it.id) }
            .take(limit)
            .map { it.id }
            .toSet()
        logEntries.removeAll { it.sagaId in ids }
        results.removeAll { it.sagaId in ids }
        ids.forEach { sagas.remove(it); updatedAt.remove(it) }
        return ids.size
    }

    internal class Snapshot(
        val sagas: Map<Uuid, SagaRecord>,
        val results: List<StoredResult>,
        val logEntries: List<SagaLogEntry>,
        val updatedAt: Map<Uuid, Instant>,
    )

    internal fun snapshot() = Snapshot(LinkedHashMap(sagas), results.toList(), logEntries.toList(), HashMap(updatedAt))

    internal fun restore(snapshot: Snapshot) {
        sagas.clear(); sagas.putAll(snapshot.sagas)
        results.clear(); results.addAll(snapshot.results)
        logEntries.clear(); logEntries.addAll(snapshot.logEntries)
        updatedAt.clear(); updatedAt.putAll(snapshot.updatedAt)
    }
}

/**
 * Runs each "transaction" under one lock. For tests only.
 * Given the [repository], a transaction that throws is rolled back, as in a real database.
 */
class InMemorySagaTransactions(private val repository: InMemorySagaRepository? = null) : SagaTransactions {
    private val lock = ReentrantLock()

    override suspend fun <T> inTransaction(block: () -> T): T = lock.withLock {
        if (lock.holdCount > 1) return@withLock block()        // nested: part of the outer transaction
        val snapshot = repository?.snapshot()
        try {
            block()
        } catch (e: Throwable) {
            snapshot?.let { repository.restore(it) }
            throw e
        }
    }
}
