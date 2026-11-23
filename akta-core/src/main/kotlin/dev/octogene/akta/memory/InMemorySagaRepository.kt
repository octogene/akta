package dev.octogene.akta.memory

import dev.octogene.akta.DuplicateSagaException
import dev.octogene.akta.SagaLogEntry
import dev.octogene.akta.SagaRecord
import dev.octogene.akta.SagaRepository
import dev.octogene.akta.SagaStats
import dev.octogene.akta.SagaTransactions
import dev.octogene.akta.StepStatus
import dev.octogene.akta.StuckCursor
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
    private val signals = linkedMapOf<Pair<Uuid, String>, String>()

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
        sagas[record.id] = record.truncated()
        updatedAt[record.id] = now
    }

    override fun lock(id: Uuid, skipLocked: Boolean): SagaRecord? = sagas[id]

    override fun find(id: Uuid): SagaRecord? = sagas[id]

    override fun save(record: SagaRecord, now: Instant) {
        sagas[record.id] = record.truncated()
        updatedAt[record.id] = now
    }

    /** As a database column would keep it: see [SagaRepository.MAX_TEXT]. */
    private fun SagaRecord.truncated() = copy(
        lastError = lastError?.take(SagaRepository.MAX_TEXT),
        failureReason = failureReason?.take(SagaRepository.MAX_TEXT),
    )

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
        logEntries += entry.copy(detail = entry.detail?.take(SagaRepository.MAX_TEXT))
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
        signals.keys.removeAll { it.first in ids }
        ids.forEach { sagas.remove(it); updatedAt.remove(it) }
        return ids.size
    }

    override fun saveSignal(sagaId: Uuid, name: String, value: String, now: Instant): Boolean =
        signals.putIfAbsent(sagaId to name, value) == null

    override fun signal(sagaId: Uuid, name: String): String? = signals[sagaId to name]

    override fun findStuck(limit: Int, types: Collection<String>?, after: StuckCursor?): List<SagaRecord> =
        sagas.values
            .filter { it.stepStatus == StepStatus.STUCK && (types == null || it.type in types) }
            .sortedWith(compareBy<SagaRecord> { it.stuckAt }.thenBy { it.id })
            .filter { after == null || it.stuckAt!! > after.stuckAt || (it.stuckAt == after.stuckAt && it.id > after.id) }
            .take(limit)

    // By time, then in the order written, like the database's (at, id): sortedBy is stable, and
    // logEntries is in the order written.
    override fun history(sagaId: Uuid): List<SagaLogEntry> =
        logEntries.filter { it.sagaId == sagaId }.sortedBy { it.at }

    override fun historyByTrace(traceId: String, limit: Int): List<SagaLogEntry> =
        logEntries.filter { it.traceId == traceId }.sortedBy { it.at }.take(limit)

    override fun stats(now: Instant, types: Collection<String>?): SagaStats {
        val unfinished = sagas.values.filter { !it.state.isFinished && (types == null || it.type in types) }
        val (stuck, active) = unfinished.partition { it.stepStatus == StepStatus.STUCK }
        return SagaStats(
            unfinished = unfinished.groupingBy { it.state to it.stepStatus }.eachCount().mapValues { it.value.toLong() },
            stuckByReason = stuck.groupingBy { it.stuckReason!! }.eachCount().mapValues { it.value.toLong() },
            oldestUnfinishedAt = active.minOfOrNull { it.createdAt },
            oldestDueAt = active.filter { it.nextAttemptAt <= now }.minOfOrNull { it.nextAttemptAt },
        )
    }

    internal class Snapshot(
        val sagas: Map<Uuid, SagaRecord>,
        val results: List<StoredResult>,
        val logEntries: List<SagaLogEntry>,
        val updatedAt: Map<Uuid, Instant>,
        val signals: Map<Pair<Uuid, String>, String>,
    )

    internal fun snapshot() =
        Snapshot(LinkedHashMap(sagas), results.toList(), logEntries.toList(), HashMap(updatedAt), LinkedHashMap(signals))

    internal fun restore(snapshot: Snapshot) {
        sagas.clear(); sagas.putAll(snapshot.sagas)
        results.clear(); results.addAll(snapshot.results)
        logEntries.clear(); logEntries.addAll(snapshot.logEntries)
        updatedAt.clear(); updatedAt.putAll(snapshot.updatedAt)
        signals.clear(); signals.putAll(snapshot.signals)
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
