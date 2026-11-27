package dev.octogene.akta

import kotlin.time.Clock
import kotlin.time.Instant
import kotlin.uuid.Uuid

/**
 * The engine's transactions, and what it reads and writes in them: a saga's row read as a [SagaPhase] and written
 * back as a [SagaRecord], its log, and the events reported to the [listeners] once a transaction commits.
 *
 * Within a transaction ([tx]), [save] does not write at once: each saga it changed is written once, when the block
 * ends (or at [flush]). Nothing in a transaction reads a saga row back after changing it: each works on the phase it
 * locked.
 */
internal class SagaStore(
    private val transactions: SagaTransactions,
    val repository: SagaRepository,
    private val listeners: List<SagaListener>,
    private val clock: Clock,
) {
    /**
     * The transaction running on this thread: the sagas it touched as it last read or wrote them, its log
     * entries, and the sagas it changed, written once each when its block ends (see [save]).
     */
    private class Pending {
        val latest = HashMap<Uuid, SagaRecord>()
        val events = ArrayList<SagaEvent>()
        val unsaved = LinkedHashMap<Uuid, Pair<SagaRecord, Instant>>()
    }

    /**
     * Set while the engine's transaction block runs. The block is plain blocking code on one thread
     * ([SagaTransactions.inTransaction] takes a non-suspending block), so a thread local is safe here.
     */
    private val current = ThreadLocal<Pending?>()

    /** Truncated to microseconds: the precision of timestamptz, and recording an outcome compares lockedUntil. */
    fun now(): Instant = clock.now().let {
        Instant.fromEpochSeconds(it.epochSeconds, it.nanosecondsOfSecond / 1_000 * 1_000)
    }

    /** One transaction; its events go to the listeners once it has committed. A retried attempt starts afresh. */
    suspend fun <T> tx(block: () -> T): T {
        var committed: Pending? = null
        val result = transactions.inTransaction {
            val pending = Pending()
            committed = pending
            val outer = current.get()
            current.set(pending)
            try {
                block().also {
                    // Each saga the block changed, written once: a step recorded and the next one claimed in the
                    // same transaction is one UPDATE, not two (each is a new row version, a dead one to vacuum).
                    writeUnsaved(pending)
                }
            } finally {
                // remove(), not set(null): pooled threads are left exactly as they were found.
                if (outer == null) current.remove() else current.set(outer)
            }
        }
        committed?.let { emit(it.events) }
        return result
    }

    /**
     * Writes now the sagas this transaction changed so far, instead of when its block ends: before a hook, which may
     * read the saga table, or create the next saga for the same key.
     */
    fun flush() {
        current.get()?.let(::writeUnsaved)
    }

    /** Locks the saga's row and reads it as a phase: a row the engine must never have written throws here. */
    fun lock(id: Uuid, skipLocked: Boolean = false): SagaPhase? =
        repository.lock(id, skipLocked)?.also { current.get()?.latest?.put(it.id, it) }?.phase()

    /**
     * Saves [saga], as its row, when the transaction's block ends, so that a saga changed several times in one
     * transaction is written once. Outside the engine's transactions (none today), it writes at once.
     */
    fun save(saga: SagaPhase, now: Instant) {
        val row = saga.record()
        val r = row.copy(
            lastError = row.lastError?.storable(),
            failureReason = row.failureReason?.storable(),
            cancelReason = row.cancelReason?.storable(),
        )
        val pending = current.get()
        if (pending == null) {
            repository.save(r, now)
            return
        }
        pending.unsaved[r.id] = r to now
        pending.latest[r.id] = r
    }

    /** What a decision writes besides the row, for the current step of [r] (its name is only read to save a result). */
    fun write(effects: List<Effect>, r: SagaPhase, now: Instant, stepName: () -> String) {
        for (effect in effects) when (effect) {
            is Effect.SaveResult -> repository.saveResult(r.id, r.stepIndex, stepName(), effect.output, now)
            is Effect.SaveSignal -> repository.saveSignal(r.id, effect.signal, effect.value, now)
            Effect.MarkCompensated -> repository.markCompensated(r.id, r.stepIndex, now)
        }
    }

    fun log(
        sagaId: Uuid,
        stepIndex: Int,
        stepName: String?,
        event: String,
        detail: String?,
        now: Instant,
        trace: String?,
        actor: String? = null,
    ) {
        val entry = SagaLogEntry(sagaId, now, stepIndex, stepName, event, detail?.storable(), actor, trace)
        repository.log(entry)
        // Reported after commit, with the saga as this transaction last read or wrote it. None from create(),
        // which runs in the caller's transaction.
        val pending = current.get() ?: return
        val r = pending.latest[sagaId] ?: return
        pending.events += SagaEvent(entry, r.type, r.state, r.stepStatus, r.stuckReason)
    }

    /** Writes the sagas the transaction changed and has not written yet, once each. */
    private fun writeUnsaved(pending: Pending) {
        for ((record, at) in pending.unsaved.values) repository.save(record, at)
        pending.unsaved.clear()
    }

    private fun emit(events: List<SagaEvent>) {
        for (event in events) for (listener in listeners) {
            try {
                listener.onEvent(event)
            } catch (e: Throwable) {
                if (e.isFatal()) throw e        // anything else: ignored, the saga is not affected
            }
        }
    }

    /**
     * Free text (an error message, a reason, what describe() returned) as a database can keep it: PostgreSQL
     * refuses U+0000, which an exception quoting a binary response may carry. Refused, the text would prevent
     * the outcome it describes from being recorded at all, RECORD_FAILED included.
     */
    private fun String.storable(): String = replace('\u0000', '�')
}
