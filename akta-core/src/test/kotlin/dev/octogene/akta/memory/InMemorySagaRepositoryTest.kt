package dev.octogene.akta.memory

import dev.octogene.akta.SagaLogEntry
import dev.octogene.akta.SagaRecord
import dev.octogene.akta.SagaRepository
import dev.octogene.akta.SagaState
import dev.octogene.akta.StepStatus
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals
import kotlin.test.assertNull
import kotlin.test.assertFailsWith
import kotlin.time.Duration.Companion.seconds
import kotlin.time.Instant
import kotlin.uuid.ExperimentalUuidApi
import kotlin.uuid.Uuid

/** The in-memory repository behaves as ExposedSagaRepository does, so unit tests see what production would. */
@OptIn(ExperimentalUuidApi::class)
class InMemorySagaRepositoryTest {

    private val repository = InMemorySagaRepository()
    private val t0 = Instant.parse("2026-11-20T10:00:00Z")
    private val sagaId = Uuid.generateV7()

    private fun entry(at: Instant, event: String, detail: String? = null, traceId: String? = "trace-1") =
        SagaLogEntry(sagaId, at, stepIndex = 0, stepName = "stock", event = event, detail = detail, traceId = traceId)

    private fun record(lastError: String?) = SagaRecord(
        id = sagaId, type = "T", key = "item-1", state = SagaState.RUNNING, stepIndex = 0,
        stepStatus = StepStatus.READY, attempts = 0, nextAttemptAt = t0, lockedUntil = null,
        lastError = lastError, traceparent = null, createdAt = t0,
    )

    @Test
    fun `history is by time, then in the order written`() {
        repository.log(entry(t0 + 2.seconds, "late"))
        repository.log(entry(t0, "first"))
        repository.log(entry(t0, "second"))                          // same instant: after "first"
        repository.log(entry(t0 + 1.seconds, "middle"))

        val expected = listOf("first", "second", "middle", "late")
        assertEquals(expected, repository.history(sagaId).map { it.event })
        assertEquals(expected, repository.historyByTrace("trace-1", limit = 10).map { it.event })
        assertEquals(expected.take(2), repository.historyByTrace("trace-1", limit = 2).map { it.event })
    }

    @Test
    fun `lastError, failureReason and a log detail are cut at MAX_TEXT, as in the database`() {
        val long = "x".repeat(SagaRepository.MAX_TEXT + 500)

        repository.insert(record(lastError = long), t0)
        assertEquals(SagaRepository.MAX_TEXT, repository.find(sagaId)!!.lastError!!.length)

        repository.save(record(lastError = "y$long"), t0)
        assertEquals("y" + "x".repeat(SagaRepository.MAX_TEXT - 1), repository.find(sagaId)!!.lastError)

        repository.save(record(lastError = null).copy(state = SagaState.COMPENSATING, failureReason = long), t0)
        assertEquals(SagaRepository.MAX_TEXT, repository.find(sagaId)!!.failureReason!!.length)

        repository.log(entry(t0, "EXECUTE:Unknown", detail = long))
        assertEquals(SagaRepository.MAX_TEXT, repository.history(sagaId).single().detail!!.length)
    }

    @Test
    fun `text within MAX_TEXT is kept as is`() {
        val exact = "x".repeat(SagaRepository.MAX_TEXT)
        repository.insert(record(lastError = exact), t0)
        repository.log(entry(t0, "EXECUTE:Unknown", detail = "short"))

        assertEquals(exact, repository.find(sagaId)!!.lastError)
        assertEquals("short", repository.history(sagaId).single().detail)
    }

    @Test
    fun `save keeps what the database writes once, and leaves an unknown saga alone`() {
        repository.insert(record(lastError = null).copy(input = "{\"customerId\":\"C-1\"}"), t0)

        repository.save(record(lastError = "down").copy(type = "OTHER", key = "item-2", input = "{}", createdAt = t0 + 1.seconds), t0)

        val stored = repository.find(sagaId)!!
        assertEquals("down", stored.lastError)                               // what an UPDATE writes
        assertEquals("T", stored.type)                                       // what it never writes
        assertEquals("item-1", stored.key)
        assertEquals("{\"customerId\":\"C-1\"}", stored.input)
        assertEquals(t0, stored.createdAt)

        val unknown = record(lastError = null).copy(id = Uuid.generateV7())
        repository.save(unknown, t0)                                         // UPDATE of no row
        assertNull(repository.find(unknown.id))
    }

    @Test
    fun `a row the schema would refuse is refused`() {
        assertFailsWith<IllegalStateException> {                           // saga_lock
            repository.insert(record(lastError = null).copy(stepStatus = StepStatus.IN_PROGRESS), t0)
        }
        repository.insert(record(lastError = null), t0)
        assertFailsWith<IllegalStateException> {                           // saga_cancel
            repository.save(record(lastError = null).copy(cancelRequestedAt = t0), t0)
        }
        assertFailsWith<IllegalStateException> {                           // saga_stuck_reason
            repository.save(record(lastError = null).copy(stepStatus = StepStatus.STUCK), t0)
        }
        assertFailsWith<IllegalStateException> {                           // attempts >= 0
            repository.save(record(lastError = null).copy(attempts = -1), t0)
        }
    }
}
