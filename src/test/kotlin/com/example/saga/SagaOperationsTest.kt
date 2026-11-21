package com.example.saga

import com.example.saga.memory.InMemorySagaRepository
import com.example.saga.memory.InMemorySagaTransactions
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Timeout
import java.io.IOException
import java.sql.SQLException
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertNull
import kotlin.test.assertTrue
import kotlin.time.Clock
import kotlin.time.Duration
import kotlin.time.Duration.Companion.minutes
import kotlin.time.Duration.Companion.seconds
import kotlin.time.Instant
import kotlin.uuid.Uuid

/** What operators use: stuck reasons, the reads, stats, the listener and markCompensated. */
@Timeout(10)
class SagaOperationsTest {

    private class MutableClock(var now: Instant = Instant.parse("2026-11-21T10:00:00Z")) : Clock {
        override fun now(): Instant = now
        fun advance(d: Duration) { now += d }
    }

    /** Rolls back the [failAt]-th transaction from now (1 = the next one) after its work, as a lost connection would. */
    private class FlakyTransactions(private val inner: SagaTransactions) : SagaTransactions {
        var failAt = 0
        override suspend fun <T> inTransaction(block: () -> T): T = inner.inTransaction {
            val result = block()
            if (failAt > 0 && --failAt == 0) throw SQLException("connection lost: rolled back")
            result
        }
    }

    private val clock = MutableClock()
    private val repository = InMemorySagaRepository()
    private val transactions = FlakyTransactions(InMemorySagaTransactions(repository))
    private val events = mutableListOf<SagaEvent>()
    private var trace: String? = null

    private fun engine(vararg definitions: SagaDefinition<Unit>, config: SagaConfig = SagaConfig(), listeners: List<SagaListener>? = null) =
        SagaEngine(
            transactions, repository, definitions.toList(), KotlinxJsonSagaSerializer(), config, clock,
            traceId = { trace }, listeners = listeners ?: listOf(SagaListener { events += it }),
        )

    private fun SagaEngine.run(id: Uuid) = runBlocking { run(id, clock.now + 10.seconds) }
    private fun SagaEngine.stored(id: Uuid) = runBlocking { find(id)!! }
    private fun SagaEngine.resumeAfter(d: Duration) {
        clock.advance(d)
        runBlocking { resumeDue() }
    }

    /** One step that is never sent: with maxAttempts = 1, the saga is stuck at its first run. */
    private fun notSent(type: String = "NOT_SENT") = saga(type) {
        step("call") { execute { throw StepNotSent("connection refused") } }
    }

    private val once = SagaConfig(maxAttempts = 1)

    // ---- Stuck reasons ------------------------------------------------------------------------

    @Test
    fun `each path to a stuck saga gives its reason and the time it got stuck`() {
        val unknown = saga("CANNOT_TELL") {
            step("call") { execute { throw IOException("read timeout") }; check { CheckResult.CannotTell } }
        }
        val failingCheck = saga("CHECK_FAILING") {
            step("call") { execute { throw IOException("read timeout") }; check { throw IOException("lookup down") } }
        }
        val tooBig = saga("OUTPUT_UNSTORABLE") { step<String>("call") { execute { "x".repeat(100) } } }
        val hook = saga("RECORD_FAILED") {
            step("call") { execute { } }
            onCompleted { error("hook failed") }
        }
        val signal = saga("NO_SIGNAL") { awaitSignal<String>("wait", signal = "payment", timeout = 1.minutes) }
        val definitions = arrayOf(unknown, failingCheck, tooBig, hook, signal, notSent())
        val engine = engine(
            *definitions,
            config = SagaConfig(maxAttempts = 1, settleDelay = 1.seconds, checkRetryDelay = 1.seconds, maxDataBytes = 50),
        )
        val ids = definitions.associate { it.type to engine.create(it, "K-${it.type}") }
        val started = clock.now
        ids.values.forEach { engine.run(it) }
        engine.resumeAfter(2.minutes)                                           // checks and the signal timeout

        val reasons = ids.mapValues { (_, id) -> engine.stored(id).stuckReason }
        assertEquals(
            mapOf(
                "CANNOT_TELL" to StuckReason.CANNOT_TELL, "CHECK_FAILING" to StuckReason.CHECK_FAILING,
                "OUTPUT_UNSTORABLE" to StuckReason.OUTPUT_UNSTORABLE, "RECORD_FAILED" to StuckReason.RECORD_FAILED,
                "NO_SIGNAL" to StuckReason.NO_SIGNAL, "NOT_SENT" to StuckReason.TOO_MANY_ATTEMPTS,
            ),
            reasons,
        )
        assertEquals(started, engine.stored(ids.getValue("NOT_SENT")).stuckAt)    // stuck at once
        assertEquals(clock.now, engine.stored(ids.getValue("CANNOT_TELL")).stuckAt)
    }

    @Test
    fun `leaving STUCK clears the reason, by retry or by markDone`() {
        var fail = true
        val definition = saga("T") {
            step("call") { execute { if (fail) throw StepNotSent("down") } }
            step("next") { execute { } }
        }
        val engine = engine(definition, config = once)
        val retried = engine.create(definition, "K-1")
        val marked = engine.create(definition, "K-2")
        engine.run(retried)
        engine.run(marked)
        assertEquals(StuckReason.TOO_MANY_ATTEMPTS, engine.stored(retried).stuckReason)

        fail = false
        val afterRetry = runBlocking { engine.retry(retried, "operator") }
        val afterMarkDone = runBlocking { engine.markDone(marked, null, "operator") }

        for (r in listOf(afterRetry, afterMarkDone)) {
            assertEquals(SagaState.COMPLETED, r.state)
            assertNull(r.stuckReason)
            assertNull(r.stuckAt)
        }
    }

    // ---- Reads --------------------------------------------------------------------------------

    @Test
    fun `findStuck lists stuck sagas oldest first, by page and by type`() {
        val a = notSent("A")
        val b = notSent("B")
        val engine = engine(a, b, config = once)
        val early = engine.create(a, "K-0").also { engine.run(it) }
        clock.advance(1.seconds)
        // Stuck at the same instant: the cursor must go on by id.
        val sameInstant = (1..4).map { i -> engine.create(if (i == 2) b else a, "K-$i").also { engine.run(it) } }
        val ids = listOf(early) + sameInstant.sorted()

        val first = runBlocking { engine.findStuck(limit = 2) }
        val second = runBlocking { engine.findStuck(limit = 2, after = StuckCursor.after(first.last())) }
        val third = runBlocking { engine.findStuck(limit = 2, after = StuckCursor.after(second.last())) }

        assertEquals(ids, (first + second + third).map { it.id })
        assertEquals(ids - sameInstant[1], runBlocking { engine.findStuck(types = listOf("A")) }.map { it.id })
    }

    @Test
    fun `history gives a saga's log in order, and historyByTrace a request's`() {
        val definition = saga("T") { step("one") { execute { } }; step("two") { execute { } } }
        val engine = engine(definition)
        trace = "trace-1"
        val id = engine.create(definition, "K-1")
        engine.run(id)
        trace = "trace-2"
        val other = engine.create(definition, "K-2")
        engine.run(other)

        assertEquals(
            listOf(SagaEvents.CREATED, "EXECUTE:Done", "EXECUTE:Done"),
            runBlocking { engine.history(id) }.map { it.event },
        )
        assertTrue(runBlocking { engine.historyByTrace("trace-1") }.all { it.sagaId == id })
        assertEquals(3, runBlocking { engine.historyByTrace("trace-1") }.size)
    }

    @Test
    fun `stats counts unfinished sagas, stuck ones by reason, and the recovery lag`() {
        val stuck = notSent("STUCK")
        val waiting = saga("WAITING") { awaitSignal<String>("wait", signal = "payment", timeout = 30.minutes) }
        val done = saga("DONE") { step("call") { execute { } } }
        val engine = engine(stuck, waiting, done, config = once)
        val created = clock.now
        engine.run(engine.create(stuck, "K-1"))
        engine.run(engine.create(waiting, "K-2"))
        engine.run(engine.create(done, "K-3"))
        engine.create(done, "K-4")                                              // due, never run
        clock.advance(5.minutes)

        val stats = runBlocking { engine.stats() }

        assertEquals(
            mapOf(
                (SagaState.RUNNING to StepStatus.STUCK) to 1L,
                (SagaState.RUNNING to StepStatus.WAITING) to 1L,
                (SagaState.RUNNING to StepStatus.READY) to 1L,
            ),
            stats.unfinished,
        )
        assertEquals(mapOf(StuckReason.TOO_MANY_ATTEMPTS to 1L), stats.stuckByReason)
        assertEquals(created, stats.oldestUnfinishedAt)
        assertEquals(created, stats.oldestDueAt)                                // K-4, waiting 5 minutes
        assertEquals(mapOf((SagaState.RUNNING to StepStatus.WAITING) to 1L), runBlocking { engine.stats(listOf("WAITING")) }.unfinished)
    }

    // ---- Listener -----------------------------------------------------------------------------

    @Test
    fun `the listener gets the log entries after commit, with the saga as it then stood`() {
        val definition = saga("T") {
            step("call") { execute { throw IOException("read timeout") }; check { CheckResult.CannotTell } }
        }
        val engine = engine(definition, config = SagaConfig(settleDelay = 1.seconds))
        val id = engine.create(definition, "K-1")
        engine.run(id)
        engine.resumeAfter(2.seconds)

        val logged = repository.log.filter { it.event != SagaEvents.CREATED }  // create is not reported
        assertEquals(logged, events.map { it.entry })
        val stuck = events.last()
        assertEquals("CHECK:CannotTell", stuck.entry.event)
        assertEquals("T", stuck.sagaType)
        assertEquals(StepStatus.STUCK, stuck.stepStatus)
        assertEquals(StuckReason.CANNOT_TELL, stuck.stuckReason)
        assertEquals(StepStatus.UNKNOWN, events.first().stepStatus)             // EXECUTE:Unknown
    }

    @Test
    fun `a transaction that rolls back reports nothing`() {
        val definition = saga("T") { step("call") { execute { } } }
        val engine = engine(definition)
        val id = engine.create(definition, "K-1")

        transactions.failAt = 2                     // the record transaction: it logged EXECUTE:Done, then rolled back
        assertEquals(SagaState.COMPLETED, engine.run(id).state)                 // recorded again, alone

        assertEquals(listOf("EXECUTE:Done"), events.map { it.entry.event })    // once: the committed one
        assertEquals(1, repository.log.count { it.event == "EXECUTE:Done" })
    }

    @Test
    fun `a listener that throws does not affect the saga`() {
        val definition = saga("T") { step("one") { execute { } }; step("two") { execute { } } }
        val engine = engine(definition, listeners = listOf(SagaListener { error("broken listener") }, SagaListener { events += it }))
        val id = engine.create(definition, "K-1")

        assertEquals(SagaState.COMPLETED, engine.run(id).state)
        assertEquals(2, events.size)                                            // the next listener still runs
    }

    // ---- markCompensated ----------------------------------------------------------------------

    @Test
    fun `markCompensated ends a stuck compensation, and goes on undoing`() {
        val undone = mutableListOf<String>()
        var failed = false
        val definition = saga("T") {
            step("one") { execute { }; compensate { undone += "one" } }
            step("two") { execute { }; compensate { throw IOException("undo failed") } }
            step("three") { execute { throw StepRejected("refused") } }
            onFailed { failed = true }
        }
        val engine = engine(definition, config = once)
        val id = engine.create(definition, "K-1")
        val stuck = engine.run(id)
        assertEquals(SagaState.COMPENSATING to StuckReason.TOO_MANY_ATTEMPTS, stuck.state to stuck.stuckReason)
        assertFailsWith<IllegalArgumentException> { runBlocking { engine.markDone(id, null, "operator") } }

        val done = runBlocking { engine.markCompensated(id, "operator") }       // "two" was undone by hand

        assertEquals(SagaState.FAILED, done.state)
        assertEquals(listOf("one"), undone)
        assertTrue(failed)
        assertTrue(repository.storedResults(id).all { it.compensatedAt != null })
        assertEquals("operator", repository.log.single { it.event == SagaEvents.MANUAL_COMPENSATED }.actor)
    }

    @Test
    fun `markCompensated is refused unless a compensation is stuck`() {
        val definition = notSent()
        val engine = engine(definition, config = once)
        val id = engine.create(definition, "K-1")
        engine.run(id)                                                          // stuck, but forward

        assertFailsWith<IllegalArgumentException> { runBlocking { engine.markCompensated(id, "operator") } }
    }
}
