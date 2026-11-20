package com.example.saga

import com.example.saga.memory.InMemorySagaRepository
import com.example.saga.memory.InMemorySagaTransactions
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import kotlinx.coroutines.awaitCancellation
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import kotlinx.coroutines.yield
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Timeout
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import java.util.concurrent.atomic.AtomicInteger
import kotlin.test.assertTrue
import kotlin.time.Clock
import kotlin.time.Duration
import kotlin.time.Duration.Companion.days
import kotlin.time.Duration.Companion.hours
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.minutes
import kotlin.time.Duration.Companion.seconds
import kotlin.time.Instant
import kotlin.uuid.Uuid

@Timeout(10)                // a step that never returns must fail the test, not hang the build
class SagaEngineTest {

    // ---- Fixtures ---------------------------------------------------------------------------

    private class MutableClock(var now: Instant = Instant.parse("2026-11-19T10:00:00Z")) : Clock {
        override fun now(): Instant = now
        fun advance(d: Duration) { now += d }
    }

    /** A scripted step: each call pops the next behaviour; the last one repeats. */
    private class FakeStep(
        override val name: String,
        vararg behaviours: () -> String?,
        private val checkResult: (() -> CheckResult)? = null,
        private val compensation: (() -> Unit)? = null,
    ) : SagaStep {
        private val queue = ArrayDeque(behaviours.toList())
        var executions = 0
        var compensations = 0
        var checks = 0

        override suspend fun execute(ctx: SagaContext): String? {
            executions++
            val next = if (queue.size > 1) queue.removeFirst() else queue.first()
            return next()
        }

        override suspend fun compensate(ctx: SagaContext) {
            compensations++
            compensation?.invoke()
        }

        override suspend fun check(ctx: SagaContext): CheckResult {
            checks++
            return checkResult?.invoke() ?: CheckResult.CannotTell
        }
    }

    private val clock = MutableClock()
    private val repository = InMemorySagaRepository()
    private val completed = mutableListOf<String>()
    private val failed = mutableListOf<String>()

    private fun engine(vararg steps: SagaStep, config: SagaConfig = SagaConfig()) = SagaEngine(
        transactions = InMemorySagaTransactions(repository),
        repository = repository,
        definitions = listOf(
            SagaDefinition(
                type = "TEST",
                steps = steps.toList(),
                onCompleted = { completed += it.key },
                onFailed = { failed += it.key },
            ),
        ),
        config = config,
        clock = clock,
    )

    private fun SagaEngine.start(key: String = "item-1") = runBlocking {
        val id = create("TEST", key)
        run(id, deadline = clock.now + 10.seconds)
    }

    private fun SagaEngine.resumeAfter(d: Duration): Int {
        clock.advance(d)
        val result = runBlocking { resumeDue() }
        assertEquals(emptyMap(), result.failures)
        return result.sagas
    }

    private fun SagaEngine.state(r: SagaRecord) = runBlocking { find(r.id)!! }

    // ---- Tests ------------------------------------------------------------------------------

    @Test
    fun `runs every step and completes`() {
        val stock = FakeStep("stock", { "STK-1" })
        val booking = FakeStep("booking", { "RES-1" })
        val saga = engine(stock, booking).start()

        assertEquals(SagaState.COMPLETED, saga.state)
        assertEquals(mapOf("stock" to "STK-1", "booking" to "RES-1"), repository.results(saga.id))
        assertEquals(listOf("item-1"), completed)
    }

    @Test
    fun `later steps see the references of earlier steps`() {
        var seen: String? = null
        val stock = FakeStep("stock", { "STK-1" })
        val booking = object : SagaStep {
            override val name = "booking"
            override suspend fun execute(ctx: SagaContext): String? { seen = ctx.result("stock"); return "RES-1" }
        }
        engine(stock, booking).start()
        assertEquals("STK-1", seen)
    }

    @Test
    fun `a rejection undoes the completed steps and fails the saga`() {
        val stock = FakeStep("stock", { "STK-1" })
        val booking = FakeStep("booking", { throw StepRejected("model not bookable") })
        val saga = engine(stock, booking).start()

        assertEquals(SagaState.FAILED, saga.state)
        assertEquals(1, stock.compensations)
        assertEquals(0, booking.compensations)
        assertEquals(emptyMap<String, String?>(), repository.results(saga.id))
        assertEquals(listOf("item-1"), failed)
    }

    @Test
    fun `a rejection of the first step fails without compensation`() {
        val stock = FakeStep("stock", { throw StepRejected("unknown EPC") })
        val saga = engine(stock).start()

        assertEquals(SagaState.FAILED, saga.state)
        assertEquals(0, stock.compensations)
    }

    @Test
    fun `a request that was not sent is retried later`() {
        val stock = FakeStep("stock", { throw StepNotSent("connection refused") }, { "STK-1" })
        val engine = engine(stock)
        val saga = engine.start()

        assertEquals(SagaState.RUNNING, saga.state)
        assertEquals(StepStatus.READY, saga.stepStatus)
        assertEquals(0, engine.resumeAfter(30.seconds))      // backoff: 1 minute
        engine.resumeAfter(31.seconds)
        assertEquals(SagaState.COMPLETED, engine.state(saga).state)
        assertEquals(2, stock.executions)
    }

    @Test
    fun `an unknown outcome is checked after the settle delay, and adopted if found`() {
        val stock = FakeStep(
            "stock", { throw RuntimeException("read timeout") },
            checkResult = { CheckResult.Done("STK-1") },
        )
        val engine = engine(stock)
        val saga = engine.start()

        assertEquals(StepStatus.UNKNOWN, saga.stepStatus)
        engine.resumeAfter(1.minutes)
        assertEquals(0, stock.checks)                                     // settle delay not over
        engine.resumeAfter(1.minutes)
        val done = engine.state(saga)
        assertEquals(SagaState.COMPLETED, done.state)
        assertEquals(1, stock.executions)                                 // never executed twice
        assertEquals("STK-1", repository.results(saga.id)["stock"])
    }

    @Test
    fun `an unknown outcome that was not applied is executed again`() {
        val stock = FakeStep(
            "stock", { throw RuntimeException("502") }, { "STK-1" },
            checkResult = { CheckResult.NotDone },
        )
        val engine = engine(stock)
        val saga = engine.start()
        engine.resumeAfter(2.minutes)

        assertEquals(SagaState.COMPLETED, engine.state(saga).state)
        assertEquals(2, stock.executions)
    }

    @Test
    fun `without a check, an unknown outcome gets stuck and can be marked done by hand`() {
        val stock = FakeStep("stock", { throw RuntimeException("read timeout") })
        val booking = FakeStep("booking", { "RES-1" })
        val engine = engine(stock, booking)
        val saga = engine.start()
        engine.resumeAfter(2.minutes)
        assertEquals(StepStatus.STUCK, engine.state(saga).stepStatus)

        val after = runBlocking { engine.markDone(saga.id, "STK-9", actor = "alice") }
        assertEquals(SagaState.COMPLETED, after.state)
        assertEquals("STK-9", repository.results(saga.id)["stock"])
        assertEquals("alice", repository.log.single { it.event == SagaEvents.MANUAL_DONE }.actor)
    }

    @Test
    fun `a call interrupted by a crash is treated as unknown when its lock expires`() {
        var executions = 0
        val stock = object : SagaStep {
            override val name = "stock"
            override suspend fun execute(ctx: SagaContext): String? {
                if (++executions == 1) awaitCancellation()                // the pod stops mid-call
                return "STK-2"
            }
            override suspend fun check(ctx: SagaContext) = CheckResult.NotDone
        }
        val engine = engine(stock)
        val id = runBlocking { InMemorySagaTransactions(repository).inTransaction { engine.create("TEST", "item-1") } }
        crash({ executions == 1 }) { engine.run(id, clock.now + 10.seconds) }
        assertEquals(StepStatus.IN_PROGRESS, repository.find(id)!!.stepStatus)

        engine.resumeAfter(36.seconds)                        // lock (35 s) expired
        assertEquals(StepStatus.UNKNOWN, repository.find(id)!!.stepStatus)
        engine.resumeAfter(2.minutes)                         // check, then execute again
        assertEquals(SagaState.COMPLETED, repository.find(id)!!.state)
        assertEquals(2, executions)
    }

    @Test
    fun `a timeout inside a step is an unknown outcome, not a cancellation of the engine`() {
        val stock = object : SagaStep {
            override val name = "stock"
            override suspend fun execute(ctx: SagaContext): String? = withTimeout(1) { awaitCancellation() }
        }
        val saga = engine(stock).start()                                  // returns normally

        assertEquals(StepStatus.UNKNOWN, saga.stepStatus)
        assertTrue(saga.lastError!!.contains("TimeoutCancellationException"))
    }

    @Test
    fun `an unknown outcome that is never applied gets stuck after max attempts`() {
        val stock = FakeStep("stock", { throw RuntimeException("502") }, checkResult = { CheckResult.NotDone })
        val engine = engine(stock, config = SagaConfig(maxAttempts = 3))
        val saga = engine.start()
        repeat(5) { engine.resumeAfter(2.minutes) }

        val stuck = engine.state(saga)
        assertEquals(StepStatus.STUCK, stuck.stepStatus)
        assertEquals(3, stock.executions)
        assertTrue(stuck.lastError!!.startsWith("Too many attempts"))
    }

    @Test
    fun `a failing check backs off but never gets stuck`() {
        val stock = FakeStep("stock", { throw RuntimeException("timeout") },
            checkResult = { throw RuntimeException("lookup down") })
        val engine = engine(stock, config = SagaConfig(maxAttempts = 2))
        val saga = engine.start()
        engine.resumeAfter(2.minutes)                         // 1st check fails: retry in 2 min
        engine.resumeAfter(1.minutes)
        assertEquals(1, stock.checks)
        engine.resumeAfter(1.minutes)                         // 2nd check fails: retry in 4 min
        engine.resumeAfter(3.minutes)
        assertEquals(2, stock.checks)
        repeat(10) { engine.resumeAfter(1.hours) }

        assertEquals(StepStatus.UNKNOWN, engine.state(saga).stepStatus)
        assertEquals(1, stock.executions)
    }

    @Test
    fun `the log records each outcome on the step that produced it, with its reference`() {
        val engine = engine(FakeStep("stock", { "STK-1" }), FakeStep("booking", { "RES-1" }))
        engine.start()

        val done = repository.log.filter { it.event == "EXECUTE:Done" }.map { Triple(it.stepIndex, it.stepName, it.detail) }
        assertEquals(listOf(Triple(0, "stock", "ref=STK-1"), Triple(1, "booking", "ref=RES-1")), done)
    }

    @Test
    fun `a failed compensation is retried`() {
        var failures = 1
        val stock = FakeStep("stock", { "STK-1" }, compensation = {
            if (failures-- > 0) throw RuntimeException("stock unavailable")
        })
        val booking = FakeStep("booking", { throw StepRejected("refused") })
        val engine = engine(stock, booking)
        val saga = engine.start()
        assertEquals(SagaState.COMPENSATING, saga.state)

        engine.resumeAfter(1.minutes)
        assertEquals(SagaState.FAILED, engine.state(saga).state)
        assertEquals(2, stock.compensations)
    }

    @Test
    fun `too many failed attempts make the step stuck, and a manual retry resumes it`() {
        var calls = 0
        val stock = FakeStep("stock", { if (++calls <= 3) throw StepNotSent("down") else "STK-1" })
        val engine = SagaEngine(
            InMemorySagaTransactions(repository), repository,
            listOf(SagaDefinition("TEST", listOf(stock))),
            config = SagaConfig(maxAttempts = 2), clock = clock,
        )
        val saga = engine.start()
        engine.resumeAfter(1.minutes)
        assertEquals(StepStatus.STUCK, engine.state(saga).stepStatus)

        runBlocking { engine.retry(saga.id, actor = "bob") }              // 3rd attempt fails again
        engine.resumeAfter(1.minutes)                         // 4th succeeds
        assertEquals(SagaState.COMPLETED, engine.state(saga).state)
    }

    @Test
    fun `abort undoes what was done`() {
        val stock = FakeStep("stock", { "STK-1" })
        val booking = FakeStep("booking", { throw StepNotSent("down") })
        val engine = engine(stock, booking)
        val saga = engine.start()

        val after = runBlocking { engine.abort(saga.id, actor = "carol", currentStepApplied = false) }
        assertEquals(SagaState.FAILED, after.state)
        assertEquals(1, stock.compensations)
        assertEquals(0, booking.compensations)
    }

    @Test
    fun `abort of a stuck step that was applied compensates it too, with its reference`() {
        var compensatedRef: String? = null
        val stock = FakeStep("stock", { "STK-1" })
        val booking = object : SagaStep {
            override val name = "booking"
            override suspend fun execute(ctx: SagaContext): String? = throw RuntimeException("timeout")
            override suspend fun compensate(ctx: SagaContext) { compensatedRef = ctx.result("booking") }
        }
        val engine = engine(stock, booking)
        val saga = engine.start()
        engine.resumeAfter(2.minutes)                         // no check: cannot tell
        assertEquals(StepStatus.STUCK, engine.state(saga).stepStatus)

        val after = runBlocking { engine.abort(saga.id, "carol", currentStepApplied = true, ref = "RES-7") }
        assertEquals(SagaState.FAILED, after.state)
        assertEquals("RES-7", compensatedRef)
        assertEquals(1, stock.compensations)
        assertEquals(1, repository.log.single { it.event == SagaEvents.MANUAL_ABORT }.stepIndex)   // logged on booking
    }

    @Test
    fun `abort refuses to compensate a READY step, which was never applied`() {
        val engine = engine(FakeStep("stock", { throw StepNotSent("down") }))
        val saga = engine.start()
        assertFailsWith<IllegalArgumentException> {
            runBlocking { engine.abort(saga.id, actor = "carol", currentStepApplied = true) }
        }
    }

    @Test
    fun `abort is refused while the outcome of a call is unknown`() {
        val stock = FakeStep("stock", { throw RuntimeException("timeout") })
        val engine = engine(stock)
        val saga = engine.start()
        assertFailsWith<IllegalArgumentException> {
            runBlocking { engine.abort(saga.id, actor = "carol", currentStepApplied = false) }
        }
    }

    @Test
    fun `resumeDue leaves alone the types it does not know, as during a rolling deploy`() {
        val stock = FakeStep("stock", { "STK-1" })
        fun definition(type: String) = SagaDefinition(type, listOf(stock))
        val newPod = SagaEngine(InMemorySagaTransactions(repository), repository,
            listOf(definition("NEW"), definition("TEST")), clock = clock)
        val brandNew = newPod.create("NEW", "item-1")
        val current = newPod.create("TEST", "item-2")

        val oldPod = SagaEngine(InMemorySagaTransactions(repository), repository, listOf(definition("TEST")), clock = clock)
        val result = runBlocking { oldPod.resumeDue() }

        assertEquals(1, result.sagas)
        assertEquals(emptyMap(), result.failures)
        assertEquals(SagaState.COMPLETED, repository.find(current)!!.state)
        assertEquals(StepStatus.READY, repository.find(brandNew)!!.stepStatus)        // untouched
    }

    @Test
    fun `a saga that throws on every resume is postponed, so it cannot starve the others`() {
        lateinit var poison: Uuid
        val failing = object : SagaRepository by repository {
            override fun results(sagaId: Uuid): Map<String, String?> =
                if (sagaId == poison) throw IllegalStateException("corrupted row") else repository.results(sagaId)
        }
        val definition = SagaDefinition("TEST", listOf(FakeStep("stock", { "STK-1" })))
        val engine = SagaEngine(InMemorySagaTransactions(repository), failing, listOf(definition), clock = clock)
        poison = engine.create("TEST", "item-1")
        clock.advance(1.seconds)
        val healthy = engine.create("TEST", "item-2")

        // limit = 1: each batch only takes the oldest due saga, which is the failing one.
        val first = runBlocking { engine.resumeDue(limit = 1) }
        assertEquals(setOf(poison), first.failures.keys)
        assertEquals(1, repository.log.count { it.sagaId == poison && it.event == SagaEvents.RESUME_FAILED })

        val second = runBlocking { engine.resumeDue(limit = 1) }                   // postponed: the other one runs
        assertEquals(emptyMap(), second.failures)
        assertEquals(SagaState.COMPLETED, repository.find(healthy)!!.state)

        clock.advance(1.minutes)                                                 // resumeFailureDelay
        assertEquals(setOf(poison), runBlocking { engine.resumeDue(limit = 1) }.failures.keys)
    }

    @Test
    fun `a throwing onCompleted makes the saga stuck, keeping the reference, instead of looping`() {
        var hookFails = true
        val stock = FakeStep("stock", { "STK-1" })
        val engine = SagaEngine(
            InMemorySagaTransactions(repository), repository,
            listOf(SagaDefinition("TEST", listOf(stock), onCompleted = {
                check(!hookFails) { "items table locked" }
                completed += it.key
            })),
            clock = clock,
        )
        val saga = runBlocking { engine.create("TEST", "item-1").let { engine.run(it, clock.now + 10.seconds) } }

        assertEquals(StepStatus.STUCK, saga.stepStatus)
        assertEquals(SagaState.RUNNING, saga.state)                       // rolled back, not COMPLETED
        assertTrue(saga.lastError!!.contains("ref=STK-1"))
        assertEquals(emptyMap<String, String?>(), repository.results(saga.id))
        assertEquals(1, repository.log.count { it.event == SagaEvents.RECORD_FAILED })

        hookFails = false                                                 // the bug is fixed
        val after = runBlocking { engine.markDone(saga.id, "STK-1", actor = "alice") }
        assertEquals(SagaState.COMPLETED, after.state)
        assertEquals(listOf("item-1"), completed)
        assertEquals(1, stock.executions)
    }

    @Test
    fun `a compensation that keeps outliving its lock gets stuck`() {
        val stock = object : SagaStep {
            override val name = "stock"
            var compensations = 0
            override suspend fun execute(ctx: SagaContext) = "STK-1"
            override suspend fun compensate(ctx: SagaContext) {
                compensations++
                awaitCancellation()                                       // the pod stops mid-call
            }
        }
        val booking = FakeStep("booking", { throw StepRejected("refused") })
        val engine = engine(stock, booking, config = SagaConfig(maxAttempts = 2))
        val id = runBlocking { InMemorySagaTransactions(repository).inTransaction { engine.create("TEST", "item-1") } }
        crash({ stock.compensations == 1 }) { engine.run(id, clock.now + 10.seconds) }
        clock.advance(36.seconds)
        crash({ stock.compensations == 2 }) { engine.resumeDue() }        // lock expired, 2nd compensation
        engine.resumeAfter(36.seconds)                        // lock expired again

        val stuck = repository.find(id)!!
        assertEquals(StepStatus.STUCK, stuck.stepStatus)
        assertEquals(SagaState.COMPENSATING, stuck.state)
        assertEquals(2, stock.compensations)
    }

    @Test
    fun `the trace id is read on the caller's thread, not on the transaction thread`() {
        val currentTrace = ThreadLocal<String?>()
        val inMemory = InMemorySagaTransactions(repository)
        val onAnotherThread = object : SagaTransactions {
            override suspend fun <T> inTransaction(block: () -> T): T =
                withContext(Dispatchers.IO) { inMemory.inTransaction(block) }
        }
        val engine = SagaEngine(
            onAnotherThread, repository, listOf(SagaDefinition("TEST", listOf(FakeStep("stock", { "STK-1" })))),
            clock = clock, traceId = { currentTrace.get() },
        )
        currentTrace.set("trace-1")
        runBlocking {
            val id = engine.create("TEST", "item-1")
            engine.run(id, clock.now + 10.seconds)
        }

        assertEquals(setOf("trace-1"), repository.log.map { it.traceId }.toSet())
    }

    @Test
    fun `resumeDue runs sagas concurrently, so a slow one does not hold up the others`() {
        // Each call waits until both sagas are in flight: this only finishes if they run at the same time.
        val inFlight = AtomicInteger()
        val bothInFlight = CompletableDeferred<Unit>()
        val stock = object : SagaStep {
            override val name = "stock"
            override suspend fun execute(ctx: SagaContext): String {
                if (inFlight.incrementAndGet() == 2) bothInFlight.complete(Unit)
                bothInFlight.await()
                return "STK-${ctx.key}"
            }
        }
        val engine = engine(stock)
        val ids = listOf("item-1", "item-2").map { engine.create("TEST", it) }

        val result = runBlocking { withTimeout(5.seconds) { engine.resumeDue(concurrency = 2) } }

        assertEquals(2, result.sagas)
        assertEquals(listOf(SagaState.COMPLETED, SagaState.COMPLETED), ids.map { repository.find(it)!!.state })
    }

    @Test
    fun `run returns the state left by its last transaction, without reading the saga again`() {
        var finds = 0
        val counting = object : SagaRepository by repository {
            override fun find(id: Uuid): SagaRecord? = repository.find(id).also { finds++ }
        }
        val definition = SagaDefinition("TEST", listOf(FakeStep("stock", { "STK-1" }), FakeStep("booking", { "RES-1" })))
        val engine = SagaEngine(InMemorySagaTransactions(repository), counting, listOf(definition), clock = clock)
        val id = engine.create("TEST", "item-1")

        val saga = runBlocking { engine.run(id, clock.now + 10.seconds) }
        assertEquals(SagaState.COMPLETED, saga.state)
        assertEquals(repository.find(id), saga)                           // what run returned is what was saved
        assertEquals(0, finds)

        runBlocking { engine.run(id, clock.now) }                         // deadline already passed: must read
        assertEquals(1, finds)
    }

    @Test
    fun `a call that outlives its lock is cut, and its outcome is unknown`() {
        val stock = object : SagaStep {
            override val name = "stock"
            override suspend fun execute(ctx: SagaContext): String? { delay(10.seconds); return "STK-1" }
        }
        val engine = engine(stock, config = SagaConfig(callLock = 100.milliseconds))
        val saga = engine.start()                                         // returns after ~100 ms, not 10 s

        assertEquals(StepStatus.UNKNOWN, saga.stepStatus)
        assertTrue(saga.lastError!!.contains("TimeoutCancellationException"))
    }

    @Test
    fun `a two-step run takes three transactions (claim, record and claim, record)`() {
        val inMemory = InMemorySagaTransactions(repository)
        var transactions = 0
        val counting = object : SagaTransactions {
            override suspend fun <T> inTransaction(block: () -> T): T { transactions++; return inMemory.inTransaction(block) }
        }
        val definition = SagaDefinition("TEST", listOf(FakeStep("stock", { "STK-1" }), FakeStep("booking", { "RES-1" })))
        val engine = SagaEngine(counting, repository, listOf(definition), clock = clock)
        val id = engine.create("TEST", "item-1")

        val saga = runBlocking { engine.run(id, clock.now + 10.seconds) }
        assertEquals(SagaState.COMPLETED, saga.state)
        assertEquals(3, transactions)
    }

    @Test
    fun `purgeFinished deletes finished sagas older than the retention, with their history, and nothing else`() {
        val stock = object : SagaStep {
            override val name = "stock"
            override suspend fun execute(ctx: SagaContext): String = when (ctx.key) {
                "pending" -> throw StepNotSent("down")                     // stays RUNNING, retried later
                "stuck" -> throw RuntimeException("timeout")              // no check: will get STUCK
                "refused" -> throw StepRejected("no")                     // FAILED
                else -> "STK-${ctx.key}"
            }
        }
        val engine = engine(stock)
        val oldDone = engine.start("old-done")
        val oldFailed = engine.start("refused")
        val pending = engine.start("pending")
        val stuck = engine.start("stuck")
        engine.resumeAfter(2.minutes)                                     // check: cannot tell -> STUCK
        assertEquals(StepStatus.STUCK, engine.state(stuck).stepStatus)

        clock.advance(31.days)
        val recentDone = engine.start("recent-done")

        val deleted = runBlocking { engine.purgeFinished(olderThan = 30.days) }

        assertEquals(2, deleted)
        for (gone in listOf(oldDone, oldFailed)) {
            assertNull(repository.find(gone.id))
            assertTrue(repository.log.none { it.sagaId == gone.id })
            assertTrue(repository.storedResults(gone.id).isEmpty())
        }
        for (kept in listOf(pending, stuck, recentDone)) assertNotNull(repository.find(kept.id))
        assertTrue(repository.log.any { it.sagaId == recentDone.id })
    }

    @Test
    fun `purgeFinished works in batches until everything old is gone`() {
        val engine = engine(FakeStep("stock", { "STK-1" }))
        repeat(5) { engine.start("item-$it") }
        clock.advance(31.days)

        assertEquals(5, runBlocking { engine.purgeFinished(olderThan = 30.days, batchSize = 2) })
        assertEquals(0, runBlocking { engine.purgeFinished(olderThan = 30.days) })
    }

    /**
     * Runs [block] until [inCall] says the external call has started, then cancels it: a pod stopping
     * mid-call. Waiting matters: resumeDue runs sagas in child coroutines, which start later.
     */
    private fun crash(inCall: () -> Boolean, block: suspend () -> Unit) = runBlocking {
        withTimeout(5.seconds) {
            val call = launch { block() }
            do yield() while (!inCall())
            call.cancelAndJoin()
        }
    }

    @Test
    fun `only one unfinished saga per key`() {
        val engine = engine(FakeStep("stock", { throw StepNotSent("down") }))
        engine.start("item-1")
        assertFailsWith<DuplicateSagaException> { engine.start("item-1") }
    }

    @Test
    fun `a finished saga is never resumed`() {
        val engine = engine(FakeStep("stock", { "STK-1" }))
        engine.start()
        assertEquals(0, engine.resumeAfter(1.hours))
        assertNull(repository.findDue(clock.now, 10, listOf("TEST")).firstOrNull())
    }
}
