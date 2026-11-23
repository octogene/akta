package dev.octogene.akta

import dev.octogene.akta.memory.InMemorySagaRepository
import dev.octogene.akta.memory.InMemorySagaTransactions
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
import kotlin.time.Duration.Companion.nanoseconds
import kotlin.time.Duration.Companion.seconds
import kotlin.time.Instant
import kotlin.uuid.Uuid
import kotlinx.serialization.json.Json
import kotlin.reflect.typeOf

@Timeout(10)                // a step that never returns must fail the test, not hang the build
class SagaEngineTest {

    // ---- Fixtures ---------------------------------------------------------------------------

    private class MutableClock(var now: Instant = Instant.parse("2026-11-19T10:00:00Z")) : Clock {
        override fun now(): Instant = now
        fun advance(d: Duration) { now += d }
    }

    /** A step whose output is a reference string, logged as it is (like the DSL's step<String>). */
    private abstract class RefStep(override val name: String) : SagaStep<Unit, String?> {
        override val outputType = typeOf<String?>()
        override fun describe(output: String?) = output
    }

    /** A scripted step: each call pops the next behaviour; the last one repeats. */
    private class FakeStep(
        name: String,
        vararg behaviours: () -> String?,
        private val checkResult: (() -> CheckResult<String?>)? = null,
        private val compensation: (() -> Unit)? = null,
    ) : RefStep(name) {
        private val queue = ArrayDeque(behaviours.toList())
        var executions = 0
        var compensations = 0
        var checks = 0

        override suspend fun execute(ctx: SagaContext<Unit>): String? {
            executions++
            val next = if (queue.size > 1) queue.removeFirst() else queue.first()
            return next()
        }

        override suspend fun compensate(ctx: SagaContext<Unit>, own: String?) {
            compensations++
            compensation?.invoke()
        }

        override suspend fun check(ctx: SagaContext<Unit>): CheckResult<String?> {
            checks++
            return checkResult?.invoke() ?: CheckResult.CannotTell
        }
    }

    private val clock = MutableClock()
    private val repository = InMemorySagaRepository()
    private val completed = mutableListOf<String>()
    private val failed = mutableListOf<String>()

    /** Definitions by type, so that the tests can create sagas by type name. */
    private val registered = mutableMapOf<String, SagaDefinition<Unit>>()

    private fun definition(
        type: String,
        steps: List<SagaStep<Unit, *>>,
        onCompleted: SagaContext<Unit>.() -> Unit = {},
        onFailed: SagaContext<Unit>.() -> Unit = {},
    ) = SagaDefinition(type, inputType = null, steps, onCompleted, onFailed)

    private fun newEngine(
        transactions: SagaTransactions,
        repository: SagaRepository,
        definitions: List<SagaDefinition<Unit>>,
        config: SagaConfig = SagaConfig(),
        clock: Clock = this.clock,
        traceId: () -> String? = { null },
    ): SagaEngine {
        definitions.forEach { registered[it.type] = it }
        return SagaEngine(transactions, repository, definitions, KotlinxJsonSagaSerializer(), config, clock, traceId)
    }

    private fun SagaEngine.create(type: String, key: String) = create(registered.getValue(type), key)

    /** The stored outputs, decoded: the references the steps returned. */
    private fun results(id: Uuid): Map<String, String?> =
        repository.results(id).mapValues { (_, v) -> v?.let { Json.decodeFromString<String?>(it) } }

    /** A reference as an operator types it for markDone and abort: JSON. */
    private fun json(ref: String) = Json.encodeToString(ref)

    private fun engine(vararg steps: SagaStep<Unit, *>, config: SagaConfig = SagaConfig()) = newEngine(
        transactions = InMemorySagaTransactions(repository),
        repository = repository,
        definitions = listOf(
            definition(
                type = "TEST",
                steps = steps.toList(),
                onCompleted = { completed += key },
                onFailed = { failed += key },
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
        assertNull(saga.failureReason)
        assertEquals(mapOf("stock" to "STK-1", "booking" to "RES-1"), results(saga.id))
        assertEquals(listOf("item-1"), completed)
    }

    @Test
    fun `later steps see the references of earlier steps`() {
        var seen: String? = null
        val stock = FakeStep("stock", { "STK-1" })
        val booking = object : RefStep("booking") {
            override suspend fun execute(ctx: SagaContext<Unit>): String? { seen = ctx.result(StepRef<String?>("stock")); return "RES-1" }
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
        assertEquals("dev.octogene.akta.StepRejected: model not bookable", saga.failureReason)
        assertEquals(1, stock.compensations)
        assertEquals(0, booking.compensations)
        assertEquals(emptyMap<String, String?>(), results(saga.id))
        assertEquals(listOf("item-1"), failed)
    }

    @Test
    fun `a rejection of the first step fails without compensation`() {
        val stock = FakeStep("stock", { throw StepRejected("unknown EPC") })
        val saga = engine(stock).start()

        assertEquals(SagaState.FAILED, saga.state)
        assertEquals("dev.octogene.akta.StepRejected: unknown EPC", saga.failureReason)
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
        assertEquals("STK-1", results(saga.id)["stock"])
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

        val after = runBlocking { engine.markDone(saga.id, json("STK-9"), actor = "alice") }
        assertEquals(SagaState.COMPLETED, after.state)
        assertEquals("STK-9", results(saga.id)["stock"])
        assertEquals("alice", repository.log.single { it.event == SagaEvents.MANUAL_DONE }.actor)
    }

    @Test
    fun `a call interrupted by a crash is treated as unknown when its lock expires`() {
        var executions = 0
        val stock = object : RefStep("stock") {
            override suspend fun execute(ctx: SagaContext<Unit>): String? {
                if (++executions == 1) awaitCancellation()                // the pod stops mid-call
                return "STK-2"
            }
            override suspend fun check(ctx: SagaContext<Unit>) = CheckResult.NotDone
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
        val stock = object : RefStep("stock") {
            override suspend fun execute(ctx: SagaContext<Unit>): String? = withTimeout(1) { awaitCancellation() }
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
    fun `failed checks do not use up the executions of a step`() {
        var checks = 0
        val stock = FakeStep(
            "stock", { throw RuntimeException("timeout") }, { "STK-1" },
            checkResult = { if (++checks <= 2) throw RuntimeException("lookup down") else CheckResult.NotDone },
        )
        val engine = engine(stock, config = SagaConfig(maxAttempts = 3))
        val saga = engine.start()                                         // execution 1: outcome unknown
        engine.resumeAfter(2.minutes)                                     // check fails (1): again in 1 min
        engine.resumeAfter(1.minutes)                                     // check fails (2): again in 2 min
        engine.resumeAfter(2.minutes)                                     // not applied: execution 2

        assertEquals(SagaState.COMPLETED, engine.state(saga).state)       // before: STUCK, "Too many attempts"
        assertEquals(2, stock.executions)
    }

    @Test
    fun `a check that keeps failing gets stuck, and a human can resolve it`() {
        val stock = FakeStep(
            "stock", { throw RuntimeException("timeout") },
            checkResult = { throw RuntimeException("lookup down") },
        )
        val engine = engine(stock, config = SagaConfig(maxAttempts = 3))
        val saga = engine.start()
        engine.resumeAfter(2.minutes)                                     // check fails (1): again in 1 min
        engine.resumeAfter(1.minutes)                                     // check fails (2): again in 2 min
        engine.resumeAfter(2.minutes)                                     // check fails (3): STUCK

        val stuck = engine.state(saga)
        assertEquals(StepStatus.STUCK, stuck.stepStatus)                  // before: UNKNOWN forever, no way out
        assertTrue(stuck.lastError!!.startsWith("Check keeps failing"))
        assertEquals(3, stock.checks)
        assertEquals(1, stock.executions)

        val after = runBlocking { engine.markDone(saga.id, json("STK-7"), actor = "alice") }   // found by hand
        assertEquals(SagaState.COMPLETED, after.state)
    }

    @Test
    fun `a repeated error is logged every time`() {
        val down = { throw StepNotSent("connection refused") }
        val engine = engine(FakeStep("stock", down, down, down, { "STK-1" }))
        engine.start()
        engine.resumeAfter(1.minutes)
        engine.resumeAfter(2.minutes)

        val details = repository.log.filter { it.event == "EXECUTE:NotSent" }.map { it.detail }
        assertEquals(3, details.size)
        assertTrue(details.all { it!!.contains("connection refused") }, "$details")   // before: [error, null, null]
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
        assertEquals("java.lang.RuntimeException: stock unavailable", saga.lastError)     // the latest problem
        assertEquals("dev.octogene.akta.StepRejected: refused", saga.failureReason)      // why it compensates

        engine.resumeAfter(1.minutes)
        val failedSaga = engine.state(saga)
        assertEquals(SagaState.FAILED, failedSaga.state)
        assertEquals("dev.octogene.akta.StepRejected: refused", failedSaga.failureReason)  // not the retried failure
        assertEquals(2, stock.compensations)
    }

    @Test
    fun `too many failed attempts make the step stuck, and a manual retry resumes it`() {
        var calls = 0
        val stock = FakeStep("stock", { if (++calls <= 3) throw StepNotSent("down") else "STK-1" })
        val engine = newEngine(
            InMemorySagaTransactions(repository), repository,
            listOf(definition("TEST", listOf(stock))),
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
        assertEquals("Aborted by carol", after.failureReason)
        assertEquals(1, stock.compensations)
        assertEquals(0, booking.compensations)
    }

    @Test
    fun `abort of a stuck step that was applied compensates it too, with its reference`() {
        var compensatedRef: String? = null
        val stock = FakeStep("stock", { "STK-1" })
        val booking = object : RefStep("booking") {
            override suspend fun execute(ctx: SagaContext<Unit>): String? = throw RuntimeException("timeout")
            override suspend fun compensate(ctx: SagaContext<Unit>, own: String?) { compensatedRef = own }
        }
        val engine = engine(stock, booking)
        val saga = engine.start()
        engine.resumeAfter(2.minutes)                         // no check: cannot tell
        assertEquals(StepStatus.STUCK, engine.state(saga).stepStatus)

        val after = runBlocking { engine.abort(saga.id, "carol", currentStepApplied = true, output = json("RES-7")) }
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
        fun definition(type: String) = definition(type, listOf(stock))
        val newPod = newEngine(InMemorySagaTransactions(repository), repository,
            listOf(definition("NEW"), definition("TEST")), clock = clock)
        val brandNew = newPod.create("NEW", "item-1")
        val current = newPod.create("TEST", "item-2")

        val oldPod = newEngine(InMemorySagaTransactions(repository), repository, listOf(definition("TEST")), clock = clock)
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
        val definition = definition("TEST", listOf(FakeStep("stock", { "STK-1" })))
        val engine = newEngine(InMemorySagaTransactions(repository), failing, listOf(definition), clock = clock)
        poison = engine.create("TEST", "item-1")
        clock.advance(1.seconds)
        val healthy = engine.create("TEST", "item-2")

        // limit = 1: each batch only takes the oldest due saga, which is the failing one.
        val first = runBlocking { engine.resumeDue(limit = 1) }
        assertEquals(setOf(poison), first.failures.keys)
        val resumeFailed = repository.log.single { it.sagaId == poison && it.event == SagaEvents.RESUME_FAILED }
        assertTrue(resumeFailed.detail!!.contains("corrupted row"))
        assertNull(repository.find(poison)!!.lastError)                         // the step's own error is kept

        val second = runBlocking { engine.resumeDue(limit = 1) }                   // postponed: the other one runs
        assertEquals(emptyMap(), second.failures)
        assertEquals(SagaState.COMPLETED, repository.find(healthy)!!.state)

        clock.advance(1.minutes)                                                 // resumeFailureDelay
        assertEquals(setOf(poison), runBlocking { engine.resumeDue(limit = 1) }.failures.keys)
    }

    @Test
    fun `a throwing onCompleted makes the saga stuck, keeping the output, instead of looping`() {
        var hookFails = true
        val stock = FakeStep("stock", { "STK-1" })
        val engine = newEngine(
            InMemorySagaTransactions(repository), repository,
            listOf(definition("TEST", listOf(stock), onCompleted = {
                check(!hookFails) { "items table locked" }
                completed += key
            })),
            clock = clock,
        )
        val saga = runBlocking { engine.create("TEST", "item-1").let { engine.run(it, clock.now + 10.seconds) } }

        assertEquals(StepStatus.STUCK, saga.stepStatus)
        assertEquals(SagaState.RUNNING, saga.state)                       // rolled back, not COMPLETED
        assertTrue(saga.lastError!!.contains("ref=STK-1"))                // what describe() logs, an id
        assertEquals(mapOf("stock" to "STK-1"), results(saga.id))         // the output is kept with the result
        assertEquals(1, repository.log.count { it.event == SagaEvents.RECORD_FAILED })

        hookFails = false                                                 // the bug is fixed
        val after = runBlocking { engine.markDone(saga.id, output = null, actor = "alice") }   // continue with it
        assertEquals(SagaState.COMPLETED, after.state)
        assertEquals(listOf("item-1"), completed)
        assertEquals(1, stock.executions)
    }

    /**
     * Transactions after the first of which the clock jumps by [pause], as a long GC pause between a claim and
     * its call would, and [meanwhile] runs: what another pod does during the pause.
     */
    private inner class PausingTransactions(
        private val pause: Duration,
        private val meanwhile: suspend () -> Unit = {},
    ) : SagaTransactions {
        private val inMemory = InMemorySagaTransactions(repository)
        private var paused = false

        override suspend fun <T> inTransaction(block: () -> T): T {
            val result = inMemory.inTransaction(block)
            if (!paused) {
                paused = true
                clock.advance(pause)
                meanwhile()
            }
            return result
        }
    }

    @Test
    fun `a claim left with no time to call is undone, with no attempt used`() {
        val stock = FakeStep("stock", { "STK-1" })
        // 33 s: past the call's budget (callLock 35 s, minus 3.5 s kept to record), before the lock expires.
        val transactions = PausingTransactions(pause = 33.seconds)
        val engine = newEngine(transactions, repository, listOf(definition("TEST", listOf(stock))))
        val id = engine.create("TEST", "item-1")

        val after = runBlocking { engine.run(id, deadline = clock.now + 10.seconds) }   // claimed, then the pause

        assertEquals(0, stock.executions)                                 // never called
        assertEquals(StepStatus.READY, after.stepStatus)
        assertEquals(0, after.attempts)
        assertNull(after.lockedUntil)
        assertEquals(clock.now, after.nextAttemptAt)                      // due at once, no settle delay
        assertEquals(listOf(SagaEvents.CREATED, SagaEvents.NOT_CALLED), runBlocking { engine.history(id) }.map { it.event })

        engine.resumeAfter(1.seconds)
        assertEquals(SagaState.COMPLETED, engine.state(after).state)
        assertEquals(1, stock.executions)
    }

    @Test
    fun `a claim left with no time keeps a cancellation requested meanwhile`() {
        val stock = FakeStep("stock", { "STK-1" })
        lateinit var engine: SagaEngine
        lateinit var id: Uuid
        val transactions = PausingTransactions(pause = 33.seconds, meanwhile = { engine.cancel(id, "customer left", "support") })
        engine = newEngine(transactions, repository, listOf(definition("TEST", listOf(stock))))
        id = engine.create("TEST", "item-1")

        val after = runBlocking { engine.run(id, deadline = clock.now + 10.seconds) }

        assertEquals(0, stock.executions)
        assertEquals(SagaState.FAILED, after.state)                       // released, then the cancellation applies
        assertEquals("Cancelled: customer left", after.failureReason)
    }

    @Test
    fun `a manual action with runFor zero commits and leaves the saga to resumeDue`() {
        var down = true
        val stock = FakeStep("stock", { if (down) throw StepNotSent("down") else "STK-1" })
        val engine = engine(stock, config = SagaConfig(maxAttempts = 1))
        val saga = engine.start()
        assertEquals(StepStatus.STUCK, saga.stepStatus)
        down = false

        val after = runBlocking { engine.retry(saga.id, actor = "alice", runFor = Duration.ZERO) }

        assertEquals(StepStatus.READY, after.stepStatus)                  // committed, not run
        assertEquals(1, stock.executions)
        engine.resumeAfter(1.seconds)
        assertEquals(SagaState.COMPLETED, engine.state(saga).state)
    }

    @Test
    fun `a manual action does not run a saga that cannot move on`() {
        val stock = FakeStep("stock", { throw java.io.IOException("read timeout") })   // unknown outcome
        val transactions = object : SagaTransactions {
            val inMemory = InMemorySagaTransactions(repository)
            var count = 0
            override suspend fun <T> inTransaction(block: () -> T): T { count++; return inMemory.inTransaction(block) }
        }
        val engine = newEngine(transactions, repository, listOf(definition("TEST", listOf(stock))))
        val saga = engine.start()
        assertEquals(StepStatus.UNKNOWN, saga.stepStatus)                 // to check after settleDelay

        transactions.count = 0
        val after = runBlocking { engine.cancel(saga.id, "customer left", "support") }

        assertEquals("customer left", after.cancelReason)
        assertEquals(1, transactions.count)                               // the cancellation only
    }

    @Test
    fun `a failure to run the saga on does not fail the action, which is committed`() {
        val stock = FakeStep("stock", { "STK-1" })
        val booking = FakeStep("booking", { throw StepNotSent("down") })
        val engine = engine(stock, booking, config = SagaConfig(maxAttempts = 3))
        val saga = engine.start()                                         // booking: retried later
        // A pod that does not know this saga type (a rolling deploy): its run after the action throws.
        val other = newEngine(
            InMemorySagaTransactions(repository), repository, listOf(definition("OTHER", listOf(FakeStep("x", { "X" })))),
        )

        val after = runBlocking { other.cancel(saga.id, "customer left", "support") }

        assertEquals(SagaState.COMPENSATING, after.state)                 // the cancellation is committed
        assertEquals("Cancelled: customer left", after.failureReason)
        val failure = repository.log.last()
        assertEquals(SagaEvents.RESUME_FAILED, failure.event)
        assertTrue(failure.detail!!.contains("Unknown saga type: TEST"), failure.detail)
        assertTrue(after.nextAttemptAt >= clock.now + SagaConfig().resumeFailureDelay)   // postponed

        engine.resumeAfter(2.minutes)                                     // a pod that knows the type
        assertEquals(SagaState.FAILED, engine.state(saga).state)
        assertEquals(1, stock.compensations)
    }

    @Test
    fun `durations that make no sense are refused`() {
        assertFailsWith<IllegalArgumentException> { SagaConfig(callLock = Duration.ZERO) }
        assertFailsWith<IllegalArgumentException> { SagaConfig(callLock = 500.milliseconds) }
        assertFailsWith<IllegalArgumentException> { SagaConfig(settleDelay = (-1).seconds) }
        assertFailsWith<IllegalArgumentException> { SagaConfig(checkRetryDelay = (-1).seconds) }
        assertFailsWith<IllegalArgumentException> { SagaConfig(maxBackoff = Duration.ZERO) }
        assertFailsWith<IllegalArgumentException> { SagaConfig(resumeFailureDelay = (-1).seconds) }
        SagaConfig(callLock = 1.seconds, settleDelay = Duration.ZERO)      // the smallest accepted
    }

    @Test
    fun `a compensation that keeps outliving its lock gets stuck`() {
        val stock = object : RefStep("stock") {
            var compensations = 0
            override suspend fun execute(ctx: SagaContext<Unit>) = "STK-1"
            override suspend fun compensate(ctx: SagaContext<Unit>, own: String?) {
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
        assertTrue(stuck.lastError!!.startsWith("Too many attempts"), stuck.lastError)
        assertEquals("dev.octogene.akta.StepRejected: refused", stuck.failureReason)

        // An operator undoes it by hand: the saga ends FAILED with the refusal, not the lock expiries.
        val failedSaga = runBlocking { engine.markCompensated(id, actor = "alice") }
        assertEquals(SagaState.FAILED, failedSaga.state)
        assertEquals("dev.octogene.akta.StepRejected: refused", failedSaga.failureReason)
    }

    @Test
    fun `the trace id is read on the caller's thread, not on the transaction thread`() {
        val currentTrace = ThreadLocal<String?>()
        val inMemory = InMemorySagaTransactions(repository)
        val onAnotherThread = object : SagaTransactions {
            override suspend fun <T> inTransaction(block: () -> T): T =
                withContext(Dispatchers.IO) { inMemory.inTransaction(block) }
        }
        val engine = newEngine(
            onAnotherThread, repository, listOf(definition("TEST", listOf(FakeStep("stock", { "STK-1" })))),
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
        val stock = object : RefStep("stock") {
            override suspend fun execute(ctx: SagaContext<Unit>): String {
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
        val definition = definition("TEST", listOf(FakeStep("stock", { "STK-1" }), FakeStep("booking", { "RES-1" })))
        val engine = newEngine(InMemorySagaTransactions(repository), counting, listOf(definition), clock = clock)
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
        val stock = object : RefStep("stock") {
            override suspend fun execute(ctx: SagaContext<Unit>): String? { delay(10.seconds); return "STK-1" }
        }
        val engine = engine(stock, config = SagaConfig(callLock = 2.seconds))
        val started = System.nanoTime()
        val saga = engine.start()
        val elapsed = (System.nanoTime() - started).nanoseconds

        assertEquals(StepStatus.UNKNOWN, saga.stepStatus)
        assertTrue(saga.lastError!!.contains("TimeoutCancellationException"))
        // Cut before the lock expires (callLock minus a margin to record it), not at the expiry itself.
        assertTrue(elapsed < 2.seconds, "the call ran $elapsed, as long as its lock")
    }

    @Test
    fun `an Error thrown by a step stays with its saga and does not stop the batch`() {
        val stock = object : RefStep("stock") {
            override suspend fun execute(ctx: SagaContext<Unit>): String =
                if (ctx.key == "stub") TODO("stock client not written yet") else { delay(50); "STK-1" }
        }
        val engine = engine(stock)
        val stub = engine.create("TEST", "stub")
        val healthy = engine.create("TEST", "healthy")

        val result = runBlocking { engine.resumeDue(concurrency = 2) }    // before: NotImplementedError escaped

        assertEquals(emptyMap(), result.failures)
        assertEquals(SagaState.COMPLETED, repository.find(healthy)!!.state)
        val unknown = repository.find(stub)!!
        assertEquals(StepStatus.UNKNOWN, unknown.stepStatus)              // checked later, like any failed call
        assertTrue(unknown.lastError!!.contains("NotImplementedError"))
    }

    @Test
    fun `a failure to claim the next step does not lose the outcome just recorded`() {
        var resultsCalls = 0
        val failingOnce = object : SagaRepository by repository {
            // Call 1: claim of step 1. Call 2: claim of step 2, in the transaction recording step 1.
            override fun results(sagaId: Uuid): Map<String, String?> =
                if (++resultsCalls == 2) throw IllegalStateException("statement timeout") else repository.results(sagaId)
        }
        val definition = definition("TEST", listOf(FakeStep("stock", { "STK-1" }), FakeStep("booking", { "RES-1" })))
        val engine = newEngine(InMemorySagaTransactions(repository), failingOnce, listOf(definition), clock = clock)
        val id = engine.create("TEST", "item-1")

        val saga = runBlocking { engine.run(id, clock.now + 10.seconds) }

        assertEquals(StepStatus.READY, saga.stepStatus)                   // before: STUCK on step 1, RECORD_FAILED
        assertEquals(1, saga.stepIndex)
        assertEquals(mapOf("stock" to "STK-1"), results(id))
        assertTrue(repository.log.none { it.event == SagaEvents.RECORD_FAILED })
        assertEquals(SagaState.COMPLETED, runBlocking { engine.run(id, clock.now + 10.seconds) }.state)
    }

    @Test
    fun `a two-step run takes three transactions (claim, record and claim, record)`() {
        val inMemory = InMemorySagaTransactions(repository)
        var transactions = 0
        val counting = object : SagaTransactions {
            override suspend fun <T> inTransaction(block: () -> T): T { transactions++; return inMemory.inTransaction(block) }
        }
        val definition = definition("TEST", listOf(FakeStep("stock", { "STK-1" }), FakeStep("booking", { "RES-1" })))
        val engine = newEngine(counting, repository, listOf(definition), clock = clock)
        val id = engine.create("TEST", "item-1")

        val saga = runBlocking { engine.run(id, clock.now + 10.seconds) }
        assertEquals(SagaState.COMPLETED, saga.state)
        assertEquals(3, transactions)
    }

    @Test
    fun `purgeFinished deletes finished sagas older than the retention, with their history, and nothing else`() {
        val stock = object : RefStep("stock") {
            override suspend fun execute(ctx: SagaContext<Unit>): String = when (ctx.key) {
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
