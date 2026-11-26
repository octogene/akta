package dev.octogene.akta

import dev.octogene.akta.memory.InMemorySagaRepository
import dev.octogene.akta.memory.InMemorySagaTransactions
import kotlinx.coroutines.runBlocking
import kotlinx.serialization.Serializable
import kotlinx.serialization.json.Json
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Timeout
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

/** Signal steps: waiting for an event delivered from outside, its timeout, and cancellation. */
@Timeout(10)
class SagaSignalTest {

    @Serializable
    data class PaymentResult(val paymentId: String, val succeeded: Boolean, val customer: String = "PII-customer")

    private class MutableClock(var now: Instant = Instant.parse("2026-11-21T10:00:00Z")) : Clock {
        override fun now(): Instant = now
        fun advance(d: Duration) { now += d }
    }

    private val clock = MutableClock()
    private val repository = InMemorySagaRepository()
    private val calls = mutableListOf<String>()
    private val released = mutableListOf<String?>()
    private val completed = mutableListOf<String>()
    private val failed = mutableListOf<String>()

    /** A payment, a wait for the payment event, then a confirmation that reads the signal's value. */
    private fun reservation(
        checkAtTimeout: (suspend SagaContext<Unit>.() -> CheckResult<PaymentResult>)? = null,
        pay: () -> String = { "P-1" },
    ) = saga("RESERVATION") {
        step<String>("create-payment") {
            execute { calls += "create-payment"; pay() }
            compensate { own -> released += own }
        }
        val paid = awaitSignal<PaymentResult>("await-payment", signal = "payment", timeout = 30.minutes) {
            onSignal { if (!it.succeeded) throw StepRejected("payment ${it.paymentId} failed") }
            checkAtTimeout?.let { check(it) }
            logAs { it.paymentId }
        }
        step<String>("confirm") {
            execute { calls += "confirm ${requireResult(paid).paymentId}"; "C-1" }
        }
        onCompleted { completed += key }
        onFailed { failed += key }
    }

    private fun engine(definition: SagaDefinition<Unit>, config: SagaConfig = SagaConfig()) =
        SagaEngine(InMemorySagaTransactions(repository), repository, listOf(definition), KotlinxJsonSagaSerializer(), config, clock)

    private fun SagaEngine.run(id: Uuid) = runBlocking { run(id, clock.now + 10.seconds) }
    private fun SagaEngine.pay(id: Uuid, succeeded: Boolean = true, paymentId: String = "P-1") =
        runBlocking { signal(id, "payment", PaymentResult(paymentId, succeeded)) }
    private fun SagaEngine.resumeAfter(d: Duration) {
        clock.advance(d)
        assertEquals(emptyMap(), runBlocking { resumeDue() }.failures)
    }
    private fun details(event: String) = repository.log.filter { it.event == event }.map { it.detail }

    // ---- Delivering ---------------------------------------------------------------------------

    @Test
    fun `a saga waits for its signal, then moves on with the signal's value`() {
        val definition = reservation()
        val engine = engine(definition)
        val id = engine.create(definition, "R-1")

        val waiting = engine.run(id)
        assertEquals(StepStatus.WAITING, waiting.stepStatus)                    // the HTTP handler answers 202
        assertEquals(clock.now + 30.minutes, waiting.nextAttemptAt)

        assertEquals(SignalResult.Delivered(id), engine.pay(id))
        val done = engine.run(id)

        assertEquals(SagaState.COMPLETED, done.state)
        assertEquals(listOf("create-payment", "confirm P-1"), calls)
        assertEquals(listOf("R-1"), completed)
    }

    @Test
    fun `a signal that arrives before the step waits is stored, then used at once`() {
        val definition = reservation()
        val engine = engine(definition)
        val id = engine.create(definition, "R-1")

        assertEquals(SignalResult.Stored(id), engine.pay(id))                   // faster than the HTTP request
        val done = engine.run(id)

        assertEquals(SagaState.COMPLETED, done.state)                           // no wait at all
        assertEquals("consumed (stored earlier) ref=P-1", details(SagaEvents.SIGNAL_RECEIVED).last())
    }

    @Test
    fun `only the first signal of a name counts`() {
        val definition = reservation()
        val engine = engine(definition)
        val id = engine.create(definition, "R-1")
        engine.run(id)

        assertEquals(SignalResult.Delivered(id), engine.pay(id, paymentId = "P-1"))
        assertEquals(SignalResult.Ignored(id), engine.pay(id, paymentId = "P-2"))   // a redelivered event
        engine.run(id)

        assertEquals(listOf("create-payment", "confirm P-1"), calls)
    }

    @Test
    fun `a rejected signal compensates what was done`() {
        val definition = reservation()
        val engine = engine(definition)
        val id = engine.create(definition, "R-1")
        engine.run(id)

        assertEquals(SignalResult.Delivered(id), engine.pay(id, succeeded = false))
        val failedSaga = engine.run(id)

        assertEquals(SagaState.FAILED, failedSaga.state)
        assertEquals("dev.octogene.akta.StepRejected: payment P-1 failed", failedSaga.failureReason)
        assertEquals(listOf<String?>("P-1"), released)                          // the payment step was undone
        assertEquals(listOf("R-1"), failed)
    }

    @Test
    fun `compensation passes over a signal step, which made no call`() {
        val definition = saga("RESERVATION") {
            step<String>("create-payment") {
                execute { "P-1" }
                compensate { own -> released += own }
            }
            awaitSignal<PaymentResult>("await-payment", signal = "payment", timeout = 30.minutes)
            step("confirm") { execute { throw StepRejected("reservation expired") } }
        }
        val engine = engine(definition)
        val id = engine.create(definition, "R-1")
        engine.run(id)

        engine.pay(id)
        val done = engine.run(id)

        assertEquals(SagaState.FAILED, done.state)
        assertEquals(listOf<String?>("P-1"), released)
        assertEquals(listOf("await-payment"), repository.log.filter { it.event == SagaEvents.NOTHING_TO_UNDO }.map { it.stepName })
        assertTrue(repository.log.none { it.event.startsWith("COMPENSATE") && it.stepName == "await-payment" })
    }

    @Test
    fun `a signal for a name no step waits for, or of another type, is refused`() {
        val definition = reservation()
        val engine = engine(definition)
        val id = engine.create(definition, "R-1")

        assertFailsWith<IllegalArgumentException> { runBlocking { engine.signal(id, "shipment", PaymentResult("P-1", true)) } }
        assertFailsWith<IllegalArgumentException> { runBlocking { engine.signal(id, "payment", "P-1") } }
        assertEquals(SignalResult.NoSaga, runBlocking { engine.signal(Uuid.random(), "payment", PaymentResult("P-1", true)) })
        assertTrue(repository.log.none { it.event == SagaEvents.SIGNAL_RECEIVED })
    }

    @Test
    fun `two steps cannot wait for the same signal name`() {
        assertFailsWith<IllegalArgumentException> {
            saga("T") {
                awaitSignal<String>("first", signal = "payment", timeout = 1.minutes)
                awaitSignal<String>("second", signal = "payment", timeout = 1.minutes)
            }
        }
    }

    // ---- Timeouts -----------------------------------------------------------------------------

    @Test
    fun `without a check, a timeout makes the saga stuck without reading its data`() {
        val json = KotlinxJsonSagaSerializer()
        var decodes = 0
        val counting = object : SagaSerializer by json {
            override fun decode(text: String, type: kotlin.reflect.KType): Any? { decodes++; return json.decode(text, type) }
        }
        val definition = reservation()
        val engine = SagaEngine(InMemorySagaTransactions(repository), repository, listOf(definition), counting, SagaConfig(), clock)
        val id = engine.create(definition, "R-1")
        engine.run(id)

        decodes = 0
        engine.resumeAfter(31.minutes)

        assertEquals(StepStatus.STUCK, runBlocking { engine.find(id)!! }.stepStatus)
        assertEquals(0, decodes)                                                // nothing to call: nothing decoded
    }

    @Test
    fun `without a check, a timeout makes the saga stuck, and a retry waits again`() {
        val definition = reservation()
        val engine = engine(definition)
        val id = engine.create(definition, "R-1")
        engine.run(id)

        engine.resumeAfter(31.minutes)
        val stuck = runBlocking { engine.find(id)!! }
        assertEquals(StepStatus.STUCK, stuck.stepStatus)
        assertEquals("No signal 'payment' before the timeout", stuck.lastError)

        assertEquals(SignalResult.Stored(id), engine.pay(id))                   // the event finally arrives
        val done = runBlocking { engine.retry(id, "operator") }                 // waits again: consumes it
        assertEquals(SagaState.COMPLETED, done.state)
    }

    @Test
    fun `at the timeout the check finds the payment, and its value is used`() {
        val definition = reservation(checkAtTimeout = { CheckResult.Done(PaymentResult("P-9", succeeded = true)) })
        val engine = engine(definition)
        val id = engine.create(definition, "R-1")
        engine.run(id)

        engine.resumeAfter(31.minutes)

        assertEquals(SagaState.COMPLETED, runBlocking { engine.find(id)!! }.state)
        assertEquals("confirm P-9", calls.last())
        assertEquals(SignalResult.Ignored(id), engine.pay(id))                  // the event, too late
        assertEquals("ignored: too late ref=P-1", details(SagaEvents.SIGNAL_RECEIVED).last())
        assertTrue(repository.signal(id, "payment")!!.contains("P-9"))          // the value found, kept; not the late one
    }

    @Test
    fun `at the timeout the check finds nothing, and the saga compensates`() {
        val definition = reservation(checkAtTimeout = { CheckResult.NotDone })
        val engine = engine(definition)
        val id = engine.create(definition, "R-1")
        engine.run(id)

        engine.resumeAfter(31.minutes)

        val failedSaga = runBlocking { engine.find(id)!! }
        assertEquals(SagaState.FAILED, failedSaga.state)
        assertTrue(failedSaga.failureReason!!.startsWith("No signal 'payment' before the timeout"), failedSaga.failureReason)
        assertEquals(listOf<String?>("P-1"), released)
    }

    @Test
    fun `a check that finds a failed payment rejects it, like the signal would`() {
        val definition = reservation(checkAtTimeout = { CheckResult.Done(PaymentResult("P-1", succeeded = false)) })
        val engine = engine(definition)
        val id = engine.create(definition, "R-1")
        engine.run(id)

        engine.resumeAfter(31.minutes)

        assertEquals(SagaState.FAILED, runBlocking { engine.find(id)!! }.state)
    }

    @Test
    fun `a signal during a timeout check that finds a value is stored, and the signal received wins`() {
        lateinit var engine: SagaEngine
        lateinit var id: Uuid
        val definition = reservation(checkAtTimeout = {
            assertEquals(SignalResult.Stored(id), engine.pay(id, paymentId = "P-late"))   // arrives mid-check
            CheckResult.Done(PaymentResult("P-1", succeeded = true))
        })
        engine = engine(definition)
        id = engine.create(definition, "R-1")
        engine.run(id)

        engine.resumeAfter(31.minutes)

        assertEquals(SagaState.COMPLETED, runBlocking { engine.find(id)!! }.state)
        // The same event, twice: the signal, received first, is kept; the check's value is not stored over it.
        assertEquals("confirm P-late", calls.last())
    }

    @Test
    fun `a signal during a timeout check that finds nothing is used, since the lookup raced with the event`() {
        lateinit var engine: SagaEngine
        lateinit var id: Uuid
        val definition = reservation(checkAtTimeout = {
            assertEquals(SignalResult.Stored(id), engine.pay(id, paymentId = "P-2"))   // arrives mid-check
            CheckResult.NotDone
        })
        engine = engine(definition)
        id = engine.create(definition, "R-1")
        engine.run(id)

        engine.resumeAfter(31.minutes)

        val saga = runBlocking { engine.find(id)!! }
        assertEquals(SagaState.COMPLETED, saga.state)
        assertEquals("confirm P-2", calls.last())                               // the signal's value
        assertTrue(released.isEmpty())                                          // nothing undone
        assertEquals("consumed (stored earlier) ref=P-2", details(SagaEvents.SIGNAL_RECEIVED).last())
    }

    @Test
    fun `a signal during a timeout check that cannot tell is used, instead of stopping`() {
        lateinit var engine: SagaEngine
        lateinit var id: Uuid
        val definition = reservation(checkAtTimeout = {
            engine.pay(id, paymentId = "P-2")
            CheckResult.CannotTell
        })
        engine = engine(definition)
        id = engine.create(definition, "R-1")
        engine.run(id)

        engine.resumeAfter(31.minutes)

        assertEquals(SagaState.COMPLETED, runBlocking { engine.find(id)!! }.state)
        assertEquals("confirm P-2", calls.last())
    }

    @Test
    fun `a signal used instead of the check still goes through onSignal`() {
        lateinit var engine: SagaEngine
        lateinit var id: Uuid
        val definition = reservation(checkAtTimeout = {
            engine.pay(id, succeeded = false, paymentId = "P-2")                 // a failed payment
            CheckResult.NotDone
        })
        engine = engine(definition)
        id = engine.create(definition, "R-1")
        engine.run(id)

        engine.resumeAfter(31.minutes)

        val saga = runBlocking { engine.find(id)!! }
        assertEquals(SagaState.FAILED, saga.state)
        assertEquals("dev.octogene.akta.StepRejected: payment P-2 failed", saga.failureReason)
        assertEquals(listOf<String?>("P-1"), released)
    }

    @Test
    fun `a signal between two timeout checks is delivered at once, and no check runs again`() {
        var checks = 0
        val definition = reservation(checkAtTimeout = { checks++; throw java.io.IOException("payments down") })
        val engine = engine(definition)
        val id = engine.create(definition, "R-1")
        engine.run(id)
        engine.resumeAfter(31.minutes)                                          // the check fails: CHECKING again
        assertEquals(StepStatus.CHECKING, runBlocking { engine.find(id)!! }.stepStatus)

        assertEquals(SignalResult.Delivered(id), engine.pay(id))
        val done = engine.run(id)

        assertEquals(SagaState.COMPLETED, done.state)
        assertEquals("confirm P-1", calls.last())
        assertEquals(1, checks)
        assertEquals(0, done.checkFailures)                                     // not carried to the next steps
    }

    @Test
    fun `a signal during a timeout check that fails is used, instead of checking again`() {
        lateinit var engine: SagaEngine
        lateinit var id: Uuid
        var checks = 0
        val definition = reservation(checkAtTimeout = {
            checks++
            assertEquals(SignalResult.Stored(id), engine.pay(id, paymentId = "P-2"))   // arrives mid-check
            throw java.io.IOException("payments down")
        })
        engine = engine(definition)
        id = engine.create(definition, "R-1")
        engine.run(id)
        engine.resumeAfter(31.minutes)                                          // the check fails: the signal is used

        assertEquals(SagaState.COMPLETED, runBlocking { engine.find(id)!! }.state)
        assertEquals("confirm P-2", calls.last())                               // the signal's value
        assertEquals(1, checks)
        assertEquals("consumed (stored earlier) ref=P-2", details(SagaEvents.SIGNAL_RECEIVED).last())
    }

    @Test
    fun `a signal during the last check, which fails, is used instead of stopping the saga`() {
        lateinit var engine: SagaEngine
        lateinit var id: Uuid
        val definition = reservation(checkAtTimeout = {
            assertEquals(SignalResult.Stored(id), engine.pay(id, paymentId = "P-2"))   // arrives mid-check
            throw java.io.IOException("payments down")
        })
        engine = engine(definition, SagaConfig(maxAttempts = 1))                // this check is the last one allowed
        id = engine.create(definition, "R-1")
        engine.run(id)

        engine.resumeAfter(31.minutes)

        val saga = runBlocking { engine.find(id)!! }
        assertEquals(SagaState.COMPLETED, saga.state)                           // not STUCK (CHECK_FAILING)
        assertEquals("confirm P-2", calls.last())
    }

    @Test
    fun `a check that cannot tell makes the saga stuck`() {
        val definition = reservation(checkAtTimeout = { CheckResult.CannotTell })
        val engine = engine(definition)
        val id = engine.create(definition, "R-1")
        engine.run(id)

        engine.resumeAfter(31.minutes)

        assertEquals(StepStatus.STUCK, runBlocking { engine.find(id)!! }.stepStatus)
    }

    // ---- onSignal: inside a transaction, and a bug in it ------------------------------------------

    private var onSignalBug = false
    private val inTransaction = ThreadLocal.withInitial { false }
    private val onSignalInTransaction = mutableListOf<Boolean>()

    /** The reservation, whose onSignal notes whether it runs in a transaction, and throws while [onSignalBug]. */
    private fun watchedReservation(checkAtTimeout: (suspend SagaContext<Unit>.() -> CheckResult<PaymentResult>)? = null) =
        saga("RESERVATION") {
            step<String>("create-payment") {
                execute { "P-1" }
                compensate { own -> released += own }
            }
            val paid = awaitSignal<PaymentResult>("await-payment", signal = "payment", timeout = 30.minutes) {
                onSignal {
                    onSignalInTransaction += inTransaction.get()
                    if (onSignalBug) error("mapping bug")
                    if (!it.succeeded) throw StepRejected("payment ${it.paymentId} failed")
                }
                checkAtTimeout?.let { check(it) }
                logAs { it.paymentId }
            }
            step<String>("confirm") {
                execute { calls += "confirm ${requireResult(paid).paymentId}"; "C-1" }
            }
        }

    /** An engine whose transactions say they are open, for onSignal to see. */
    private fun watchedEngine(definition: SagaDefinition<Unit>): SagaEngine {
        val inner = InMemorySagaTransactions(repository)
        val flagging = object : SagaTransactions {
            override suspend fun <T> inTransaction(block: () -> T): T = inner.inTransaction {
                inTransaction.set(true)
                try { block() } finally { inTransaction.set(false) }
            }
        }
        return SagaEngine(flagging, repository, listOf(definition), KotlinxJsonSagaSerializer(), SagaConfig(), clock)
    }

    @Test
    fun `onSignal runs inside a transaction, for a signal and for a value its check found`() {
        val definition = watchedReservation(checkAtTimeout = { CheckResult.Done(PaymentResult("P-1", succeeded = true)) })
        val engine = watchedEngine(definition)
        val delivered = engine.create(definition, "R-1")
        engine.run(delivered)
        engine.pay(delivered)
        val checked = engine.create(definition, "R-2")
        engine.run(checked)

        engine.resumeAfter(31.minutes)                                          // R-2: the check finds it

        assertEquals(SagaState.COMPLETED, runBlocking { engine.find(checked)!! }.state)
        assertEquals(listOf(true, true), onSignalInTransaction)
    }

    @Test
    fun `a bug in onSignal never fails the listener, and stops the saga with the signal kept`() {
        val definition = watchedReservation()
        val engine = watchedEngine(definition)
        val id = engine.create(definition, "R-1")
        engine.run(id)
        onSignalBug = true

        assertEquals(SignalResult.Delivered(id), engine.pay(id))                // onSignal does not run here
        val stuck = engine.run(id)                                               // it does when the saga runs

        assertEquals(StuckReason.RECORD_FAILED, stuck.stuckReason)
        assertTrue(stuck.lastError!!.startsWith("onSignal threw on signal 'payment'"), stuck.lastError)
        assertEquals("delivered ref=P-1", details(SagaEvents.SIGNAL_RECEIVED).single())

        onSignalBug = false                                                     // fixed
        val done = runBlocking { engine.retry(id, "operator") }                 // uses the signal kept
        assertEquals(SagaState.COMPLETED, done.state)
        assertEquals("confirm P-1", calls.last())
    }

    @Test
    fun `a bug in onSignal on a signal stored earlier stops the saga, instead of failing every resume`() {
        val definition = watchedReservation()
        val engine = watchedEngine(definition)
        val id = engine.create(definition, "R-1")
        assertEquals(SignalResult.Stored(id), engine.pay(id))                   // faster than the request
        onSignalBug = true

        engine.run(id)
        engine.resumeAfter(1.seconds)                                           // no failure reported

        val stuck = runBlocking { engine.find(id)!! }
        assertEquals(StuckReason.RECORD_FAILED, stuck.stuckReason)
        assertTrue(stuck.lastError!!.contains("The signal is kept"), stuck.lastError)
        assertTrue(repository.log.none { it.event == SagaEvents.RESUME_FAILED })

        onSignalBug = false
        assertEquals(SagaState.COMPLETED, runBlocking { engine.retry(id, "operator") }.state)
    }

    @Test
    fun `a bug in onSignal after its timeout check died stops the saga, instead of being claimed for ever`() {
        lateinit var engine: SagaEngine
        var dies = true
        val definition = watchedReservation(checkAtTimeout = {
            if (dies) {
                engine.pay(sagaId)                                              // the event, during the lookup
                throw OutOfMemoryError("the pod dies during the check")
            }
            CheckResult.NotDone
        })
        engine = watchedEngine(definition)
        val id = engine.create(definition, "R-1")
        engine.run(id)
        clock.advance(31.minutes)
        assertFailsWith<OutOfMemoryError> { engine.run(id) }                    // left IN_PROGRESS, the signal stored
        dies = false
        onSignalBug = true

        engine.resumeAfter(10.minutes)                                          // long after the lock and the settle delay

        val stuck = runBlocking { engine.find(id)!! }
        assertEquals(StuckReason.RECORD_FAILED, stuck.stuckReason)
        assertNull(stuck.lockedUntil)
        assertTrue(stuck.lastError!!.contains("The signal is kept"), stuck.lastError)

        onSignalBug = false
        assertEquals(SagaState.COMPLETED, runBlocking { engine.retry(id, "operator") }.state)
    }

    // ---- A waiting saga that cannot be run -------------------------------------------------------

    private var broken = false
    private val unreadable = object : SagaRepository by repository {
        override fun results(sagaId: Uuid): Map<String, String?> =
            if (broken) throw IllegalStateException("corrupted row") else repository.results(sagaId)
    }

    /** A reservation stopped by resume failures at its timeout, its check never run; then readable again. */
    private fun stoppedAtTimeout(check: (suspend SagaContext<Unit>.() -> CheckResult<PaymentResult>)): Pair<SagaEngine, Uuid> {
        val definition = reservation(check)
        val engine = SagaEngine(
            InMemorySagaTransactions(repository), unreadable, listOf(definition), KotlinxJsonSagaSerializer(), SagaConfig(maxAttempts = 2), clock,
        )
        val id = engine.create(definition, "R-1")
        engine.run(id)
        broken = true
        clock.advance(31.minutes)
        repeat(2) { runBlocking { engine.resumeDue() }; clock.advance(1.minutes) }
        broken = false
        val stuck = repository.find(id)!!
        assertEquals(StuckReason.RESUME_FAILING, stuck.stuckReason)
        assertEquals(StepStatus.WAITING, stuck.stuckFrom)
        return engine to id
    }

    @Test
    fun `retry after resume failures at a signal's timeout checks at once, instead of waiting a whole timeout again`() {
        val (engine, id) = stoppedAtTimeout { CheckResult.Done(PaymentResult("P-1", succeeded = true)) }

        val done = runBlocking { engine.retry(id, "operator") }

        assertEquals(SagaState.COMPLETED, done.state)
        assertEquals("confirm P-1", calls.last())
    }

    @Test
    fun `retry after resume failures at a signal's timeout uses a signal received while stuck`() {
        var checks = 0
        val (engine, id) = stoppedAtTimeout { checks++; CheckResult.CannotTell }
        assertEquals(SignalResult.Stored(id), engine.pay(id))

        assertEquals(SagaState.COMPLETED, runBlocking { engine.retry(id, "operator") }.state)
        assertEquals(0, checks)
    }

    @Test
    fun `retry after resume failures at a signal's timeout applies a cancellation requested while stuck`() {
        var checks = 0
        val (engine, id) = stoppedAtTimeout { checks++; CheckResult.CannotTell }
        runBlocking { engine.cancel(id, "changed my mind", "customer") }

        assertEquals(SagaState.FAILED, runBlocking { engine.retry(id, "operator") }.state)
        assertEquals(0, checks)
        assertEquals(listOf<String?>("P-1"), released)
    }

    // ---- A hook that throws when a signal ends the saga -------------------------------------------

    private var hookBug = true

    /** A saga that a signal ends: completed when accepted, failed (nothing to undo) when rejected. */
    private fun endedBySignal() = saga("APPROVAL") {
        awaitSignal<PaymentResult>("await-payment", signal = "payment", timeout = 30.minutes) {
            onSignal { if (!it.succeeded) throw StepRejected("payment ${it.paymentId} failed") }
            logAs { it.paymentId }
        }
        onCompleted { if (hookBug) error("hook bug"); completed += key }
        onFailed { if (hookBug) error("hook bug"); failed += key }
    }

    @Test
    fun `a hook that throws after a delivered signal never fails the listener, and runs again once fixed`() {
        val definition = endedBySignal()
        val engine = engine(definition)
        val id = engine.create(definition, "R-1")
        engine.run(id)

        assertEquals(SignalResult.Delivered(id), engine.pay(id))                // the hook does not run here
        assertEquals(SignalResult.Ignored(id), engine.pay(id))                  // a redelivery changes nothing
        assertEquals(SagaState.RUNNING, runBlocking { engine.find(id)!! }.state)  // woken: consumed when it runs

        val tried = engine.run(id)                                               // the hook, in a transaction of its own
        assertEquals(SagaState.FINISHING, tried.state)
        assertTrue(tried.lastError!!.startsWith("onCompleted threw"), tried.lastError)
        assertEquals("P-1", repository.storedResults(id).single().output?.let { Json.decodeFromString<PaymentResult>(it).paymentId })

        hookBug = false                                                         // fixed
        engine.resumeAfter(1.minutes)
        assertEquals(SagaState.COMPLETED, runBlocking { engine.find(id)!! }.state)
        assertEquals(listOf("R-1"), completed)
    }

    @Test
    fun `a hook that throws after a rejected signal runs again once fixed`() {
        val definition = endedBySignal()
        val engine = engine(definition)
        val id = engine.create(definition, "R-1")
        engine.run(id)

        assertEquals(SignalResult.Delivered(id), engine.pay(id, succeeded = false))
        val tried = engine.run(id)
        assertEquals(SagaState.FINISHING, tried.state)
        assertTrue(tried.lastError!!.startsWith("onFailed threw"), tried.lastError)

        hookBug = false
        engine.resumeAfter(1.minutes)
        assertEquals(SagaState.FAILED, runBlocking { engine.find(id)!! }.state)
        assertEquals(listOf("R-1"), failed)
    }

    @Test
    fun `a hook that throws after a signal stored earlier is no resume failure`() {
        val definition = endedBySignal()
        val engine = engine(definition)
        val id = engine.create(definition, "R-1")
        assertEquals(SignalResult.Stored(id), engine.pay(id))                   // faster than the request

        engine.resumeAfter(1.seconds)                                           // no failure reported

        val finishing = runBlocking { engine.find(id)!! }
        assertEquals(SagaState.FINISHING, finishing.state)
        assertTrue(repository.log.none { it.event == SagaEvents.RESUME_FAILED })
        assertEquals(1, repository.log.count { it.event == SagaEvents.HOOK_FAILED })

        hookBug = false
        engine.resumeAfter(1.minutes)
        assertEquals(SagaState.COMPLETED, runBlocking { engine.find(id)!! }.state)
    }

    // ---- markDone on a signal step: the value delivered by hand, through onSignal (BUG-06) ------------

    /** The reservation, stuck at its payment's timeout (no check): NO_SIGNAL. */
    private fun stuckWithoutSignal(): Pair<SagaEngine, Uuid> {
        val definition = reservation()
        val engine = engine(definition)
        val id = engine.create(definition, "R-1")
        engine.run(id)
        engine.resumeAfter(31.minutes)
        assertEquals(StuckReason.NO_SIGNAL, runBlocking { engine.find(id)!! }.stuckReason)
        return engine to id
    }

    @Test
    fun `markDone on a signal step delivers the value by hand, and onSignal accepts it`() {
        val (engine, id) = stuckWithoutSignal()

        val done = runBlocking { engine.markDone(id, Json.encodeToString(PaymentResult("P-7", succeeded = true)), "operator") }

        assertEquals(SagaState.COMPLETED, done.state)
        assertEquals("confirm P-7", calls.last())
        assertEquals("signal by hand ref=P-7", details(SagaEvents.MANUAL_DONE).single())
    }

    @Test
    fun `markDone on a signal step with a value onSignal refuses compensates, instead of moving on`() {
        val (engine, id) = stuckWithoutSignal()

        val refused = runBlocking { engine.markDone(id, Json.encodeToString(PaymentResult("P-7", succeeded = false)), "operator") }

        assertEquals(SagaState.FAILED, refused.state)                           // before: COMPLETED, onSignal skipped
        assertEquals("dev.octogene.akta.StepRejected: payment P-7 failed", refused.failureReason)
        assertEquals(listOf<String?>("P-1"), released)                          // the payment is undone
        assertTrue("confirm P-7" !in calls)
    }

    @Test
    fun `markDone on a signal step uses the signal kept, and never replaces it`() {
        val definition = watchedReservation()
        val engine = watchedEngine(definition)
        val id = engine.create(definition, "R-1")
        engine.run(id)
        onSignalBug = true
        engine.pay(id)
        assertEquals(StuckReason.RECORD_FAILED, engine.run(id).stuckReason)     // the signal kept
        onSignalBug = false

        val other = Json.encodeToString(PaymentResult("P-other", succeeded = true))
        assertFailsWith<IllegalArgumentException> { runBlocking { engine.markDone(id, other, "operator") } }
        val done = runBlocking { engine.markDone(id, null, "operator") }

        assertEquals(SagaState.COMPLETED, done.state)
        assertEquals("confirm P-1", calls.last())                               // the signal received
    }

    @Test
    fun `two signals stored early are consumed one transaction each, and a bug stops the right step`() {
        var bug = true
        val definition = saga("TWO") {
            awaitSignal<String>("first", signal = "a", timeout = 1.minutes)
            awaitSignal<String>("second", signal = "b", timeout = 1.minutes) { onSignal { if (bug) error("onSignal bug") } }
        }
        val engine = engine(definition)
        val id = engine.create(definition, "R-1")
        runBlocking { engine.signal(id, "a", "A"); engine.signal(id, "b", "B") }  // both faster than the request

        val stuck = engine.run(id)

        assertEquals(1, stuck.stepIndex)                                        // before: step 0 blamed (BUG-02)
        assertEquals(StuckReason.RECORD_FAILED, stuck.stuckReason)
        assertEquals("second", repository.log.single { it.event == SagaEvents.RECORD_FAILED }.stepName)
        assertEquals(listOf("first"), repository.storedResults(id).map { it.stepName })   // step 0 is kept
        bug = false
        assertEquals(SagaState.COMPLETED, runBlocking { engine.retry(id, "operator") }.state)
    }

    @Test
    fun `a bug in onSignal on a value its check found is not taken for a failing lookup`() {
        var checks = 0
        val definition = watchedReservation(checkAtTimeout = { checks++; CheckResult.Done(PaymentResult("P-1", succeeded = true)) })
        val engine = watchedEngine(definition)
        val id = engine.create(definition, "R-1")
        engine.run(id)
        onSignalBug = true

        engine.resumeAfter(31.minutes)

        val stuck = runBlocking { engine.find(id)!! }
        assertEquals(StuckReason.RECORD_FAILED, stuck.stuckReason)               // not CHECK_FAILING
        assertTrue(stuck.lastError!!.contains("onSignal threw on signal 'payment'"), stuck.lastError)
        assertTrue(stuck.lastError!!.contains("The signal is kept"), stuck.lastError)    // the value the check found
        assertEquals(1, checks)                                                 // the lookup is not repeated

        onSignalBug = false                                                     // fixed
        val done = runBlocking { engine.retry(id, "operator") }                 // uses the value found, at once
        assertEquals(SagaState.COMPLETED, done.state)
        assertEquals("confirm P-1", calls.last())
        assertEquals(1, checks)                                                 // no second timeout, no second check
    }

    @Test
    fun `a bug in onSignal on a signal received during a check keeps no refusal`() {
        lateinit var engine: SagaEngine
        lateinit var id: Uuid
        val definition = watchedReservation(checkAtTimeout = {
            engine.pay(id, paymentId = "P-2")                                   // arrives mid-check: stored
            CheckResult.NotDone
        })
        engine = watchedEngine(definition)
        id = engine.create(definition, "R-1")
        engine.run(id)
        onSignalBug = true

        engine.resumeAfter(31.minutes)

        val stuck = runBlocking { engine.find(id)!! }
        assertEquals(StuckReason.RECORD_FAILED, stuck.stuckReason)
        assertNull(stuck.failureReason)                                         // not "No signal before the timeout"
        assertTrue(stuck.lastError!!.contains("The signal is kept"), stuck.lastError)

        onSignalBug = false
        val done = runBlocking { engine.retry(id, "operator") }
        assertEquals(SagaState.COMPLETED, done.state)
        assertEquals("confirm P-2", calls.last())
    }

    // ---- Cancellation -------------------------------------------------------------------------

    @Test
    fun `cancelling a waiting saga compensates at once`() {
        val definition = reservation()
        val engine = engine(definition)
        val id = engine.create(definition, "R-1")
        engine.run(id)

        val cancelled = runBlocking { engine.cancel(id, "customer cancelled", "support") }

        assertEquals(SagaState.FAILED, cancelled.state)
        assertEquals("customer cancelled", cancelled.cancelReason)
        assertEquals("Cancelled: customer cancelled", cancelled.failureReason)
        assertEquals(listOf<String?>("P-1"), released)
        assertEquals(SignalResult.Ignored(id), engine.pay(id))                  // too late now
        assertNull(repository.signal(id, "payment"))
    }

    @Test
    fun `cancelling during a call compensates once its outcome is recorded, that call included`() {
        lateinit var engine: SagaEngine
        lateinit var id: Uuid
        val definition = reservation(pay = {
            runBlocking { engine.cancel(id, "customer cancelled", "support") }   // while the payment is created
            assertEquals(SignalResult.Ignored(id), engine.pay(id))              // the saga will never wait for it
            "P-1"
        })
        engine = engine(definition)
        id = engine.create(definition, "R-1")

        val done = engine.run(id)

        assertEquals(SagaState.FAILED, done.state)
        assertEquals(listOf<String?>("P-1"), released)                          // the payment just made is undone
        assertTrue(calls.none { it.startsWith("confirm") })
        assertNull(repository.signal(id, "payment"))
        assertEquals("ignored: cancelled ref=P-1", details(SagaEvents.SIGNAL_RECEIVED).single())
    }

    @Test
    fun `the cancellation reason survives a stuck step and its retry`() {
        lateinit var engine: SagaEngine
        lateinit var id: Uuid
        val definition = saga("STUCK") {
            step<String>("create-payment") {
                execute {
                    runBlocking { engine.cancel(id, "customer cancelled", "support") }
                    runBlocking { engine.cancel(id, "second click", "support") }   // only logged
                    throw java.io.IOException("read timeout")                    // unknown outcome
                }
                check { CheckResult.CannotTell }                                 // a human must look
            }
        }
        engine = engine(definition, SagaConfig(settleDelay = 1.seconds))
        id = engine.create(definition, "R-1")
        engine.run(id)
        engine.resumeAfter(2.seconds)

        val stuck = runBlocking { engine.find(id)!! }
        assertEquals(StepStatus.STUCK, stuck.stepStatus)
        assertEquals("customer cancelled", stuck.cancelReason)                  // lastError says why it is stuck

        // The operator found it was not applied: retry. The saga compensates instead of executing it again.
        val done = runBlocking { engine.retry(id, "operator") }
        assertEquals(SagaState.FAILED, done.state)
        assertEquals("customer cancelled", done.cancelReason)
        assertEquals("Cancelled: customer cancelled", done.failureReason)
        assertEquals(listOf("customer cancelled", "second click"), details(SagaEvents.CANCEL_REQUESTED))
    }

    @Test
    fun `cancelling during the last step's call compensates instead of completing`() {
        lateinit var engine: SagaEngine
        lateinit var id: Uuid
        val definition = saga("SHORT") {
            step<String>("create-payment") {
                execute { runBlocking { engine.cancel(id, "customer cancelled", "support") }; "P-1" }
                compensate { own -> released += own }
            }
            onCompleted { completed += key }
        }
        engine = engine(definition)
        id = engine.create(definition, "R-1")

        val done = engine.run(id)
        assertEquals(SagaState.FAILED, done.state)
        assertEquals("Cancelled: customer cancelled", done.failureReason)
        assertEquals(listOf<String?>("P-1"), released)
        assertTrue(completed.isEmpty())
    }

    @Test
    fun `cancelling a saga already compensating changes nothing, and the history says so`() {
        val definition = saga("REFUSED") {
            step<String>("create-payment") {
                execute { "P-1" }
                compensate { throw java.io.IOException("refund service down") }    // keeps it compensating
            }
            step("confirm") { execute { throw StepRejected("seat taken") } }
        }
        val engine = engine(definition)
        val id = engine.create(definition, "R-1")
        assertEquals(SagaState.COMPENSATING, engine.run(id).state)

        val after = runBlocking { engine.cancel(id, "customer left", "support", runFor = Duration.ZERO) }

        assertEquals(SagaState.COMPENSATING, after.state)
        assertNull(after.cancelRequestedAt)                                     // not cancelled: already failing
        assertEquals("dev.octogene.akta.StepRejected: seat taken", after.failureReason)
        assertEquals(listOf("customer left (already compensating: nothing changed)"), details(SagaEvents.CANCEL_REQUESTED))
    }

    @Test
    fun `a finished saga cannot be cancelled`() {
        val definition = reservation()
        val engine = engine(definition)
        val id = engine.create(definition, "R-1")
        engine.pay(id)
        engine.run(id)

        assertFailsWith<IllegalArgumentException> { runBlocking { engine.cancel(id, "too late", "support") } }
    }

    // ---- History ------------------------------------------------------------------------------

    @Test
    fun `the log names the signal but never carries its value`() {
        val definition = reservation()
        val engine = engine(definition)
        val id = engine.create(definition, "R-1")
        engine.run(id)
        engine.pay(id)
        engine.run(id)

        assertTrue(details(SagaEvents.WAIT_STARTED).single()!!.startsWith("signal 'payment' until "))
        assertEquals(listOf<String?>("delivered ref=P-1", "consumed (stored earlier) ref=P-1"), details(SagaEvents.SIGNAL_RECEIVED))
        assertTrue(repository.log.none { "PII-" in it.detail.orEmpty() })
    }
}
