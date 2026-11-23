package dev.octogene.akta

import dev.octogene.akta.memory.InMemorySagaRepository
import dev.octogene.akta.memory.InMemorySagaTransactions
import kotlinx.coroutines.runBlocking
import kotlinx.serialization.Serializable
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
        assertNull(repository.signal(id, "payment"))                            // a late value is not kept
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
    fun `a signal during a timeout check that finds a value is stored, and the value found wins`() {
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
        assertEquals("confirm P-1", calls.last())                               // the check's value, not the signal's
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
        assertEquals("consumed (stored during the check) ref=P-2", details(SagaEvents.SIGNAL_RECEIVED).last())
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
        engine.resumeAfter(31.minutes)                                          // the check fails: UNKNOWN
        assertEquals(StepStatus.UNKNOWN, runBlocking { engine.find(id)!! }.stepStatus)

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
        engine.resumeAfter(31.minutes)                                          // the check fails: UNKNOWN

        engine.resumeAfter(2.minutes)                                           // when it would check again

        assertEquals(SagaState.COMPLETED, runBlocking { engine.find(id)!! }.state)
        assertEquals("confirm P-2", calls.last())                               // the signal's value
        assertEquals(1, checks)
        assertEquals("consumed (stored earlier) ref=P-2", details(SagaEvents.SIGNAL_RECEIVED).last())
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
        assertEquals(listOf<String?>("delivered ref=P-1"), details(SagaEvents.SIGNAL_RECEIVED))
        assertTrue(repository.log.none { "PII-" in it.detail.orEmpty() })
    }
}
