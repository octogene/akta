package dev.octogene.akta

import dev.octogene.akta.memory.InMemorySagaRepository
import dev.octogene.akta.memory.InMemorySagaTransactions
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Timeout
import java.io.IOException
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertNotEquals
import kotlin.test.assertTrue
import kotlin.time.Clock
import kotlin.time.Duration
import kotlin.time.Duration.Companion.days
import kotlin.time.Duration.Companion.hours
import kotlin.time.Duration.Companion.minutes
import kotlin.time.Duration.Companion.seconds
import kotlin.time.Instant
import kotlin.uuid.Uuid

/** Steps whose service accepts an idempotency key: replayed after an unknown outcome, never checked. */
@Timeout(10)
class SagaIdempotencyTest {

    private class MutableClock(var now: Instant = Instant.parse("2026-11-22T10:00:00Z")) : Clock {
        override fun now(): Instant = now
        fun advance(d: Duration) { now += d }
    }

    private val clock = MutableClock()
    private val repository = InMemorySagaRepository()
    private val keys = mutableListOf<String>()
    private val undone = mutableListOf<String?>()

    /** A payment service that remembers keys: a replay returns the first payment. */
    private val payments = HashMap<String, String>()

    /** One idempotent payment step, whose calls fail as [outcomes] say, one per attempt (then succeed). */
    private fun payment(
        vararg outcomes: Throwable,
        keyRetention: Duration = 24.hours,
        check: (suspend SagaContext<Unit>.() -> CheckResult<String>)? = { error("an idempotent step is never checked") },
    ): SagaDefinition<Unit> {
        val failures = ArrayDeque(outcomes.toList())
        return saga("PAYMENT") {
            step<String>("create-payment") {
                idempotent(keyRetention)
                execute {
                    keys += idempotencyKey
                    val payment = payments.getOrPut(idempotencyKey) { "P-${payments.size + 1}" }
                    failures.removeFirstOrNull()?.let { throw it }                // applied, but the response is lost
                    payment
                }
                compensate { own -> keys += idempotencyKey; undone += own }
                check?.let { check(it) }
            }
            step("confirm") { execute { } }
        }
    }

    private fun engine(definition: SagaDefinition<Unit>, config: SagaConfig = SagaConfig(settleDelay = 10.minutes)) =
        SagaEngine(InMemorySagaTransactions(repository), repository, listOf(definition), KotlinxJsonSagaSerializer(), config, clock)

    private fun SagaEngine.run(id: Uuid) = runBlocking { run(id, clock.now + 10.seconds) }
    private fun SagaEngine.stored(id: Uuid) = runBlocking { find(id)!! }
    private fun SagaEngine.resumeAfter(d: Duration) {
        clock.advance(d)
        assertEquals(emptyMap(), runBlocking { resumeDue() }.failures)
    }

    @Test
    fun `an unknown outcome is replayed with the same key after backoff, without check or settle delay`() {
        val definition = payment(IOException("read timeout"))
        val engine = engine(definition)
        val id = engine.create(definition, "R-1")

        val unknown = engine.run(id)
        assertEquals(StepStatus.REPLAYING, unknown.stepStatus)                    // it may have been applied
        assertEquals(clock.now + 1.minutes, unknown.nextAttemptAt)              // backoff, not the 10-minute settle delay
        assertTrue(unknown.lastError!!.endsWith("retrying with the same key"))

        engine.resumeAfter(1.minutes)

        assertEquals(SagaState.COMPLETED, engine.stored(id).state)
        assertEquals(listOf("$id/create-payment/execute", "$id/create-payment/execute"), keys)
        assertEquals(1, payments.size)                                          // applied once
        assertEquals("\"P-1\"", repository.results(id)["create-payment"])     // the first payment
    }

    @Test
    fun `a lock that expired mid-call is replayed at once with the same key, on another pod`() {
        var first = true
        val definition = saga("PAYMENT") {
            step<String>("create-payment") {
                idempotent(24.hours)
                execute {
                    keys += idempotencyKey
                    if (first) {
                        first = false
                        throw SimulatedCrash()                                  // the pod dies during the call
                    }
                    "P-1"
                }
                check { error("an idempotent step is never checked") }
            }
        }
        val crashed = engine(definition)
        val id = crashed.create(definition, "R-1")
        assertFailsWith<SimulatedCrash> { runBlocking { crashed.run(id, clock.now + 10.seconds) } }

        val other = engine(definition)                                          // another pod's engine
        other.resumeAfter(SagaConfig().callLock + 1.seconds)

        assertEquals(SagaState.COMPLETED, other.stored(id).state)
        assertEquals(listOf("$id/create-payment/execute", "$id/create-payment/execute"), keys)
    }

    @Test
    fun `a compensation gets its own key`() {
        val definition = saga("PAYMENT") {
            step<String>("create-payment") {
                idempotent(24.hours)
                execute { keys += idempotencyKey; "P-1" }
                compensate { own -> keys += idempotencyKey; undone += own }
            }
            step("confirm") { execute { throw StepRejected("refused") } }
        }
        val engine = engine(definition)
        val id = engine.create(definition, "R-1")

        assertEquals(SagaState.FAILED, engine.run(id).state)
        assertEquals(listOf("$id/create-payment/execute", "$id/create-payment/compensate"), keys)
    }

    @Test
    fun `cancel waits for the replay of an unknown call, then undoes it`() {
        lateinit var engine: SagaEngine
        lateinit var id: Uuid
        var cancelled = false
        val definition = saga("PAYMENT") {
            step<String>("create-payment") {
                idempotent(24.hours)
                execute {
                    val payment = payments.getOrPut(idempotencyKey) { "P-1" }
                    if (!cancelled) {
                        cancelled = true
                        runBlocking { engine.cancel(id, "customer cancelled", "support") }
                        throw IOException("read timeout")                        // applied, response lost
                    }
                    payment
                }
                compensate { own -> undone += own }
            }
        }
        engine = engine(definition)
        id = engine.create(definition, "R-1")

        val waiting = engine.run(id)
        assertEquals(SagaState.RUNNING to StepStatus.REPLAYING, waiting.state to waiting.stepStatus)   // not undone blindly
        assertFailsWith<IllegalArgumentException> { runBlocking { engine.abort(id, "operator", currentStepApplied = false) } }

        engine.resumeAfter(1.minutes)                                           // the replay finds the payment

        assertEquals(SagaState.FAILED, engine.stored(id).state)
        assertEquals(listOf<String?>("P-1"), undone)                            // the payment made is refunded
    }

    @Test
    fun `a replay that is not sent leaves the step maybe applied, so cancel still waits for an answer`() {
        var attempt = 0
        val definition = saga("PAYMENT") {
            step<String>("create-payment") {
                idempotent(24.hours)
                execute {
                    attempt++
                    payments.getOrPut(idempotencyKey) { "P-1" }                 // applied by the first attempt
                    when (attempt) {
                        1 -> throw SimulatedCrash()                             // then the pod dies
                        2 -> throw StepNotSent("connection refused")            // the replay never leaves
                        else -> payments.getValue(idempotencyKey)
                    }
                }
                compensate { own -> undone += own }
            }
        }
        val engine = engine(definition)
        val id = engine.create(definition, "R-1")
        assertFailsWith<SimulatedCrash> { runBlocking { engine.run(id, clock.now + 10.seconds) } }
        engine.resumeAfter(SagaConfig().callLock + 1.seconds)                   // lock expired: replayed, not sent

        assertEquals(StepStatus.REPLAYING, engine.stored(id).stepStatus)          // not READY: it may be applied
        runBlocking { engine.cancel(id, "customer cancelled", "support") }
        assertEquals(SagaState.RUNNING, engine.stored(id).state)                // cancel waits for an answer

        engine.resumeAfter(2.minutes)                                           // the replay finds the payment

        assertEquals(SagaState.FAILED, engine.stored(id).state)
        assertEquals(listOf<String?>("P-1"), undone)
    }

    @Test
    fun `requests that never left do not use the key`() {
        val down = StepNotSent("connection refused")
        val definition = payment(down, down, down, keyRetention = 2.minutes, check = null)
        val engine = engine(definition, SagaConfig(maxAttempts = 3))
        val id = engine.create(definition, "R-1")

        val first = engine.run(id)
        assertEquals(StepStatus.READY to null, first.stepStatus to first.keySince)   // not a used key
        engine.resumeAfter(1.minutes)
        assertEquals(StepStatus.READY, engine.stored(id).stepStatus)            // not REPLAYING: abort is allowed
        engine.resumeAfter(2.minutes)                                           // 3 minutes after the first: no KEY_EXPIRED

        val stuck = engine.stored(id)
        assertEquals(StuckReason.TOO_MANY_ATTEMPTS, stuck.stuckReason)
        val retried = runBlocking { engine.retry(id, "operator") }              // a fresh start, not a replay
        assertEquals(SagaState.COMPLETED, retried.state)
        assertTrue(repository.log.none { it.event == SagaEvents.KEY_EXPIRED })
    }

    @Test
    fun `too many unknown outcomes make the step stuck, and a retry keeps the key`() {
        val definition = payment(IOException("timeout"), IOException("timeout"), check = null)
        val engine = engine(definition, SagaConfig(maxAttempts = 2))
        val id = engine.create(definition, "R-1")
        engine.run(id)
        engine.resumeAfter(1.minutes)
        assertEquals(StuckReason.TOO_MANY_ATTEMPTS, engine.stored(id).stuckReason)

        assertEquals(SagaState.COMPLETED, runBlocking { engine.retry(id, "operator") }.state)   // safe without looking

        assertEquals(1, payments.size)
        assertEquals(1, keys.distinct().size)
    }

    @Test
    fun `retrying a cancelled saga that may be applied replays first, then undoes the step`() {
        val definition = payment(IOException("timeout"), IOException("timeout"), check = null)
        val engine = engine(definition, SagaConfig(maxAttempts = 2))
        val id = engine.create(definition, "R-1")
        engine.run(id)
        engine.resumeAfter(1.minutes)                                           // stuck: applied, both responses lost
        runBlocking { engine.cancel(id, "customer cancelled", "support") }      // stays stuck

        val done = runBlocking { engine.retry(id, "operator") }                 // a replay, not a fresh start

        assertEquals(SagaState.FAILED, done.state)
        assertEquals(listOf<String?>("P-1"), undone)                            // the payment found by the replay is refunded
    }

    @Test
    fun `past the key's retention the step stops for a human, and a new key starts afresh`() {
        val definition = payment(IOException("timeout"), IOException("timeout"), IOException("timeout"), keyRetention = 2.minutes)
        val engine = engine(definition)
        val id = engine.create(definition, "R-1")
        engine.run(id)                                                          // t0: unknown
        engine.resumeAfter(1.minutes)                                           // t0+1m: replayed, unknown again
        engine.resumeAfter(2.minutes)                                           // t0+3m: past the window

        val expired = engine.stored(id)
        assertEquals(StuckReason.KEY_EXPIRED, expired.stuckReason)
        assertEquals(2, keys.size)                                              // no call past the window
        assertTrue(repository.log.any { it.event == SagaEvents.KEY_EXPIRED })
        assertFailsWith<IllegalArgumentException> { runBlocking { engine.retry(id, "operator") } }

        // The operator found the payment was not applied (here, the service forgot it), and asks for a new key.
        payments.clear()
        val retried = runBlocking { engine.retry(id, "operator", newKey = true) }   // the 3rd failure, then...
        engine.resumeAfter(1.minutes)

        assertEquals(SagaState.COMPLETED, engine.stored(id).state)
        assertEquals("$id/create-payment/execute/1", keys.last())
        assertEquals(StepStatus.REPLAYING, retried.stepStatus)
        assertEquals("newKey: generation 1", repository.log.last { it.event == SagaEvents.MANUAL_RETRY }.detail)
    }

    @Test
    fun `an undo is not sent again with a key its service may have forgotten`() {
        var undoFails = true
        val definition = saga("PAYMENT") {
            step<String>("create-payment") {
                idempotent(24.hours)
                execute { "P-1" }
                compensate { own ->
                    keys += idempotencyKey                                      // the refund relies on its key
                    if (undoFails) throw IOException("refund service down")
                    undone += own
                }
            }
            step("confirm") { execute { throw StepRejected("seat taken") } }
        }
        val engine = engine(definition, SagaConfig(maxAttempts = 2))
        val id = engine.create(definition, "R-1")
        engine.run(id)                                                          // refused: the undo fails
        engine.resumeAfter(1.minutes)                                           // fails again: stuck
        assertEquals(StuckReason.TOO_MANY_ATTEMPTS, engine.stored(id).stuckReason)

        clock.advance(2.days)                                                   // the operator comes back late
        undoFails = false
        val expired = runBlocking { engine.retry(id, "operator") }

        assertEquals(StuckReason.KEY_EXPIRED, expired.stuckReason)              // not sent with a forgotten key
        assertTrue(expired.lastError!!.contains("undo"), expired.lastError)
        assertEquals(2, keys.size)
        assertFailsWith<IllegalArgumentException> { runBlocking { engine.retry(id, "operator") } }

        // Looked in the provider: not refunded. A fresh key.
        val done = runBlocking { engine.retry(id, "operator", newKey = true) }
        assertEquals(SagaState.FAILED, done.state)
        assertEquals(listOf<String?>("P-1"), undone)
        assertEquals(listOf("$id/create-payment/compensate", "$id/create-payment/compensate", "$id/create-payment/compensate/1"), keys)
    }

    @Test
    fun `the key moves on with the saga and is the same as a UUID every time`() {
        val uuids = mutableListOf<Uuid>()
        val definition = saga("T") {
            step("one") { execute { uuids += idempotencyUuid; uuids += idempotencyUuid } }
            step("two") { execute { uuids += idempotencyUuid } }
        }
        val engine = engine(definition)
        val id = engine.create(definition, "R-1")
        engine.run(id)

        assertEquals(uuids[0], uuids[1])
        assertNotEquals(uuids[0], uuids[2])
        assertEquals(5, uuids[0].toString()[14].digitToInt())                   // version 5
        val record = engine.stored(id)
        assertEquals(null, record.keySince)
    }

    @Test
    fun `the key is refused outside a call, and bad declarations at startup`() {
        val inHook = saga("T") {
            step("one") { execute { } }
            onCompleted { idempotencyKey }
        }
        val engine = engine(inHook)
        val id = engine.create(inHook, "R-1")
        engine.run(id)
        engine.stored(id).let {                                                 // the hook threw: tried again later
            assertEquals(SagaState.FINISHING, it.state)
            assertTrue(it.lastError!!.contains("No idempotency key outside"), it.lastError)
        }

        assertFailsWith<IllegalArgumentException> {                            // a single call could outlive the key
            engine(payment(keyRetention = 10.seconds))
        }
        assertFailsWith<IllegalArgumentException> { saga("T") { step("x".repeat(101)) { execute { } } } }
        val idempotentWait = object : SignalStep<Unit, String> {
            override val name = "wait"
            override val signal = "payment"
            override val timeout = 1.minutes
            override val checksOnTimeout = false
            override val outputType = kotlin.reflect.typeOf<String>()
            override val idempotency = Idempotency(1.hours)
        }
        assertFailsWith<IllegalArgumentException> { SagaDefinition("T", null, listOf(idempotentWait)) }
    }
}
