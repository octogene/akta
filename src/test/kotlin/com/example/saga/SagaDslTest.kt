package com.example.saga

import com.example.saga.memory.InMemorySagaRepository
import com.example.saga.memory.InMemorySagaTransactions
import kotlinx.coroutines.runBlocking
import kotlinx.serialization.Serializable
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertNull
import kotlin.test.assertTrue
import kotlin.time.Clock
import kotlin.time.Duration.Companion.minutes
import kotlin.time.Duration.Companion.seconds
import kotlin.time.Instant
import kotlin.uuid.ExperimentalUuidApi
import kotlin.uuid.Uuid

/** The DSL, and the typed data it declares: saga inputs and step outputs. */
class SagaDslTest {

    @Serializable
    data class BookingRequest(val customerId: String, val start: String, val end: String)

    @Serializable
    data class Hold(val id: String, val warehouse: String)

    /** Not @Serializable: the engine must refuse it at startup. */
    data class Unserializable(val id: String)

    private var now = Instant.parse("2026-11-19T10:00:00Z")
    private val clock = object : Clock {
        override fun now() = now
    }
    private val repository = InMemorySagaRepository()
    private val serializer = KotlinxJsonSagaSerializer()

    private fun engine(vararg definitions: SagaDefinition<*>, config: SagaConfig = SagaConfig()) =
        SagaEngine(InMemorySagaTransactions(repository), repository, definitions.toList(), serializer, config, clock)

    private fun SagaDefinition<Unit>.runFor(key: String): SagaRecord = runBlocking {
        val engine = engine(this@runFor)
        engine.run(engine.create(this@runFor, key), now + 10.seconds)
    }

    // ---- Building sagas ---------------------------------------------------------------------

    @Test
    fun `builds steps in order and passes outputs between them`() {
        val calls = mutableListOf<String>()
        val completed = mutableListOf<String>()
        val definition = saga("ITEM_SETUP") {
            val stock = step<String>("stock") {
                execute { calls += "stock:$key"; "STK-1" }
            }
            step<String>("booking") {
                execute { calls += "booking:${requireResult(stock)}"; "RES-1" }
            }
            onCompleted { completed += key }
        }

        val saga = definition.runFor("item-1")

        assertEquals(SagaState.COMPLETED, saga.state)
        assertEquals(listOf("stock:item-1", "booking:STK-1"), calls)
        assertEquals(listOf("item-1"), completed)
        assertEquals(listOf("stock", "booking"), definition.steps.map { it.name })
        assertEquals(mapOf("stock" to "\"STK-1\"", "booking" to "\"RES-1\""), repository.results(saga.id))  // JSON
    }

    @Test
    fun `a step without a type argument has no output`() {
        val definition = saga("NOTIFY") {
            step("notify") { execute { } }
        }
        val saga = definition.runFor("item-1")

        assertEquals(SagaState.COMPLETED, saga.state)
        assertEquals(mapOf("notify" to null), repository.results(saga.id))
        assertNull(definition.steps.single().outputType)
    }

    @Test
    fun `compensation receives the step's own output`() {
        val released = mutableListOf<Hold?>()
        val failed = mutableListOf<String>()
        val definition = saga("ITEM_SETUP") {
            step<Hold>("hold") {
                execute { Hold("H-1", "north") }
                compensate { own -> released += own }
            }
            step("booking") {
                execute { throw StepRejected("not bookable") }
            }
            onFailed { failed += key }
        }

        val saga = definition.runFor("item-1")

        assertEquals(SagaState.FAILED, saga.state)
        assertEquals(listOf<Hold?>(Hold("H-1", "north")), released)
        assertEquals(listOf("item-1"), failed)
    }

    @Test
    fun `the input and the outputs reach every step, check, compensation and hook, typed`() {
        val seen = mutableListOf<String>()
        var confirmed: Pair<BookingRequest, Hold>? = null
        var checks = 0
        val definition = saga<BookingRequest>("BOOKING") {
            val hold = step<Hold>("hold") {
                execute { seen += "execute ${input.start}"; throw RuntimeException("read timeout") }   // unknown
                checkByLookup { checks++; seen += "check ${input.start}"; listOf(Hold("H-7", "south")) }
            }
            step<String>("booking") {
                execute { seen += "book ${input.customerId} at ${requireResult(hold).warehouse}"; "B-1" }
            }
            onCompleted { confirmed = input to requireResult(hold) }
        }
        val engine = engine(definition)
        val request = BookingRequest("C-1", "2026-12-01", "2026-12-03")
        val id = engine.create(definition, "item-1", request)

        runBlocking { engine.run(id, now + 10.seconds) }
        now += 2.minutes
        runBlocking { engine.resumeDue() }

        assertEquals(SagaState.COMPLETED, repository.find(id)!!.state)
        assertEquals(listOf("execute 2026-12-01", "check 2026-12-01", "book C-1 at south"), seen)
        assertEquals(request to Hold("H-7", "south"), confirmed)              // the check's output was stored
        assertEquals(1, checks)
    }

    @Test
    fun `saga_log records what logAs returns, never the value`() {
        val definition = saga<BookingRequest>("BOOKING") {
            step<Hold>("hold") {
                execute { Hold("H-1", "warehouse with a customer name in it") }
                logAs { it.id }
            }
            step<Hold>("silent") { execute { Hold("H-2", "secret") } }               // no logAs: nothing
            step<String>("ref") { execute { "R-1" } }                                // a String: logged
        }
        val engine = engine(definition)
        val id = engine.create(definition, "item-1", BookingRequest("C-1", "2026-12-01", "2026-12-03"))
        runBlocking { engine.run(id, now + 10.seconds) }

        val details = repository.log.filter { it.event == "EXECUTE:Done" }.map { it.stepName to it.detail }
        assertEquals(listOf("hold" to "ref=H-1", "silent" to null, "ref" to "ref=R-1"), details)
        val everything = repository.log.joinToString { "${it.detail}" }
        assertTrue("C-1" !in everything && "secret" !in everything && "warehouse" !in everything, everything)
    }

    @OptIn(ExperimentalUuidApi::class)
    @Test
    fun `checkByLookup cannot tell when several matches exist`() {
        val step = saga("T") {
            step<String>("stock") {
                execute { "x" }
                checkByLookup { listOf("STK-1", "STK-2") }
            }
        }.steps.single()
        val ctx = SagaContext(Uuid.generateV7(), "item-1", Unit, emptyMap())
        assertEquals(CheckResult.CannotTell, runBlocking { step.check(ctx) })
    }

    @Test
    fun `a step without execute is refused`() {
        assertFailsWith<IllegalArgumentException> {
            saga("T") { step<String>("stock") { checkByLookup { emptyList() } } }
        }
    }

    @Test
    fun `duplicate step names are refused`() {
        assertFailsWith<IllegalArgumentException> {
            saga("T") {
                step("stock") { execute { } }
                step("stock") { execute { } }
            }
        }
    }

    // ---- The engine and the data ------------------------------------------------------------

    @Test
    fun `a type that cannot be serialized is refused when the engine is built`() {
        val definition = saga("T") { step<Unserializable>("stock") { execute { Unserializable("x") } } }
        val error = assertFailsWith<IllegalArgumentException> { engine(definition) }
        assertTrue("step stock" in error.message!!, error.message)
    }

    @Test
    fun `an input over the size limit is refused by create, and nothing is written`() {
        val definition = saga<BookingRequest>("BOOKING") { step("x") { execute { } } }
        val engine = engine(definition, config = SagaConfig(maxDataBytes = 50))

        assertFailsWith<IllegalArgumentException> {
            engine.create(definition, "item-1", BookingRequest("C-1", "2026-12-01", "a".repeat(100)))
        }
        assertTrue(repository.log.isEmpty())
    }

    @Test
    fun `an output that cannot be stored makes the step stuck without calling again`() {
        var calls = 0
        val definition = saga("T") {
            step<String>("stock") { execute { calls++; "x".repeat(100) } }
        }
        val engine = engine(definition, config = SagaConfig(maxDataBytes = 50))
        val saga = runBlocking { engine.run(engine.create(definition, "item-1"), now + 10.seconds) }

        assertEquals(StepStatus.STUCK, saga.stepStatus)
        assertTrue(saga.lastError!!.startsWith("The call was made, but its output could not be stored"))
        assertTrue("xxxx" !in saga.lastError)                                // the value is not in the error
        now += 1.minutes
        runBlocking { engine.resumeDue() }
        assertEquals(1, calls)
    }

    @Test
    fun `data that cannot be read makes no call, and the saga is postponed`() {
        var calls = 0
        val definition = saga<BookingRequest>("BOOKING") { step("x") { execute { calls++ } } }
        val engine = engine(definition)
        val id = engine.create(definition, "item-1", BookingRequest("C-1", "2026-12-01", "2026-12-03"))
        repository.save(repository.find(id)!!.copy(input = """{"customerId": 42"""), now)   // written by "another version"

        val result = runBlocking { engine.resumeDue() }

        assertTrue(result.failures.getValue(id) is SagaDataException)
        assertEquals(0, calls)
        assertEquals(StepStatus.READY, repository.find(id)!!.stepStatus)
        assertEquals(1, repository.log.count { it.event == SagaEvents.RESUME_FAILED })
    }

    @Test
    fun `markDone checks the output against the step's type`() {
        val definition = saga("T") {
            val hold = step<Hold>("hold") { execute { throw RuntimeException("timeout") } }   // no check: stuck
            step<String>("next") { execute { requireResult(hold).id } }
        }
        val engine = engine(definition)
        val id = engine.create(definition, "item-1")
        runBlocking { engine.run(id, now + 10.seconds) }
        now += 2.minutes
        runBlocking { engine.resumeDue() }
        assertEquals(StepStatus.STUCK, repository.find(id)!!.stepStatus)

        assertFailsWith<IllegalArgumentException> { runBlocking { engine.markDone(id, """{"id":"H-1"}""", "alice") } }
        assertFailsWith<IllegalArgumentException> { runBlocking { engine.markDone(id, null, "alice") } }
        val done = runBlocking { engine.markDone(id, """{"id":"H-1","warehouse":"north"}""", "alice") }

        assertEquals(SagaState.COMPLETED, done.state)
        assertEquals("\"H-1\"", repository.results(id)["next"])              // the next step read it, typed
    }

    @Test
    fun `after a record failure the output is kept, and markDone continues with it`() {
        var hookFails = true
        val definition = saga("T") {
            step<Hold>("hold") { execute { Hold("H-1", "north") } }
            onCompleted { check(!hookFails) { "items table locked" } }
        }
        val engine = engine(definition)
        val saga = runBlocking { engine.run(engine.create(definition, "item-1"), now + 10.seconds) }
        assertEquals(StepStatus.STUCK, saga.stepStatus)
        assertTrue("H-1" !in saga.lastError!!)                                 // not in the error text

        hookFails = false
        val done = runBlocking { engine.markDone(saga.id, output = null, actor = "alice") }
        assertEquals(SagaState.COMPLETED, done.state)
        assertEquals("""{"id":"H-1","warehouse":"north"}""", repository.results(saga.id)["hold"])
    }
}
