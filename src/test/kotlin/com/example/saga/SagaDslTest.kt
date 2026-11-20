package com.example.saga

import com.example.saga.memory.InMemorySagaRepository
import com.example.saga.memory.InMemorySagaTransactions
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.time.Clock
import kotlin.time.Duration.Companion.minutes
import kotlin.time.Duration.Companion.seconds
import kotlin.time.Instant
import kotlin.uuid.ExperimentalUuidApi
import kotlin.uuid.Uuid

class SagaDslTest {

    private var now = Instant.parse("2026-11-19T10:00:00Z")
    private val clock = object : Clock {
        override fun now() = now
    }
    private val repository = InMemorySagaRepository()

    private fun SagaDefinition.runFor(key: String): SagaRecord = runBlocking {
        val engine = SagaEngine(InMemorySagaTransactions(repository), repository, listOf(this@runFor), clock = clock)
        engine.run(engine.create(type, key), now + 10.seconds)
    }

    @Test
    fun `builds steps in order and passes results between them`() {
        val calls = mutableListOf<String>()
        val completed = mutableListOf<String>()
        val definition = saga("ITEM_SETUP") {
            val stock = step("stock") {
                execute { calls += "stock:$key"; "STK-1" }
            }
            step("booking") {
                execute { calls += "booking:${requireResult(stock)}"; "RES-1" }
            }
            onCompleted { completed += key }
        }

        val saga = definition.runFor("item-1")

        assertEquals(SagaState.COMPLETED, saga.state)
        assertEquals(listOf("stock:item-1", "booking:STK-1"), calls)
        assertEquals(listOf("item-1"), completed)
        assertEquals(listOf("stock", "booking"), definition.steps.map { it.name })
    }

    @Test
    fun `compensation receives the step's own result`() {
        val reverted = mutableListOf<String?>()
        val failed = mutableListOf<String>()
        val definition = saga("ITEM_SETUP") {
            step("stock") {
                execute { "STK-1" }
                compensate { stockId -> reverted += stockId }
            }
            step("booking") {
                execute { throw StepRejected("not bookable") }
            }
            onFailed { failed += key }
        }

        val saga = definition.runFor("item-1")

        assertEquals(SagaState.FAILED, saga.state)
        assertEquals(listOf<String?>("STK-1"), reverted)
        assertEquals(listOf("item-1"), failed)
    }

    @Test
    fun `checkByLookup adopts a single match`() {
        val definition = saga("ITEM_SETUP") {
            step("stock") {
                execute { throw RuntimeException("read timeout") }
                checkByLookup { listOf("STK-7") }
            }
        }
        val saga = definition.runFor("item-1")
        assertEquals(StepStatus.UNKNOWN, saga.stepStatus)

        now += 2.minutes
        runBlocking {
            SagaEngine(InMemorySagaTransactions(repository), repository, listOf(definition), clock = clock).resumeDue()
        }
        assertEquals(SagaState.COMPLETED, repository.find(saga.id)!!.state)
        assertEquals("STK-7", repository.results(saga.id)["stock"])
    }

    @OptIn(ExperimentalUuidApi::class)
    @Test
    fun `checkByLookup cannot tell when several matches exist`() {
        val step = saga("T") {
            step("stock") {
                execute { "x" }
                checkByLookup { listOf("STK-1", "STK-2") }
            }
        }.steps.single()
        val ctx = SagaContext(Uuid.generateV7(), "item-1", emptyMap())
        assertEquals(CheckResult.CannotTell, runBlocking { step.check(ctx) })
    }

    @Test
    fun `a step without execute is refused`() {
        assertFailsWith<IllegalArgumentException> {
            saga("T") { step("stock") { checkByLookup { emptyList() } } }
        }
    }

    @Test
    fun `duplicate step names are refused`() {
        assertFailsWith<IllegalArgumentException> {
            saga("T") {
                step("stock") { execute { null } }
                step("stock") { execute { null } }
            }
        }
    }
}
