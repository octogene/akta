package dev.octogene.akta

import dev.octogene.akta.memory.InMemorySagaRepository
import dev.octogene.akta.memory.InMemorySagaTransactions
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Test
import java.io.IOException
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue
import kotlin.time.Clock
import kotlin.time.Duration.Companion.seconds
import kotlin.uuid.Uuid

/** What a SagaTracer sees of each call, and that no tracer can change what a saga does. */
class SagaTracerTest {
    private val repository = InMemorySagaRepository()
    private val transactions = InMemorySagaTransactions(repository)
    private var holds = 0

    private val definition = saga("ORDER") {
        step<String>("hold") { execute { holds++; "H-1" }; compensate { } }
        step("pay") { execute { throw StepRejected("card declined") } }
    }

    private fun engine(tracer: SagaTracer, traceId: () -> String? = { null }) = SagaEngine(
        transactions, repository, listOf(definition), KotlinxJsonSagaSerializer(), SagaConfig(maxAttempts = 1),
        traceId = traceId, tracer = tracer,
    )

    private fun SagaEngine.start(traceparent: String? = null): Uuid = runBlocking {
        val id = transactions.inTransaction { create(definition, "order:1", traceparent) }
        run(id, Clock.System.now() + 10.seconds)
        id
    }

    @Test
    fun `a tracer sees each call before it is made, and its outcome after`() {
        val seen = mutableListOf<Pair<SagaCall, SagaCallOutcome>>()
        val engine = engine({ call, block -> block().also { seen += call to it } })
        val traceparent = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01"
        val id = engine.start(traceparent)

        assertEquals(
            listOf("hold" to CallAction.EXECUTE, "pay" to CallAction.EXECUTE, "hold" to CallAction.COMPENSATE),
            seen.map {
                it.first.step to
                    it.first.action
            },
        )
        val first = seen.first().first
        assertEquals(SagaCall(id, "ORDER", "order:1", "hold", 0, CallAction.EXECUTE, attempt = 1, traceparent), first)
        assertEquals(SagaCallOutcome("Done", failed = false, error = null), seen[0].second)
        // A refusal is the service's answer, not a failure of the call.
        assertEquals("Rejected", seen[1].second.name)
        assertFalse(seen[1].second.failed)
        assertTrue(seen[1].second.error!!.contains("card declined"))
        assertEquals("Compensated", seen[2].second.name)
    }

    @Test
    fun `a failed call is reported failed, with its error`() {
        val seen = mutableListOf<SagaCallOutcome>()
        val failing = saga("FAILING") { step("call") { execute { throw IOException("read timeout") } } }
        val engine = SagaEngine(
            transactions, repository, listOf(failing), KotlinxJsonSagaSerializer(), SagaConfig(maxAttempts = 1),
            tracer = { _, block -> block().also { seen += it } },
        )
        runBlocking { engine.run(transactions.inTransaction { engine.create(failing, "f:1") }, Clock.System.now() + 10.seconds) }
        assertEquals("Unknown", seen.single().name)
        assertTrue(seen.single().failed)
        assertTrue(seen.single().error!!.contains("read timeout"))
    }

    @Test
    fun `a tracer that throws before the call is bypassed, after it changes nothing`() {
        for (tracer in listOf<SagaTracer>(
            SagaTracer { _, _ -> error("tracer bug, before") },
            SagaTracer { _, block -> block(); error("tracer bug, after") },
        )) {
            holds = 0
            repository.restore(InMemorySagaRepository().snapshot())
            val id = engine(tracer).start()
            assertEquals(1, holds)                                                     // called once
            assertEquals(SagaState.FAILED, runBlocking { engine(tracer).find(id)!!.state })   // pay refused, hold undone
        }
    }

    @Test
    fun `a tracer that runs the call twice still makes it once`() {
        val engine = engine({ _, block -> block(); block() })
        engine.start()
        assertEquals(1, holds)
    }

    @Test
    fun `a call's history entry carries the trace id read during the call, in the tracer's span`() {
        val current = ThreadLocal<String?>()
        val engine = engine(
            tracer = { call, block ->
                current.set("trace-${call.step}-${call.action}")
                try {
                    block()
                } finally {
                    current.remove()
                }
            },
            traceId = { current.get() },
        )
        val id = engine.start()
        val history = runBlocking { engine.history(id) }
        assertEquals("trace-hold-EXECUTE", history.single { it.event == "EXECUTE:Done" }.traceId)
        assertEquals("trace-pay-EXECUTE", history.single { it.event == "EXECUTE:Rejected" }.traceId)
        assertEquals("trace-hold-COMPENSATE", history.single { it.event == "COMPENSATE:Compensated" }.traceId)
    }
}
