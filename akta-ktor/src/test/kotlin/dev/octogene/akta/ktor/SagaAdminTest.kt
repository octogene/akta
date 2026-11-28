package dev.octogene.akta.ktor

import dev.octogene.akta.KotlinxJsonSagaSerializer
import dev.octogene.akta.SagaConfig
import dev.octogene.akta.SagaDefinition
import dev.octogene.akta.SagaEngine
import dev.octogene.akta.SagaEvents
import dev.octogene.akta.StepNotSent
import dev.octogene.akta.StepRejected
import dev.octogene.akta.memory.InMemorySagaRepository
import dev.octogene.akta.memory.InMemorySagaTransactions
import dev.octogene.akta.saga
import io.ktor.client.request.get
import io.ktor.client.request.header
import io.ktor.client.request.post
import io.ktor.client.request.setBody
import io.ktor.client.statement.HttpResponse
import io.ktor.client.statement.bodyAsText
import io.ktor.http.HttpStatusCode
import io.ktor.server.routing.route
import io.ktor.server.routing.routing
import io.ktor.server.testing.ApplicationTestBuilder
import io.ktor.server.testing.testApplication
import kotlinx.coroutines.runBlocking
import kotlinx.serialization.Serializable
import kotlinx.serialization.json.Json
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue
import kotlin.time.Clock
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds
import kotlin.uuid.ExperimentalUuidApi
import kotlin.uuid.Uuid

@OptIn(ExperimentalUuidApi::class)
class SagaAdminTest {

    @Serializable
    data class Request(val customerId: String)

    private val repository = InMemorySagaRepository()
    private val json = Json { ignoreUnknownKeys = true }

    /** Calls that fail until [down] is false: the saga gets stuck after one attempt (maxAttempts = 1). */
    private var down = true
    private val released = mutableListOf<String?>()

    private val stock = saga<Request>("STOCK") {
        step<String>("hold") {
            execute { if (down) throw StepNotSent("stock unavailable") else "STK-1" }
            compensate { own -> released += own }
        }
    }

    private val booking = saga("BOOKING") {
        step<String>("hold") {
            execute { "STK-2" }
            compensate { if (down) throw RuntimeException("stock unavailable") }
        }
        step("book") { execute { throw StepRejected("no slot") } }
    }

    /** An approval that times out at once, and has no check: stuck, for a human to deliver the answer. */
    private val approval = saga("APPROVAL") {
        step<String>("hold") {
            execute { "STK-3" }
            compensate { own -> released += own }
        }
        awaitSignal<String>("await-approval", signal = "approval", timeout = 1.milliseconds) {
            onSignal { if (it != "yes") throw StepRejected("not approved: $it") }
        }
    }

    private val engine = SagaEngine(
        InMemorySagaTransactions(repository), repository, listOf(stock, booking, approval), KotlinxJsonSagaSerializer(),
        SagaConfig(maxAttempts = 1),
    )

    private fun <I> start(definition: SagaDefinition<I>, key: String, input: I): Uuid = runBlocking {
        val id = InMemorySagaTransactions(repository).inTransaction { engine.create(definition, key, input) }
        engine.run(id, Clock.System.now() + 10.seconds)
        id
    }

    private fun stuckStock(key: String) = start(stock, key, Request("customer-42"))

    /** The API as an application mounts it, with the operator taken from a header for the tests. */
    private fun api(test: suspend ApplicationTestBuilder.() -> Unit) = testApplication {
        routing {
            route("/admin/sagas") {
                sagaAdminRead(engine)
                sagaAdminActions(engine, actor = { it.request.headers["X-Operator"] })
            }
        }
        test()
    }

    private suspend fun ApplicationTestBuilder.post(path: String, body: String = "", operator: String? = "alice") =
        client.post("/admin/sagas$path") {
            operator?.let { header("X-Operator", it) }
            setBody(body)
        }

    private suspend inline fun <reified T> HttpResponse.read(status: HttpStatusCode = HttpStatusCode.OK): T {
        val text = bodyAsText()
        assertEquals(status, this.status, text)
        return json.decodeFromString(text)
    }

    // ---- Reads ------------------------------------------------------------------------------

    @Test
    fun `a saga says which actions it takes now, and why it refuses the others`() = api {
        val id = stuckStock("item-actions")
        val saga = client.get("/admin/sagas/$id").read<SagaView>()
        val allowed = saga.actions.filter { it.allowed }.map { it.action }
        assertEquals(listOf("RETRY", "MARK_DONE", "ABORT", "ABORT_CURRENT_STEP_APPLIED", "CANCEL"), allowed)
        val newKey = saga.actions.single { it.action == "RETRY_WITH_NEW_KEY" }
        assertTrue(newKey.refusal!!.contains("sends no idempotency key"), newKey.refusal)

        // The answer to an action carries them too: once aborted, nothing is left to do.
        val aborted = post("/$id/abort", """{"currentStepApplied": false}""").read<SagaView>()
        assertTrue(aborted.actions.none { it.allowed }, aborted.actions.toString())
        // And so do the lists: the queue, a key's sagas.
        val stuck = client.get("/admin/sagas/stuck").read<StuckPage>()
        assertTrue(stuck.sagas.all { it.actions.any { action -> action.allowed } })
    }

    @Test
    fun `one saga comes with its steps, a list without`() = api {
        val id = start(booking, "item-steps", Unit)                       // hold's undo stuck, book refused
        val saga = client.get("/admin/sagas/$id").read<SagaView>()
        assertEquals(listOf("hold" to "CURRENT", "book" to "NOT_APPLIED"), saga.steps!!.map { it.name to it.progress })

        val stuck = client.get("/admin/sagas/stuck").read<StuckPage>()
        assertTrue(stuck.sagas.isNotEmpty() && stuck.sagas.all { it.steps == null })
        // The answer to an action carries them: the compensation done by hand, the step is undone.
        down = false
        val after = post("/$id/mark-compensated").read<SagaView>()
        assertEquals(listOf("hold" to "UNDONE", "book" to "NOT_APPLIED"), after.steps!!.map { it.name to it.progress })
    }

    @Test
    fun `stats count the unfinished sagas, for all types or some`() = api {
        stuckStock("item-1")
        stuckStock("item-2")
        start(booking, "item-3", Unit)                                 // a compensation stuck

        val all = client.get("/admin/sagas/stats").read<StatsView>()
        assertEquals(
            listOf(StatusCount("COMPENSATING", "STUCK", 1), StatusCount("RUNNING", "STUCK", 2)),
            all.unfinished,
        )
        assertEquals(mapOf("TOO_MANY_ATTEMPTS" to 3L), all.stuckByReason)

        val ofStock = client.get("/admin/sagas/stats?type=STOCK").read<StatsView>()
        assertEquals(listOf(StatusCount("RUNNING", "STUCK", 2)), ofStock.unfinished)
    }

    @Test
    fun `stuck sagas come by page, stuck longest first`() = api {
        val ids = (1..5).map { stuckStock("item-$it") }

        val seen = mutableListOf<String>()
        var after: String? = null
        do {
            val page = client.get("/admin/sagas/stuck?limit=2" + (after?.let { "&after=$it" } ?: "")).read<StuckPage>()
            seen += page.sagas.map { it.id }
            after = page.next
        } while (after != null)

        assertEquals(ids.map { it.toString() }, seen)
        assertEquals(400, client.get("/admin/sagas/stuck?limit=0").status.value)
        assertEquals(400, client.get("/admin/sagas/stuck?after=nonsense").status.value)
    }

    @Test
    fun `a saga is shown without its input, with its history`() = api {
        val id = stuckStock("item-1")

        val response = client.get("/admin/sagas/$id")
        val text = response.bodyAsText()
        val saga = response.read<SagaView>()
        assertEquals("STUCK", saga.stepStatus)
        assertEquals("TOO_MANY_ATTEMPTS", saga.stuckReason)
        assertTrue("customer-42" !in text, text)                       // the input never leaves the database

        val history = client.get("/admin/sagas/$id/history").read<List<LogEntryView>>()
        assertEquals(listOf(SagaEvents.CREATED, "EXECUTE:NotSent"), history.map { it.event })
    }

    @Test
    fun `the sagas of a business key come newest first, the unfinished one included`() = api {
        down = false
        val done = stuckStock("item/1")                                   // completed; a key holding a slash
        down = true
        val stuck = stuckStock("item/1")                                  // unfinished, holds the key now
        stuckStock("item/2")

        val response = client.get("/admin/sagas/by-key") { url.parameters.append("key", "item/1") }
        val text = response.bodyAsText()
        val sagas = response.read<List<SagaView>>()

        assertEquals(listOf(stuck.toString(), done.toString()), sagas.map { it.id })
        assertTrue("customer-42" !in text, text)                         // still no input
        assertEquals(1, client.get("/admin/sagas/by-key?key=item%2F1&limit=1").read<List<SagaView>>().size)
        assertEquals(emptyList(), client.get("/admin/sagas/by-key?key=unknown").read<List<SagaView>>())
        assertEquals(HttpStatusCode.BadRequest, client.get("/admin/sagas/by-key").status)
        assertEquals(HttpStatusCode.BadRequest, client.get("/admin/sagas/by-key?key=x&limit=0").status)
    }

    @Test
    fun `an unknown or malformed id answers 404 or 400`() = api {
        val unknown = Uuid.random()
        assertEquals("Saga $unknown not found", client.get("/admin/sagas/$unknown").read<ErrorView>(HttpStatusCode.NotFound).error)
        assertEquals(HttpStatusCode.NotFound, client.get("/admin/sagas/$unknown/history").status)
        assertEquals(HttpStatusCode.BadRequest, client.get("/admin/sagas/not-a-uuid").status)
        assertEquals(HttpStatusCode.NotFound, post("/$unknown/retry").status)
    }

    // ---- Actions ----------------------------------------------------------------------------

    @Test
    fun `retry runs the stuck step again, logged under the operator`() = api {
        val id = stuckStock("item-1")
        down = false

        val saga = post("/$id/retry").read<SagaView>()

        assertEquals("COMPLETED", saga.state)
        val retry = engine.history(id).single { it.event == SagaEvents.MANUAL_RETRY }
        assertEquals("alice", retry.actor)
    }

    @Test
    fun `without an operator, an action answers 401 and changes nothing`() = api {
        val id = stuckStock("item-1")

        assertEquals(HttpStatusCode.Unauthorized, post("/$id/retry", operator = null).status)
        assertEquals("STUCK", client.get("/admin/sagas/$id").read<SagaView>().stepStatus)
    }

    @Test
    fun `markDone takes the step's output as a JSON value`() = api {
        val id = stuckStock("item-1")

        val saga = post("/$id/mark-done", """{"output": "STK-9"}""").read<SagaView>()

        assertEquals("COMPLETED", saga.state)
        assertEquals("\"STK-9\"", repository.results(id)["hold"])
    }

    @Test
    fun `markDone on a signal step delivers its value, which onSignal may refuse`() = api {
        val id = start(approval, "item-1", Unit)
        Thread.sleep(5)                                                     // past the timeout
        runBlocking { engine.resumeDue() }                                  // no check: it cannot tell
        assertEquals("STUCK", client.get("/admin/sagas/$id").read<SagaView>().stepStatus)

        val saga = post("/$id/mark-done", """{"output": "no"}""").read<SagaView>()

        assertEquals("FAILED", saga.state)                                  // refused, not marked done
        assertTrue("not approved: no" in saga.failureReason.orEmpty(), saga.failureReason)
        assertEquals(listOf<String?>("STK-3"), released)
    }

    @Test
    fun `an action the saga's state does not allow answers 409 with the engine's reason`() = api {
        val id = stuckStock("item-1")
        post("/$id/mark-done", """{"output": "STK-9"}""")                // now COMPLETED

        val error = post("/$id/mark-done", """{"output": "STK-9"}""").read<ErrorView>(HttpStatusCode.Conflict)
        assertEquals("Saga $id has no stuck forward step", error.error)

        val other = stuckStock("item-2")
        val wrongType = post("/$other/mark-done", """{"output": {"id": 1}}""").read<ErrorView>(HttpStatusCode.Conflict)
        assertTrue(wrongType.error.startsWith("Not a valid output of step 'hold'"), wrongType.error)
    }

    @Test
    fun `a malformed body answers 400`() = api {
        val id = stuckStock("item-1")

        assertEquals(HttpStatusCode.BadRequest, post("/$id/mark-done", "{not json").status)
        assertEquals(HttpStatusCode.BadRequest, post("/$id/abort", """{"output": "x"}""").status)   // currentStepApplied missing
        assertEquals(HttpStatusCode.BadRequest, post("/$id/abort").status)                         // no body
        assertEquals(HttpStatusCode.BadRequest, post("/$id/cancel", """{"reason": " "}""").status)
        assertEquals("STUCK", client.get("/admin/sagas/$id").read<SagaView>().stepStatus)
    }

    @Test
    fun `abort gives up, with the operator as the failure reason`() = api {
        val id = stuckStock("item-1")

        val saga = post("/$id/abort", """{"currentStepApplied": true, "output": "STK-7"}""").read<SagaView>()

        assertEquals("FAILED", saga.state)
        assertEquals("Aborted by alice", saga.failureReason)
        assertEquals(listOf<String?>("STK-7"), released)               // its compensation received that output
    }

    @Test
    fun `markCompensated goes on undoing after a stuck compensation`() = api {
        val id = start(booking, "item-1", Unit)
        assertEquals("COMPENSATING", client.get("/admin/sagas/$id").read<SagaView>().state)

        val saga = post("/$id/mark-compensated").read<SagaView>()

        assertEquals("FAILED", saga.state)
        assertEquals("dev.octogene.akta.StepRejected: no slot", saga.failureReason)
    }

    @Test
    fun `cancel keeps the reason, and a stuck saga compensates once retried`() = api {
        val id = stuckStock("item-1")

        val cancelled = post("/$id/cancel", """{"reason": "customer left"}""").read<SagaView>()
        assertEquals("customer left", cancelled.cancelReason)
        assertEquals("STUCK", cancelled.stepStatus)                    // a human decides first

        val saga = post("/$id/retry").read<SagaView>()
        assertEquals("FAILED", saga.state)
        assertEquals("Cancelled: customer left", saga.failureReason)

        val finished = post("/$id/cancel", """{"reason": "again"}""").read<ErrorView>(HttpStatusCode.Conflict)
        assertEquals("Saga $id is finished: undoing it is a new saga", finished.error)
    }

    @Test
    fun `the read routes can be mounted without the actions`() = testApplication {
        routing { route("/admin/sagas") { sagaAdminRead(engine) } }
        val id = stuckStock("item-1")

        assertEquals(HttpStatusCode.OK, client.get("/admin/sagas/$id").status)
        assertEquals(HttpStatusCode.NotFound, client.post("/admin/sagas/$id/retry").status)
    }
}
