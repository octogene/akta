package dev.octogene.akta.console

import dev.octogene.akta.KotlinxJsonSagaSerializer
import dev.octogene.akta.SagaConfig
import dev.octogene.akta.SagaEngine
import dev.octogene.akta.SagaState
import dev.octogene.akta.StepNotSent
import dev.octogene.akta.memory.InMemorySagaRepository
import dev.octogene.akta.memory.InMemorySagaTransactions
import dev.octogene.akta.saga
import io.ktor.client.request.forms.FormDataContent
import io.ktor.client.request.get
import io.ktor.client.request.header
import io.ktor.client.request.post
import io.ktor.client.request.setBody
import io.ktor.client.statement.HttpResponse
import io.ktor.client.statement.bodyAsText
import io.ktor.http.HttpHeaders
import io.ktor.http.HttpStatusCode
import io.ktor.http.parameters
import io.ktor.server.application.ApplicationCall
import io.ktor.server.routing.route
import io.ktor.server.routing.routing
import io.ktor.server.testing.ApplicationTestBuilder
import io.ktor.server.testing.testApplication
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue
import kotlin.time.Clock
import kotlin.time.Duration.Companion.seconds
import kotlin.uuid.ExperimentalUuidApi
import kotlin.uuid.Uuid

@OptIn(ExperimentalUuidApi::class)
class SagaConsoleTest {

    private val repository = InMemorySagaRepository()
    private var down = true
    private var error = "stock unavailable"

    private val stock = saga("STOCK") {
        step<String>("hold") {
            execute { if (down) throw StepNotSent(error) else "STK-1" }
            compensate { }
            logAs { it }
        }
        step("confirm") { execute { } }
    }

    private val engine = SagaEngine(
        InMemorySagaTransactions(repository), repository, listOf(stock), KotlinxJsonSagaSerializer(), SagaConfig(maxAttempts = 1),
    )

    private fun stuck(key: String): Uuid = runBlocking {
        val id = InMemorySagaTransactions(repository).inTransaction { engine.create(stock, key) }
        engine.run(id, Clock.System.now() + 10.seconds)
        id
    }

    private fun console(actor: ((ApplicationCall) -> String?)? = { it.request.headers["X-Operator"] }, test: suspend ApplicationTestBuilder.() -> Unit) =
        testApplication {
            routing { route("/admin/console") { sagaConsole(engine, actor) } }
            test()
        }

    private suspend fun ApplicationTestBuilder.page(path: String, status: HttpStatusCode = HttpStatusCode.OK): String {
        val response = client.get("/admin/console$path")
        val text = response.bodyAsText()
        assertEquals(status, response.status, text)
        return text
    }

    private suspend fun ApplicationTestBuilder.act(
        path: String,
        form: Map<String, String> = emptyMap(),
        htmx: Boolean = true,
        operator: String? = "alice",
    ): HttpResponse = client.post("/admin/console$path") {
        if (htmx) header("HX-Request", "true")
        operator?.let { header("X-Operator", it) }
        setBody(FormDataContent(parameters { form.forEach { (k, v) -> append(k, v) } }))
    }

    @Test
    fun `the overview counts the stuck sagas by reason, and every page forbids inline code and framing`() = console {
        stuck("item-1")
        val response = client.get("/admin/console")
        val text = response.bodyAsText()
        assertTrue("TOO_MANY_ATTEMPTS" in text && "/admin/console/queue#TOO_MANY_ATTEMPTS" in text, text)
        val csp = response.headers["Content-Security-Policy"].orEmpty()
        assertTrue("script-src 'self'" in csp && "frame-ancestors 'none'" in csp, csp)
        assertEquals("no-store", response.headers[HttpHeaders.CacheControl])
    }

    @Test
    fun `the queue groups stuck sagas by reason, and search finds them by key or id`() = console {
        val id = stuck("item-2")
        val queue = page("/queue")
        assertTrue("""id="TOO_MANY_ATTEMPTS"""" in queue && "/admin/console/saga/$id" in queue, queue)

        assertTrue("/admin/console/saga/$id" in page("/search?q=item-2"))
        val byId = createClient { followRedirects = false }.get("/admin/console/search?q=$id")
        assertEquals(HttpStatusCode.Found, byId.status)
        assertEquals("/admin/console/saga/$id", byId.headers[HttpHeaders.Location])
        assertTrue("None found" in page("/search?q=nothing-here"))
    }

    @Test
    fun `a saga's page shows its steps, what happened, and only the actions it takes`() = console {
        val id = stuck("item-3")
        val text = page("/saga/$id")
        assertTrue("hold" in text && "confirm" in text && "current, 1 attempt(s)" in text, text)
        assertTrue("kept failing" in text && "Fix the cause, then retry" in text, text)              // the guidance
        assertTrue("""hx-post="/admin/console/saga/$id/retry"""" in text, text)
        assertTrue("""hx-post="/admin/console/saga/$id/mark-done"""" in text, text)
        // mark-done needs hold's String output: required, and asked for.
        assertTrue("Output, as JSON (String)" in text && """name="output" required="required"""" in text, text)
        // Refused ones are listed with why, not offered.
        assertFalse("""hx-post="/admin/console/saga/$id/retry-new-key"""" in text, text)
        assertTrue("sends no idempotency key" in text, text)
    }

    @Test
    fun `an action posted by htmx is taken, and answers the saga's block alone, with what happened`() = console {
        val id = stuck("item-4")
        down = false
        val response = act("/saga/$id/retry")
        val text = response.bodyAsText()
        assertEquals(HttpStatusCode.OK, response.status, text)
        assertTrue(text.startsWith("""<div id="saga">"""), text)                                    // swapped in place
        assertTrue("Retry: done, by alice." in text, text)
        assertEquals(SagaState.COMPLETED, runBlocking { engine.find(id)!!.state })
        assertEquals("alice", runBlocking { engine.history(id) }.single { it.event == "MANUAL_RETRY" }.actor)
    }

    @Test
    fun `an action the engine refuses says why, and changes nothing`() = console {
        val id = stuck("item-5")
        val text = act("/saga/$id/mark-done").bodyAsText()                                        // no output given
        assertTrue("Mark done refused" in text && "needs an output" in text, text)
        assertEquals(SagaState.RUNNING, runBlocking { engine.find(id)!!.state })
    }

    @Test
    fun `an action not sent by htmx, or without an operator, is refused`() = console {
        val id = stuck("item-6")
        down = false
        assertEquals(HttpStatusCode.Forbidden, act("/saga/$id/retry", htmx = false).status)        // a form from another site
        assertEquals(HttpStatusCode.Unauthorized, act("/saga/$id/retry", operator = null).status)
        assertEquals(SagaState.RUNNING, runBlocking { engine.find(id)!!.state })
    }

    @Test
    fun `read-only, the console offers no action and takes none`() = console(actor = null) {
        val id = stuck("item-7")
        val text = page("/saga/$id")
        assertFalse("hx-post" in text, text)
        assertFalse("What to do" in text, text)
        down = false
        assertTrue(act("/saga/$id/retry").status.value in setOf(404, 405))
        assertEquals(SagaState.RUNNING, runBlocking { engine.find(id)!!.state })
    }

    @Test
    fun `what a saga says is escaped, never markup`() = console {
        error = """<script>alert("x")</script>"""
        val id = stuck("item-<b>8</b>")
        val text = page("/saga/$id")
        assertFalse("<script>alert" in text, text)
        assertFalse("<b>8</b>" in text, text)
        assertTrue("&lt;script&gt;" in text, text)
    }

    @Test
    fun `htmx is served from the console, with the page's own stylesheet`() = console {
        val js = client.get("/admin/console/assets/$HTMX")
        assertEquals(HttpStatusCode.OK, js.status)
        assertTrue(js.headers[HttpHeaders.ContentType].orEmpty().contains("javascript"))
        assertTrue(js.bodyAsText().contains("htmx"))
        assertEquals(HttpStatusCode.OK, client.get("/admin/console/assets/console.css").status)
        assertEquals(HttpStatusCode.NotFound, client.get("/admin/console/assets/../../secret").status)
    }

    @Test
    fun `an unknown saga, or a malformed id, has a page that says so`() = console {
        assertTrue("No saga" in page("/saga/${Uuid.random()}", HttpStatusCode.NotFound))
        assertTrue("Not a saga id" in page("/saga/not-an-id", HttpStatusCode.BadRequest))
    }
}
