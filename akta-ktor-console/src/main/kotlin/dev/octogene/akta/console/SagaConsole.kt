package dev.octogene.akta.console

import dev.octogene.akta.ManualAction
import dev.octogene.akta.SagaEngine
import dev.octogene.akta.SagaRecord
import dev.octogene.akta.SagaStepProgress
import dev.octogene.akta.StuckCursor
import io.ktor.http.ContentType
import io.ktor.http.HttpStatusCode
import io.ktor.http.withCharset
import io.ktor.server.application.ApplicationCall
import io.ktor.server.request.path
import io.ktor.server.request.receiveParameters
import io.ktor.server.response.respondBytes
import io.ktor.server.response.respondRedirect
import io.ktor.server.response.respondText
import io.ktor.server.routing.Route
import io.ktor.server.routing.get
import io.ktor.server.routing.post
import kotlin.time.Instant
import kotlin.uuid.ExperimentalUuidApi
import kotlin.uuid.Uuid

/*
 * The operators' console: HTML pages, rendered on the server, with htmx for the actions. Mount it in your own
 * routes, inside your own authentication, as the admin API:
 *
 *   authenticate("operators") {
 *       route("/admin/console") {
 *           sagaConsole(engine, actor = { it.principal<UserIdPrincipal>()?.name })
 *       }
 *   }
 *
 * Pages: the overview (`/`), the stuck sagas (`/queue`), a search by business key, saga id or trace id (`/search`),
 * and a saga (`/saga/{id}`): its steps, what happened, what to do, its history. htmx (bundled, served from
 * `/assets`) posts each action and swaps the saga's block with its new state.
 *
 * Security: every page is sent with a Content-Security-Policy that allows no inline script or style, nothing from
 * another origin, and no framing. An action is only taken from a request htmx sent (it carries `HX-Request`, which
 * another site cannot set on a cross-origin form): a form posted from elsewhere answers 403. The operator comes from
 * [actor], from your login, never from the request.
 */

private const val CSP = "default-src 'none'; script-src 'self'; style-src 'self'; img-src 'self'; connect-src 'self'; " +
    "form-action 'self'; frame-ancestors 'none'; base-uri 'none'"

private const val QUEUE_PAGE = 50

/**
 * The console's routes. With [actor] null, it is read-only: no action is offered, and none can be posted. Otherwise
 * [actor] names the operator of each action, from your authentication's principal: null answers 401.
 */
@OptIn(ExperimentalUuidApi::class)
public fun Route.sagaConsole(engine: SagaEngine, actor: ((ApplicationCall) -> String?)? = null) {
    // The overview, with or without a trailing slash: Ktor tells the two apart.
    val overview: suspend io.ktor.server.routing.RoutingContext.() -> Unit = {
        val base = call.base(depth = 0)
        val stats = engine.stats()
        call.html(page(base, "Sagas") { overview(base, stats, now()) })
    }
    get(overview)
    get("/", overview)
    get("queue") {
        val base = call.base(depth = 1)
        val after = call.request.queryParameters["after"]?.let { parseCursor(it) ?: return@get call.badRequest(base, "Not a page cursor: $it") }
        val sagas = engine.findStuck(QUEUE_PAGE, after = after)
        val next = sagas.takeIf { it.size == QUEUE_PAGE }?.last()?.let { "${it.stuckAt}_${it.id}" }
        call.html(page(base, "Stuck sagas") { queue(base, sagas, next, now()) })
    }
    get("search") {
        val base = call.base(depth = 1)
        val query = call.request.queryParameters["q"]?.trim().orEmpty()
        if (query.isEmpty()) return@get call.respondRedirect(base)
        val id = try {
            Uuid.parse(query)
        } catch (_: IllegalArgumentException) {
            null
        }
        if (id != null && engine.find(id) != null) return@get call.respondRedirect("$base/saga/$id")
        val byKey = engine.findByKey(query, limit = 50)
        val byTrace = engine.historyByTrace(query, limit = 200).map { it.sagaId }.distinct()
            .filter { traced -> byKey.none { it.id == traced } }.mapNotNull { engine.find(it) }
        call.html(page(base, "Search") { searchResults(base, query, byKey + byTrace, now()) })
    }
    get("saga/{id}") {
        val base = call.base(depth = 2)
        val id = call.sagaId() ?: return@get call.badRequest(base, "Not a saga id: ${call.parameters["id"]}")
        val panel = engine.panel(id) ?: return@get call.notFound(base, id)
        call.html(page(base, "${panel.saga.type} ${panel.saga.key}") { sagaPanel(base, panel, canAct = actor != null, flash = null, now()) })
    }
    get("assets/{file}") {
        val file = call.parameters["file"]
        val asset = ASSETS[file] ?: return@get call.respondText("Not found", status = HttpStatusCode.NotFound)
        call.response.headers.append("Cache-Control", asset.cache)
        call.response.headers.append("X-Content-Type-Options", "nosniff")
        call.respondBytes(asset.bytes, asset.type)
    }
    if (actor == null) return
    post("saga/{id}/{action}") {
        val base = call.base(depth = 3)
        // A form another site posts cannot carry this header: it takes htmx, from a page of this origin.
        if (call.request.headers["HX-Request"] != "true") {
            return@post call.respondText("Actions are taken from the console's pages only", status = HttpStatusCode.Forbidden)
        }
        val who = actor(call) ?: return@post call.respondText("Not signed in", status = HttpStatusCode.Unauthorized)
        val id = call.sagaId() ?: return@post call.respondText("Not a saga id", status = HttpStatusCode.BadRequest)
        val action = actionOf(call.parameters["action"].orEmpty())
            ?: return@post call.respondText("No such action", status = HttpStatusCode.NotFound)
        val form = call.receiveParameters()
        val output = form["output"]?.trim()?.takeIf { it.isNotEmpty() }
        val flash = try {
            when (action) {
                ManualAction.RETRY -> engine.retry(id, who)
                ManualAction.RETRY_WITH_NEW_KEY -> engine.retry(id, who, newKey = true)
                ManualAction.MARK_DONE -> engine.markDone(id, output, who)
                ManualAction.ABORT -> engine.abort(id, who, currentStepApplied = false)
                ManualAction.ABORT_CURRENT_STEP_APPLIED -> engine.abort(id, who, currentStepApplied = true, output = output)
                ManualAction.MARK_COMPENSATED -> engine.markCompensated(id, who)
                ManualAction.CANCEL -> {
                    val reason = form["reason"]?.trim().orEmpty()
                    require(reason.isNotEmpty()) { "A cancellation needs a reason" }
                    engine.cancel(id, reason, who)
                }
            }
            Flash(ok = true, "${action.label}: done, by $who.")
        } catch (e: IllegalArgumentException) {
            Flash(ok = false, "${action.label} refused: ${e.message}")         // the saga moved on, or the output does not fit
        } catch (_: NoSuchElementException) {
            return@post call.respondText("Saga $id not found", status = HttpStatusCode.NotFound)
        }
        val panel = engine.panel(id) ?: return@post call.respondText("Saga $id not found", status = HttpStatusCode.NotFound)
        call.html(sagaFragment(base, panel, canAct = true, flash, now()))
    }
}

/** Everything a saga's page shows, or null if there is no such saga. */
@OptIn(ExperimentalUuidApi::class)
private suspend fun SagaEngine.panel(id: Uuid): SagaPanel? {
    val saga: SagaRecord = find(id) ?: return null
    val steps: List<SagaStepProgress>? = try {
        steps(id)
    } catch (_: IllegalArgumentException) {
        null                                                            // a type this engine does not know
    }
    return SagaPanel(saga, steps, actions(saga), history(id))
}

/** The console's root path: this request's path without its last [depth] segments, the console's own. */
private fun ApplicationCall.base(depth: Int): String {
    val segments = request.path().trimEnd('/').split('/')
    return segments.dropLast(depth).joinToString("/")
}

@OptIn(ExperimentalUuidApi::class)
private fun ApplicationCall.sagaId(): Uuid? = try {
    Uuid.parse(parameters["id"].orEmpty())
} catch (_: IllegalArgumentException) {
    null
}

@OptIn(ExperimentalUuidApi::class)
private fun parseCursor(text: String): StuckCursor? = try {
    StuckCursor(Instant.parse(text.substringBefore('_')), Uuid.parse(text.substringAfter('_')))
} catch (_: IllegalArgumentException) {
    null
}

private suspend fun ApplicationCall.html(text: String, status: HttpStatusCode = HttpStatusCode.OK) {
    response.headers.append("Content-Security-Policy", CSP)
    response.headers.append("X-Content-Type-Options", "nosniff")
    response.headers.append("Referrer-Policy", "same-origin")
    response.headers.append("Cache-Control", "no-store")
    respondText(text, ContentType.Text.Html.withCharset(Charsets.UTF_8), status)
}

private suspend fun ApplicationCall.badRequest(base: String, text: String) =
    html(page(base, "Bad request") { problem("Bad request", text) }, HttpStatusCode.BadRequest)

@OptIn(ExperimentalUuidApi::class)
private suspend fun ApplicationCall.notFound(base: String, id: Uuid) =
    html(page(base, "Not found") { problem("Not found", "No saga $id: it may have been purged.") }, HttpStatusCode.NotFound)

private class Asset(val bytes: ByteArray, val type: ContentType, val cache: String)

private val ASSETS: Map<String, Asset> by lazy {
    fun read(name: String) = checkNotNull(Asset::class.java.getResourceAsStream("/dev/octogene/akta/console/$name")) { "Missing $name" }
        .use { it.readBytes() }
    mapOf(
        // Its name carries its version: it never changes under that name.
        HTMX to Asset(read(HTMX), ContentType.Application.JavaScript, "public, max-age=31536000, immutable"),
        "console.css" to Asset(read("console.css"), ContentType.Text.CSS, "public, max-age=3600"),
    )
}
