package dev.octogene.akta.ktor

import dev.octogene.akta.SagaEngine
import dev.octogene.akta.SagaRecord
import dev.octogene.akta.StuckCursor
import io.ktor.http.ContentType
import io.ktor.http.HttpStatusCode
import io.ktor.server.application.ApplicationCall
import io.ktor.server.request.receiveText
import io.ktor.server.response.respondText
import io.ktor.server.routing.Route
import io.ktor.server.routing.get
import io.ktor.server.routing.post
import kotlinx.serialization.SerializationException
import kotlinx.serialization.json.Json
import kotlin.time.Instant
import kotlin.uuid.ExperimentalUuidApi
import kotlin.uuid.Uuid

/*
 * The saga admin API, for an operators' screen or tool. Mount it in your own routes, inside your own
 * authentication: the module installs nothing on the application.
 *
 *   routing {
 *       authenticate("operators") {
 *           route("/admin/sagas") {
 *               sagaAdminRead(engine)
 *               authenticate("saga-admins") {          // a stricter role to change sagas
 *                   sagaAdminActions(engine, actor = { it.principal<UserIdPrincipal>()?.name })
 *               }
 *           }
 *       }
 *   }
 *
 * Bodies are JSON, written and read by the module itself: it does not need ContentNegotiation, and does
 * not depend on how the application configured it. Errors are {"error": "…"}: 400 for a malformed
 * request, 401 when [sagaAdminActions]' actor is null, 404 for an unknown saga, 409 when the engine
 * refuses the action in the saga's current state (the message says why).
 */

/** Largest page of stuck sagas, list of a key's sagas, or trace history, one request may ask for. */
public const val SAGA_ADMIN_MAX_LIMIT: Int = 500

private val json = Json {
    encodeDefaults = true
    explicitNulls = true
    ignoreUnknownKeys = false
}

/**
 * The read routes, which change nothing:
 *
 * - `GET /stats?type=` — counts and ages of the unfinished sagas, for all types or the given ones.
 * - `GET /stuck?type=&limit=&after=` — stuck sagas, stuck longest first; `after` is the `next` of the previous page.
 * - `GET /by-key?key=&limit=` — the sagas of a business key, newest first: the unfinished one if any, then
 *   the finished ones still kept. The key is a query parameter, so that one holding a `/` needs no care.
 * - `GET /trace/{traceId}?limit=` — what the request with this trace id did to sagas.
 * - `GET /{id}` — one saga (without its input).
 * - `GET /{id}/history` — everything that happened to it, oldest first.
 */
@OptIn(ExperimentalUuidApi::class)
public fun Route.sagaAdminRead(engine: SagaEngine) {
    get("stats") {
        call.answer { engine.stats(call.types()).toView() }
    }
    get("stuck") {
        call.answer {
            val limit = call.limit(default = 50)
            val page = engine.findStuck(limit, call.types(), call.request.queryParameters["after"]?.let(::parseCursor))
            StuckPage(page.map { it.toView() }, next = page.takeIf { it.size == limit }?.last()?.let(::cursorOf))
        }
    }
    get("by-key") {
        call.answer {
            val key = call.request.queryParameters["key"]?.takeIf { it.isNotBlank() } ?: throw BadRequest("key is required")
            engine.findByKey(key, call.limit(default = 20)).map { it.toView() }
        }
    }
    get("trace/{traceId}") {
        call.answer { engine.historyByTrace(call.parameters["traceId"]!!, call.limit(default = 200)).map { it.toView() } }
    }
    get("{id}") {
        call.answer {
            val id = call.sagaId()
            (engine.find(id) ?: throw NoSuchElementException("Saga $id not found")).toView()
        }
    }
    get("{id}/history") {
        call.answer {
            val id = call.sagaId()
            if (engine.find(id) == null) throw NoSuchElementException("Saga $id not found")
            engine.history(id).map { it.toView() }
        }
    }
}

/**
 * The routes that change a saga, each logged in `saga_log` under the operator [actor] returns for the
 * call (from your authentication's principal, never from the request). A null actor answers 401.
 * Each answers with the saga after the action: the engine runs it on at once, for up to 10 seconds.
 *
 * - `POST /{id}/retry` `{"newKey": false}` — runs the stuck step (or compensation) again.
 * - `POST /{id}/mark-done` `{"output": …}` — the stuck step was found done by hand: go on.
 * - `POST /{id}/abort` `{"currentStepApplied": false, "output": …}` — give up, undoing what was done.
 * - `POST /{id}/mark-compensated` — the stuck compensation was done by hand: go on undoing.
 * - `POST /{id}/cancel` `{"reason": "…"}` — abandon an unfinished saga, compensating it.
 *
 * Each is the [SagaEngine] method of the same name: read its documentation before exposing it.
 */
@OptIn(ExperimentalUuidApi::class)
public fun Route.sagaAdminActions(engine: SagaEngine, actor: (ApplicationCall) -> String?) {
    post("{id}/retry") {
        call.action(actor) { id, who ->
            val body = call.body(RetryRequest())
            engine.retry(id, who, newKey = body.newKey)
        }
    }
    post("{id}/mark-done") {
        call.action(actor) { id, who ->
            val body = call.body(MarkDoneRequest())
            engine.markDone(id, body.output?.toString(), who)
        }
    }
    post("{id}/abort") {
        call.action(actor) { id, who ->
            val body = call.body<AbortRequest>(default = null)
            engine.abort(id, who, body.currentStepApplied, body.output?.toString())
        }
    }
    post("{id}/mark-compensated") {
        call.action(actor) { id, who -> engine.markCompensated(id, who) }
    }
    post("{id}/cancel") {
        call.action(actor) { id, who ->
            val body = call.body<CancelRequest>(default = null)
            if (body.reason.isBlank()) throw BadRequest("reason must not be blank")
            engine.cancel(id, body.reason, who)
        }
    }
}

// ---- Plumbing ----------------------------------------------------------------------------

/** A malformed request: 400. */
private class BadRequest(message: String, cause: Throwable? = null) : Exception(message, cause)

private class Unauthenticated : Exception("No operator for this request")

/** Runs [block] and answers its value as JSON, or the error it threw with its status. */
private suspend inline fun <reified T> ApplicationCall.answer(block: () -> T) {
    val (status, text) = try {
        HttpStatusCode.OK to json.encodeToString(block())
    } catch (e: BadRequest) {
        HttpStatusCode.BadRequest to errorBody(e.message)
    } catch (e: Unauthenticated) {
        HttpStatusCode.Unauthorized to errorBody(e.message)
    } catch (e: NoSuchElementException) {
        HttpStatusCode.NotFound to errorBody(e.message)
    } catch (e: IllegalArgumentException) {
        // The engine refuses the action in the saga's current state, or the output given for a step.
        HttpStatusCode.Conflict to errorBody(e.message)
    }
    respondText(text, ContentType.Application.Json, status)
}

private fun errorBody(message: String?) = json.encodeToString(ErrorView(message ?: "Error"))

@OptIn(ExperimentalUuidApi::class)
private suspend fun ApplicationCall.action(
    actor: (ApplicationCall) -> String?,
    block: suspend (Uuid, String) -> SagaRecord,
) = answer {
    val who = actor(this) ?: throw Unauthenticated()
    block(sagaId(), who).toView()
}

@OptIn(ExperimentalUuidApi::class)
private fun ApplicationCall.sagaId(): Uuid {
    val text = parameters["id"]!!
    return try {
        Uuid.parse(text)
    } catch (e: IllegalArgumentException) {
        throw BadRequest("Not a saga id: $text", e)
    }
}

private fun ApplicationCall.types(): List<String>? = request.queryParameters.getAll("type")?.takeIf { it.isNotEmpty() }

private fun ApplicationCall.limit(default: Int): Int {
    val text = request.queryParameters["limit"] ?: return default
    val limit = text.toIntOrNull()
    if (limit == null || limit !in 1..SAGA_ADMIN_MAX_LIMIT) throw BadRequest("limit must be between 1 and $SAGA_ADMIN_MAX_LIMIT")
    return limit
}

/** The request's JSON body; an empty body is [default], when the request has one. */
private suspend inline fun <reified T : Any> ApplicationCall.body(default: T?): T {
    val text = receiveText()
    if (text.isBlank()) return default ?: throw BadRequest("A JSON body is required")
    return try {
        json.decodeFromString<T>(text)
    } catch (e: SerializationException) {
        throw BadRequest("Invalid body: ${e.message}", e)
    } catch (e: IllegalArgumentException) {
        throw BadRequest("Invalid body: ${e.message}", e)
    }
}

/** The cursor after [record]: its stuckAt and id. Opaque to clients. */
@OptIn(ExperimentalUuidApi::class)
private fun cursorOf(record: SagaRecord): String = "${record.stuckAt}_${record.id}"

@OptIn(ExperimentalUuidApi::class)
private fun parseCursor(text: String): StuckCursor {
    val at = text.substringBefore('_')
    val id = text.substringAfter('_', missingDelimiterValue = "")
    return try {
        StuckCursor(Instant.parse(at), Uuid.parse(id))
    } catch (e: IllegalArgumentException) {
        throw BadRequest("Not a cursor: $text", e)
    }
}
