package dev.octogene.akta.console

import dev.octogene.akta.CheckResult
import dev.octogene.akta.KotlinxJsonSagaSerializer
import dev.octogene.akta.SagaConfig
import dev.octogene.akta.SagaDefinition
import dev.octogene.akta.SagaEngine
import dev.octogene.akta.StepNotSent
import dev.octogene.akta.StepRejected
import dev.octogene.akta.memory.InMemorySagaRepository
import dev.octogene.akta.memory.InMemorySagaTransactions
import dev.octogene.akta.saga
import io.ktor.server.cio.CIO
import io.ktor.server.engine.embeddedServer
import io.ktor.server.response.respondRedirect
import io.ktor.server.routing.get
import io.ktor.server.routing.route
import io.ktor.server.routing.routing
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import java.io.IOException
import java.util.concurrent.ConcurrentHashMap
import kotlin.time.Clock
import kotlin.time.Duration
import kotlin.time.Duration.Companion.hours
import kotlin.time.Duration.Companion.minutes
import kotlin.time.Duration.Companion.seconds

/*
 * A console to look at: an in-memory engine with sagas in every state an operator meets. Each external system fails
 * the first time a saga calls it, then works: retry and the other actions can be tried for real.
 *
 *   ./gradlew :akta-ktor-console:runDemo      then open http://127.0.0.1:8089/admin/console/
 */

/** Keys whose first call failed already: the next one succeeds. */
private val failedOnce = ConcurrentHashMap.newKeySet<String>()

private fun firstTime(what: String, key: String) = failedOnce.add("$what/$key")

private val reservation = saga("RESERVATION") {
    step<String>("hold-items") { execute { "H-881" }; compensate { }; logAs { it } }
    step<String>("create-payment") {
        execute { if (firstTime("payment", key)) throw IOException("read timeout after 30 s") else "P-123" }
        check { CheckResult.CannotTell }                                     // the provider's lookup is down
        compensate { }
        logAs { it }
    }
    step("confirm") { execute { } }
}

private val itemSetup = saga("ITEM_SETUP") {
    step<String>("register-item") {
        execute { if (firstTime("catalog", key)) throw StepNotSent("catalog unavailable: connection refused") else "ITM-42" }
        compensate { }
        logAs { it }
    }
    step("publish") { execute { } }
}

private val booking = saga("BOOKING") {
    step<String>("reserve-room") {
        execute { "R-12" }
        compensate { if (firstTime("release", key)) throw IOException("hotel API: 503 Service Unavailable") }
        logAs { it }
    }
    step("charge") { execute { throw StepRejected("card declined") } }
}

private val approval = saga("APPROVAL") {
    step<String>("request") { execute { "REQ-7" }; compensate { }; logAs { it } }
    // Two minutes: NO_SIGNAL two minutes after the start, and a retry leaves time to try mark done ("yes", or not).
    awaitSignal<String>("await-approval", signal = "approval", timeout = 2.minutes) {
        onSignal { if (it != "yes") throw StepRejected("not approved: $it") }
        logAs { it }
    }
}

private val export = saga("EXPORT") {
    step<String>("build-report") { execute { "x".repeat(20_000) } }           // over maxDataBytes
}

private val invoice = saga("INVOICE") {
    step<String>("issue") { execute { "INV-2026-118" }; compensate { }; logAs { it } }
    onCompleted { if (firstTime("ledger", key)) error("ledger table locked") }
}

private val subscription = saga("SUBSCRIPTION") {
    step<String>("create-account") { execute { "ACC-9" }; compensate { }; logAs { it } }
    awaitSignal<String>("await-confirmation", signal = "confirmed", timeout = 24.hours) { logAs { it } }
}

private val payout = saga("PAYOUT") {
    step<String>("send-transfer") {
        // The bank remembers keys for 36 s only: the replay after backoff comes too late.
        idempotent(keyRetention = 36.seconds)
        execute { throw IOException("bank: read timeout") }
        compensate { }
        logAs { it }
    }
}

public fun main() {
    val repository = InMemorySagaRepository()
    val transactions = InMemorySagaTransactions(repository)
    val definitions = listOf(reservation, itemSetup, booking, approval, export, invoice, subscription, payout)
    val engine = SagaEngine(
        transactions, repository, definitions, KotlinxJsonSagaSerializer(),
        SagaConfig(maxAttempts = 1, settleDelay = Duration.ZERO, checkRetryDelay = 5.seconds),
    )

    fun start(definition: SagaDefinition<Unit>, key: String) = runBlocking {
        val id = transactions.inTransaction { engine.create(definition, key) }
        engine.run(id, Clock.System.now() + 5.seconds)
    }
    start(reservation, "order:3034")                                         // CANNOT_TELL
    start(itemSetup, "item:3034257BF7194E4000001A85")                         // TOO_MANY_ATTEMPTS
    start(itemSetup, "item:3034257BF7194E4000001A86")                         // TOO_MANY_ATTEMPTS
    start(booking, "booking:77")                                              // a compensation stuck
    start(approval, "approval:5")                                             // NO_SIGNAL, after its 2-minute timeout
    start(export, "export:2026-11")                                           // OUTPUT_UNSTORABLE
    start(invoice, "invoice:118")                                             // HOOK_FAILED
    start(subscription, "customer:12")                                        // WAITING, for a day
    start(payout, "payout:31")                                                // KEY_EXPIRED, within a minute or so
    failedOnce.add("catalog/item:done")
    start(itemSetup, "item:done")                                             // COMPLETED

    // The recovery job, as an application runs it.
    CoroutineScope(Dispatchers.Default).launch {
        while (true) {
            delay(5.seconds)
            runCatching { engine.resumeDue() }
        }
    }

    println("The console: http://127.0.0.1:8089/admin/console")
    embeddedServer(CIO, host = "127.0.0.1", port = 8089) {
        routing {
            get("/") { call.respondRedirect("/admin/console") }
            route("/admin/console") { sagaConsole(engine, actor = { "demo-operator" }) }
        }
    }.start(wait = true)
}
