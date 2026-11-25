package dev.octogene.akta.exposed

import dev.octogene.akta.CheckResult
import dev.octogene.akta.SagaConfig
import dev.octogene.akta.SagaContext
import dev.octogene.akta.SagaDefinition
import dev.octogene.akta.SagaEngine
import dev.octogene.akta.SagaEvents
import dev.octogene.akta.SagaState
import dev.octogene.akta.SagaStep
import dev.octogene.akta.SignalResult
import dev.octogene.akta.SignalStep
import dev.octogene.akta.StepNotSent
import dev.octogene.akta.StepRejected
import dev.octogene.akta.KotlinxJsonSagaSerializer
import dev.octogene.akta.SagaTransactions
import dev.octogene.akta.SagaEvent
import dev.octogene.akta.SagaListener
import dev.octogene.akta.StuckCursor
import dev.octogene.akta.StuckReason
import dev.octogene.akta.Idempotency
import dev.octogene.akta.StepStatus
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.cancel
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.delay
import kotlinx.coroutines.isActive
import kotlinx.coroutines.job
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.sync.Semaphore
import kotlinx.coroutines.sync.withPermit
import kotlinx.coroutines.withContext
import kotlinx.coroutines.withTimeoutOrNull
import kotlinx.serialization.Serializable
import kotlinx.serialization.json.Json
import org.jetbrains.exposed.v1.core.eq
import org.jetbrains.exposed.v1.jdbc.selectAll
import org.jetbrains.exposed.v1.jdbc.transactions.TransactionManager
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import java.io.IOException
import java.sql.SQLException
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger
import kotlin.coroutines.AbstractCoroutineContextElement
import kotlin.coroutines.CoroutineContext
import kotlin.coroutines.cancellation.CancellationException
import kotlin.random.Random
import kotlin.reflect.typeOf
import kotlin.test.fail
import kotlin.time.Clock
import kotlin.time.Duration
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.minutes
import kotlin.time.Duration.Companion.seconds
import kotlin.uuid.Uuid

/**
 * Runs many sagas against PostgreSQL while everything that can go wrong does, at random: rejections,
 * requests not sent, lost requests and responses, calls outliving their lock, failing checks and
 * compensations, hooks that throw, and pods dying mid-call. Several pods resume sagas concurrently,
 * and an operator resolves stuck sagas by looking at the external systems, as a human would.
 * Some sagas wait for a provider's event (first, between two calls, or last), which a listener delivers early,
 * late, twice or never, and whose onSignal has bugs; some steps have nothing to undo; some refusals are only found by a check;
 * customers cancel sagas at random moments.
 *
 * The external systems are simulated and record the truth: what was really applied. At the end,
 * every invariant of the engine is checked against it (see [checkInvariants]).
 *
 * Run with `./gradlew stressTest` (Docker required). Options: -Dsaga.sagas=300 -Dsaga.pods=3
 * -Dsaga.seed=42 -Dsaga.timeout=180 (seconds). Runs are concurrent, so a seed does not replay a
 * run exactly, but it reproduces the same mix of failures.
 */
@Tag("stress")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class SagaStressIT {
    private val sagaCount = System.getProperty("saga.sagas")?.toInt() ?: 300
    private val podCount = System.getProperty("saga.pods")?.toInt() ?: 3
    private val seed = System.getProperty("saga.seed")?.toLong() ?: Random.nextLong()
    private val timeout = (System.getProperty("saga.timeout")?.toLong() ?: 180).seconds

    /** Short delays so that lock expiry, settle delay and backoff all happen within the test. */
    private val config = SagaConfig(
        callLock = 1.seconds,
        settleDelay = 500.milliseconds,
        checkRetryDelay = 100.milliseconds,
        maxAttempts = 4,
        maxBackoff = 200.milliseconds,
        resumeFailureDelay = 300.milliseconds,
    )

    private val pg = PostgresFixture(durable = false)
    private val dice = Dice(seed)
    private val violations = ConcurrentHashMap.newKeySet<String>()
    private val crashes = AtomicInteger()
    private val manualActions = ConcurrentHashMap<String, AtomicInteger>()
    private val stuckReasons = ConcurrentHashMap<String, AtomicInteger>()
    /** Every event the listeners were told about, from every engine. */
    private val reported = java.util.concurrent.ConcurrentLinkedQueue<SagaEvent>()
    private val deliveries = ConcurrentHashMap<String, AtomicInteger>()
    private val cancellations = ConcurrentHashMap<String, AtomicInteger>()

    private fun ConcurrentHashMap<String, AtomicInteger>.count(key: String) =
        computeIfAbsent(key) { AtomicInteger() }.incrementAndGet()

    @BeforeAll
    fun start() {
        pg.start(poolSize = 40)
        pg.resetSchema(
            "CREATE TABLE item (key text PRIMARY KEY, status text NOT NULL, hook_runs int NOT NULL DEFAULT 0)",
        )
    }

    @AfterAll
    fun stop() = pg.close()

    // ---- The simulated world --------------------------------------------------------------------

    /** Thread-safe seeded randomness. */
    private class Dice(seed: Long) {
        private val random = Random(seed)
        @Synchronized fun chance(p: Double) = random.nextDouble() < p
        @Synchronized fun millis(max: Int) = random.nextInt(max + 1).toLong()
        /** Picks a key with probability proportional to its weight. */
        @Synchronized fun <T> pick(weights: Map<T, Int>): T {
            var n = random.nextInt(weights.values.sum())
            for ((value, weight) in weights) { n -= weight; if (n < 0) return value }
            error("unreachable")
        }
    }

    /** A saga's input: the request, with a customer marked as personal data (it must never be logged). */
    @Serializable
    data class ItemRequest(val customer: String, val nonce: Long)

    /** What an external system returns for a call: a step's output. */
    @Serializable
    data class Effect(val ref: String, val customer: String)

    /**
     * An external system. It knows what is really applied, for each item. With [keys], it accepts an
     * idempotency key: a request with a key it remembers returns the first result, applying nothing.
     * Without [undoable], what it applied stays (a notification): its step has nothing to undo.
     */
    private inner class ExternalSystem(val name: String, val keys: Boolean = false, val undoable: Boolean = true) {
        private val refused = HashSet<String>()
        private val applied = HashMap<String, Effect>()
        private val remembered = HashMap<String, Effect>()
        private var counter = 0

        @Synchronized fun apply(key: String, request: ItemRequest, idempotencyKey: String? = null): Effect {
            if (keys) remembered[checkNotNull(idempotencyKey)]?.let { return it }
            applied[key]?.let { violations += "$name: applied a second time for $key (already ${it.ref}): duplicate" }
            return Effect("$name-${++counter}", request.customer).also {
                applied[key] = it
                refused -= key
                if (keys) remembered[idempotencyKey!!] = it
            }
        }
        @Synchronized fun remove(key: String) { applied.remove(key) }
        @Synchronized fun remembers(idempotencyKey: String) = keys && idempotencyKey in remembered
        @Synchronized fun lookup(key: String): Effect? = applied[key]
        /** A refusal the system keeps a record of: a check finds it. */
        @Synchronized fun refuse(key: String) { if (key !in applied) refused += key }
        @Synchronized fun refuses(key: String) = key in refused
    }

    /** The coroutine of a pod (or of a request): a crash cancels all of it. */
    private class Pod(val job: Job) : AbstractCoroutineContextElement(Pod) {
        companion object Key : CoroutineContext.Key<Pod>
    }

    /** The process dies: a fatal JVM error, which the engine never catches. Caught where the pod ends. */
    private class SimulatedCrash : VirtualMachineError("simulated crash: the pod died")

    /**
     * The pod dies during this call. The call dies with it: its step stays IN_PROGRESS until its lock
     * expires. The pod is cancelled too: its other calls in flight end and are recorded (the engine
     * finishes a call it started), and it starts nothing new.
     */
    private suspend fun crash(): Nothing {
        crashes.incrementAndGet()
        currentCoroutineContext()[Pod]!!.job.cancel()
        throw SimulatedCrash()
    }

    /** Runs [block] as a pod, in its own coroutine. A crash ends the pod, not the test. */
    private fun CoroutineScope.launchPod(block: suspend () -> Unit) = launch {
        withContext(Pod(coroutineContext.job)) {
            try {
                block()
            } catch (_: SimulatedCrash) {
                // The pod is gone; what it left IN_PROGRESS is for the others to recover.
            }
        }
    }

    private suspend fun pause() = delay(dice.millis(5))

    private enum class Call { OK, REJECTED, NOT_SENT, LOST_REQUEST, LOST_RESPONSE, LOST_REFUSAL, OUTLIVES_LOCK, CRASH, JVM_ERROR }
    private enum class Undo { OK, FAILS_BEFORE, FAILS_AFTER, CRASH }
    private enum class Lookup { TRUTH, FAILS, CANNOT_TELL, CRASH }

    private val callWeights = mapOf(
        Call.OK to 70, Call.REJECTED to 3, Call.NOT_SENT to 8, Call.LOST_REQUEST to 5,
        Call.LOST_RESPONSE to 6, Call.LOST_REFUSAL to 3, Call.OUTLIVES_LOCK to 3, Call.CRASH to 5, Call.JVM_ERROR to 2,
    )

    /** About 1 item in 25 per system has a lookup that always throws: its check can never answer. */
    private fun lookupBroken(system: ExternalSystem, key: String) = Math.floorMod("${system.name}/$key".hashCode(), 25) == 0
    private val undoWeights = mapOf(Undo.OK to 80, Undo.FAILS_BEFORE to 8, Undo.FAILS_AFTER to 7, Undo.CRASH to 5)
    private val lookupWeights = mapOf(Lookup.TRUTH to 80, Lookup.FAILS to 10, Lookup.CANNOT_TELL to 7, Lookup.CRASH to 3)
    private val hookFailure = 0.1
    private val onSignalFailure = 0.08

    /** A step calling [system]. It also checks that it sees the output of the previous step. */
    private inner class ChaosStep(val system: ExternalSystem, private val previous: ChaosStep?) : SagaStep<ItemRequest, Effect> {
        override val name = system.name
        override val outputType = typeOf<Effect>()
        // A short window (3 calls' locks): retries after crashes and slow calls sometimes outlive it.
        override val compensates = system.undoable
        override val idempotency = if (system.keys) Idempotency(keyRetention = 3.seconds) else null

        private fun apply(ctx: SagaContext<ItemRequest>) =
            system.apply(ctx.key, ctx.input, if (system.keys) ctx.idempotencyKey else null)
        override fun describe(output: Effect) = output.ref        // never the customer

        override suspend fun execute(ctx: SagaContext<ItemRequest>): Effect {
            previous?.let {
                val seen = ctx.result(it.ref)
                val truth = it.system.lookup(ctx.key)
                if (seen != truth) violations += "$name for ${ctx.key}: saw ${it.name}=$seen, but the system has $truth"
            }
            pause()
            return when (dice.pick(callWeights)) {
                Call.OK -> apply(ctx).also { pause() }
                // A service that remembers the key answers a replay with its first result: never a refusal.
                Call.REJECTED -> if (system.remembers(ctx.idempotencyKey)) apply(ctx) else throw StepRejected("refused")
                Call.NOT_SENT -> throw StepNotSent("connection refused")
                // A message PostgreSQL would refuse (U+0000), and a character cut in two by a truncation.
                Call.LOST_REQUEST -> throw IOException("read timeout (request lost, not applied) \u0000 " + "\uD83D\uDE00".repeat(1200))
                Call.LOST_RESPONSE -> { apply(ctx); pause(); throw IOException("read timeout (applied)") }
                // Refused, and recorded there; but the answer is lost. A check finds the refusal. With keys, a replay
                // is simply a new call.
                Call.LOST_REFUSAL -> {
                    if (!system.keys) system.refuse(ctx.key)
                    throw IOException("read timeout (refused)")
                }
                Call.OUTLIVES_LOCK -> apply(ctx).also { delay(config.callLock + 200.milliseconds) }
                Call.CRASH -> { if (dice.chance(0.5)) apply(ctx); crash() }
                // A JVM Error, not an Exception (a TODO() or an assertion in a client), applied or not.
                Call.JVM_ERROR -> { if (dice.chance(0.5)) apply(ctx); throw NotImplementedError("client bug") }
            }
        }

        override suspend fun compensate(ctx: SagaContext<ItemRequest>, own: Effect?) {
            pause()
            when (dice.pick(undoWeights)) {
                Undo.OK -> system.remove(ctx.key)
                Undo.FAILS_BEFORE -> throw IOException("undo failed (nothing done)")
                Undo.FAILS_AFTER -> { system.remove(ctx.key); throw IOException("undo failed (after removing)") }
                Undo.CRASH -> { if (dice.chance(0.5)) system.remove(ctx.key); crash() }
            }
        }

        override suspend fun check(ctx: SagaContext<ItemRequest>): CheckResult<Effect> {
            pause()
            if (lookupBroken(system, ctx.key)) throw IOException("lookup broken for ${ctx.key}")
            return when (dice.pick(lookupWeights)) {
                Lookup.TRUTH -> system.lookup(ctx.key)?.let { CheckResult.Done(it) }
                    ?: if (system.refuses(ctx.key)) throw StepRejected("found refused") else CheckResult.NotDone
                Lookup.FAILS -> throw IOException("lookup unavailable \u0000")
                Lookup.CANNOT_TELL -> CheckResult.CannotTell
                Lookup.CRASH -> crash()
            }
        }
    }

    /**
     * Steps calling one system each; each step checks the reference of the one before it. A name ending with `+keys`
     * is a system that accepts idempotency keys; with `+final`, one whose effect is not undone.
     */
    private fun chain(vararg systems: String) =
        systems.map { ExternalSystem(it.substringBefore('+'), keys = it.endsWith("+keys"), undoable = !it.endsWith("+final")) }
            .fold(emptyList<ChaosStep>()) { acc, s -> acc + ChaosStep(s, acc.lastOrNull()) }

    private val stepsByType = mapOf(
        "ITEM_SETUP" to chain("stock", "booking+keys", "promo"),     // booking: replayed, never checked
        "ITEM_EXIT" to chain("unstock", "archive"),      // the type an old pod does not know (rolling deploy)
        "ITEM_PAY" to chain("charge", "ship"),           // with a wait for the payment between them
        "ITEM_NOTIFY" to chain("notify+final", "label"), // a refusal of label: nothing to undo, then onFailed
        "ITEM_APPROVE" to chain("grant"),                // between two waits: the first and the last step are signals
    )

    /** A payment provider's event: the outcome of the payment of an item. */
    @Serializable
    data class PaymentEvent(val paymentId: String, val succeeded: Boolean, val customer: String)

    /** The provider of a kind of event: it decides each item once; its events say so (see [listener]). */
    private inner class EventProvider(private val prefix: String) {
        private val decided = HashMap<String, PaymentEvent>()
        private var counter = 0

        /** Decides the event of [key] the first time: null when it was already decided. */
        @Synchronized fun decide(key: String, customer: String): PaymentEvent? {
            if (key in decided) return null
            return PaymentEvent("$prefix-${++counter}", succeeded = dice.chance(0.85), customer).also { decided[key] = it }
        }
        @Synchronized fun lookup(key: String): PaymentEvent? = decided[key]
    }

    /**
     * Waits for an event. At its timeout, asks the provider (which fails like any lookup). The event of an item comes
     * once [ready]: when what it follows was done.
     */
    private inner class EventWait(override val signal: String, val ready: (key: String) -> Boolean) : SignalStep<ItemRequest, PaymentEvent> {
        val provider = EventProvider(signal)
        override val name = "await-$signal"
        override val timeout = 1500.milliseconds
        override val checksOnTimeout = true
        override val outputType = typeOf<PaymentEvent>()
        override fun describe(output: PaymentEvent) = output.paymentId       // never the customer

        override fun accept(ctx: SagaContext<ItemRequest>, value: PaymentEvent) {
            if (dice.chance(onSignalFailure)) error("onSignal failed for ${ctx.key}")
            if (!value.succeeded) throw StepRejected("$signal ${value.paymentId} declined")
        }

        override suspend fun check(ctx: SagaContext<ItemRequest>): CheckResult<PaymentEvent> {
            pause()
            return when (dice.pick(lookupWeights)) {
                Lookup.TRUTH -> provider.lookup(ctx.key)?.let { CheckResult.Done(it) } ?: CheckResult.NotDone
                Lookup.FAILS -> throw IOException("$signal lookup unavailable")
                Lookup.CANNOT_TELL -> CheckResult.CannotTell
                Lookup.CRASH -> crash()
            }
        }
    }

    private fun applied(type: String): (String) -> Boolean = { key -> stepsByType.getValue(type).first().system.lookup(key) != null }

    /** The waits of each saga type that has some. */
    private val waitsByType = mapOf(
        "ITEM_PAY" to listOf(EventWait("payment", ready = applied("ITEM_PAY"))),
        "ITEM_APPROVE" to listOf(EventWait("approval", ready = { true }), EventWait("receipt", ready = applied("ITEM_APPROVE"))),
    )

    /** The steps of a saga type: those calling systems, and its waits. */
    private fun stepsOf(type: String): List<SagaStep<ItemRequest, *>> {
        val calls = stepsByType.getValue(type)
        val waits = waitsByType[type].orEmpty()
        return when (type) {
            "ITEM_PAY" -> listOf(calls[0], waits[0], calls[1])
            "ITEM_APPROVE" -> listOf(waits[0], calls[0], waits[1])
            else -> calls
        }
    }

    /** The business change of the hooks, in the engine's transaction; throws sometimes, after writing. */
    private fun hook(key: String, status: String) {
        TransactionManager.current().exec(
            "UPDATE item SET status = '$status', hook_runs = hook_runs + 1 WHERE key = '$key'",
        )
        if (dice.chance(hookFailure)) error("hook failed for $key")
    }

    private val definitions = stepsByType.keys.associateWith { type ->
        SagaDefinition(type, typeOf<ItemRequest>(), stepsOf(type), onCompleted = { hook(key, "DONE") }, onFailed = { hook(key, "FAILED") })
    }

    private val serializer = KotlinxJsonSagaSerializer()

    /** An engine as deployed in one pod: [types] are the saga types its version knows. */
    private fun newEngine(types: Set<String> = stepsByType.keys) = SagaEngine(
        FlakyTransactions(ExposedSagaTransactions(pg.database)), ExposedSagaRepository(),
        definitions.filterKeys { it in types }.values.toList(), serializer, config,
        listeners = listOf(SagaListener { reported += it }),
    )

    /** A database failure injected by [FlakyTransactions]. */
    private class InjectedDatabaseFailure(message: String) : SQLException(message)

    /** A hook's failure, in a transaction it rolled back. */
    private fun Throwable.isHook() = this is IllegalStateException && message.orEmpty().startsWith("hook failed")

    private fun Throwable.isInjected() = generateSequence(this) { it.cause }.any { it is InjectedDatabaseFailure }

    private val dbFailure = 0.02
    private val dbFailures = ConcurrentHashMap<String, AtomicInteger>()

    /**
     * The database failing under the engine: a transaction is rolled back after doing its work, or
     * commits but reports an error (the connection drops before the acknowledgement).
     */
    private inner class FlakyTransactions(private val inner: SagaTransactions) : SagaTransactions {
        override suspend fun <T> inTransaction(block: () -> T): T {
            val result = inner.inTransaction {
                val result = block()
                if (dice.chance(dbFailure)) fail("connection lost: rolled back")
                result
            }
            if (dice.chance(dbFailure)) fail("connection lost after commit")
            return result
        }

        private fun fail(message: String): Nothing {
            dbFailures.computeIfAbsent(message) { AtomicInteger() }.incrementAndGet()
            throw InjectedDatabaseFailure(message)
        }
    }

    // ---- The run --------------------------------------------------------------------------------

    @Test
    fun `random failures never break the invariants`() {
        println("SagaStressIT: $sagaCount sagas, $podCount pods, seed $seed (rerun with -Dsaga.seed=$seed)")
        val started = System.nanoTime()
        val finished = runBlocking(Dispatchers.Default) {        // several threads, as on a real server
            coroutineScope {
                val requests = launch { requests() }
                // pod-1 runs the previous version, which does not know ITEM_EXIT yet.
                val workers = (1..podCount).map { n ->
                    val types = if (n == 1) setOf("ITEM_SETUP") else stepsByType.keys
                    launch { restarting("pod-$n") { pod(newEngine(types)) } }
                } +
                    launch { restarting("operator") { operator(newEngine()) } } +
                    launch { listener(newEngine()) }
                val done = withTimeoutOrNull(timeout) {
                    requests.join()
                    while (unfinished() > 0) delay(200)
                }
                workers.forEach { it.cancelAndJoin() }
                done != null
            }
        }
        val seconds = (System.nanoTime() - started) / 1e9
        printSummary(seconds)
        if (!finished) fail("Not every saga finished within $timeout (seed $seed):\n${stateHistogram()}")
        checkInvariants()
    }

    /** Runs [body] in a pod; when the pod crashes, starts a new one, until cancelled. */
    private suspend fun restarting(name: String, body: suspend () -> Unit) = coroutineScope {
        while (isActive) {
            launchPod { body() }.join()
        }
    }

    /** HTTP requests: create the item and its saga in one transaction, then run inline briefly. */
    private suspend fun requests() = coroutineScope {
        val engine = newEngine()
        val transactions = ExposedSagaTransactions(pg.database)    // the business transaction: not flaky
        val permits = Semaphore(16)
        repeat(sagaCount) { i ->
            permits.acquire()
            launch {
                try {
                    val type = dice.pick(mapOf("ITEM_SETUP" to 35, "ITEM_EXIT" to 15, "ITEM_PAY" to 20, "ITEM_NOTIFY" to 15, "ITEM_APPROVE" to 15))
                    val key = "$type-$i"
                    val id = transactions.inTransaction {
                        TransactionManager.current().exec("INSERT INTO item (key, status) VALUES ('$key', 'PENDING')")
                        engine.create(definitions.getValue(type), key, ItemRequest("PII-customer-$i", dice.millis(1_000_000)))
                    }
                    val request = launchPod {
                        try {
                            engine.run(id, Clock.System.now() + dice.millis(100).milliseconds)
                        } catch (e: CancellationException) {
                            throw e
                        } catch (e: Exception) {
                            if (!e.isInjected() && !e.isHook()) violations += "request for $key failed: $e"
                        }
                    }
                    request.join()
                    if (dice.chance(0.1)) launch { customerCancels(engine, id) }
                } finally {
                    permits.release()
                }
            }
        }
    }

    /** Runs [block] as a pod of its own: a crash ends [block] only. Errors other than injected ones are violations. */
    private suspend fun inOwnPod(what: String, block: suspend () -> Unit) = coroutineScope {
        launchPod {
            try {
                block()
            } catch (e: CancellationException) {
                throw e
            } catch (e: Exception) {
                if (!e.isInjected() && !e.isHook()) violations += "$what failed: $e"
            }
        }.join()
    }

    /**
     * The Kafka listener of the events. A provider decides an item's event once what it follows was done; the event
     * then arrives after a random delay (often after the step's timeout, sometimes before the step waits), twice, or
     * never.
     */
    private suspend fun listener(engine: SagaEngine): Unit = coroutineScope {
        while (true) {
            for ((type, waits) in waitsByType) {
                val running = pg.query("SELECT id, business_key, input FROM saga WHERE type = '$type' AND state = 'RUNNING'") {
                    Triple(Uuid.parse(it.getString(1)), it.getString(2), Json.decodeFromString<ItemRequest>(it.getString(3)))
                }
                for ((id, key, input) in running) for (wait in waits) {
                    if (!wait.ready(key)) continue                          // nothing charged: no payment
                    val event = wait.provider.decide(key, input.customer) ?: continue
                    if (dice.chance(0.1)) {
                        deliveries.count("lost")
                        continue
                    }
                    repeat(if (dice.chance(0.2)) 2 else 1) {
                        launch {
                            delay(dice.millis(2000))
                            deliver(engine, id, wait.signal, event)
                        }
                    }
                }
            }
            delay(30)
        }
    }

    /** Delivers one event: redelivered while the database fails, as Kafka does until the offset is committed. */
    private suspend fun deliver(engine: SagaEngine, id: Uuid, signal: String, event: PaymentEvent) {
        while (true) {
            val result = try {
                engine.signal(id, signal, event)
            } catch (e: CancellationException) {
                throw e
            } catch (e: Exception) {
                if (!e.isInjected()) return run { violations += "signal for saga $id failed: $e" }
                deliveries.count("failed (database)")
                delay(dice.millis(50))
                continue
            }
            deliveries.count(result::class.simpleName!!)
            if (result is SignalResult.Delivered && dice.chance(0.5)) {
                inOwnPod("run after the signal of saga $id") { engine.run(id, Clock.System.now() + 100.milliseconds) }
            }
            return
        }
    }

    /** A customer who cancels at a random moment, and tries again when it fails. */
    private suspend fun customerCancels(engine: SagaEngine, id: Uuid) {
        delay(dice.millis(3000))
        repeat(3) {
            var outcome = "crashed"
            inOwnPod("cancel of saga $id") {
                outcome = try {
                    engine.cancel(id, "customer cancelled", "customer")
                    "cancelled"
                } catch (e: IllegalArgumentException) {
                    if (!e.message.orEmpty().contains("is finished") && !e.message.orEmpty().contains("is finishing")) throw e
                    "too late"
                } catch (e: IllegalStateException) {
                    if (!e.isHook()) throw e
                    "failed (hook)"                                      // rolled back: try again
                } catch (e: Exception) {
                    if (!e.isInjected()) throw e
                    "failed (database)"
                }
            }
            cancellations.count(outcome)
            if (!outcome.startsWith("failed")) return
            delay(dice.millis(200))
        }
    }

    /** A pod's recovery job. */
    private suspend fun pod(engine: SagaEngine) {
        while (true) {
            try {
                val result = engine.resumeDue(limit = 20, budgetPerSaga = 5.seconds, concurrency = 8)
                // A hook may throw as a claim ends the saga (nothing left to undo): postponed, then RESUME_FAILING.
                result.failures.filterValues { !it.isInjected() && !it.isHook() }
                    .forEach { (id, e) -> violations += "resumeDue failed for saga $id: $e" }
            } catch (e: CancellationException) {
                throw e
            } catch (e: Exception) {
                // The batch query itself failed: a job runner logs it and tries again on its next tick.
                if (!e.isInjected()) violations += "resumeDue failed: $e"
            }
            delay(20)
        }
    }

    /** A human: looks at stuck sagas and resolves them from what the external systems really hold. */
    private suspend fun operator(engine: SagaEngine) {
        while (true) {
            // As an admin screen does: page through the stuck sagas.
            val stuck = try {
                generateSequence(engine.findStuck(limit = 20)) { page ->
                    if (page.size < 20) null else runBlocking { engine.findStuck(limit = 20, after = StuckCursor.after(page.last())) }
                }.flatten().toList()
            } catch (e: CancellationException) {
                throw e
            } catch (e: Exception) {
                if (!e.isInjected()) violations += "findStuck failed: $e"
                emptyList()
            }
            for (saga in stuck) {
                val (id, state, key) = Triple(saga.id, saga.state, saga.key)
                if (saga.stuckReason == null || saga.stuckAt == null) violations += "$key: stuck without a reason"
                stuckReasons.count(saga.stuckReason.toString())
                val step = definitions.getValue(saga.type).steps[saga.stepIndex]
                // The truth: what the step's system holds, or for a wait what its provider decided.
                val (truth, json) = when (step) {
                    is ChaosStep -> step.system.lookup(key).let { it to it?.let { e -> Json.encodeToString(e) } }
                    else -> (step as EventWait).provider.lookup(key).let { it to it?.let { e -> Json.encodeToString(e) } }
                }
                val action = when {
                    // Every step is done or undone: only the hook is left, which retry runs again.
                    state == SagaState.FINISHING -> "retry"
                    // An undo's key may be forgotten: effect gone → undone by hand; still there → a fresh key.
                    state == SagaState.COMPENSATING && saga.stuckReason == StuckReason.KEY_EXPIRED ->
                        if (truth == null) "markCompensated" else "retry(newKey)"
                    // A compensation stuck while its effect is already gone (removed, then the undo failed): done by hand.
                    state == SagaState.COMPENSATING -> if (truth == null && dice.chance(0.6)) "markCompensated" else "retry"
                    truth is PaymentEvent && !truth.succeeded -> "abort"   // declined: the saga gives up
                    // onSignal or a hook threw on a signal: kept, and used by retry once "fixed".
                    step is EventWait && saga.stuckReason == StuckReason.RECORD_FAILED && dice.chance(0.6) -> "retry"
                    // The key may be forgotten: not applied → a fresh key; applied → as any applied step, below.
                    saga.stuckReason == StuckReason.KEY_EXPIRED && truth == null -> "retry(newKey)"
                    truth != null -> if (dice.chance(0.7)) "markDone" else "abort(applied)"
                    else -> if (dice.chance(0.7)) "retry" else "abort"
                }
                manualActions.computeIfAbsent(action) { AtomicInteger() }.incrementAndGet()
                try {
                    when (action) {
                        "retry" -> engine.retry(id, "operator")
                        "retry(newKey)" -> engine.retry(id, "operator", newKey = true)
                        "markCompensated" -> engine.markCompensated(id, "operator")
                        "markDone" -> engine.markDone(id, json, "operator")
                        "abort(applied)" -> engine.abort(id, "operator", currentStepApplied = true, output = json)
                        else -> engine.abort(id, "operator", currentStepApplied = false)
                    }
                } catch (e: CancellationException) {
                    throw e
                } catch (e: Exception) {
                    // Expected: a hook threw inside the manual action (rolled back, still stuck: try again later),
                    // or an injected database failure. Anything else is an engine error.
                    val hook = e.isHook()
                    val expected = hook || e.isInjected()
                    val label = when { hook -> "failed (hook)"; e.isInjected() -> "failed (database)"; else -> "failed ($e)" }
                    manualActions.computeIfAbsent(label) { AtomicInteger() }.incrementAndGet()
                    if (!expected) violations += "$action on $key failed: $e"
                }
            }
            delay(100)
        }
    }

    private fun unfinished() = pg.long("SELECT count(*) FROM saga WHERE state IN ('RUNNING', 'COMPENSATING', 'FINISHING')") +
        (sagaCount - pg.long("SELECT count(*) FROM saga"))

    // ---- Invariants -----------------------------------------------------------------------------

    private class Row(
        val type: String, val key: String, val state: String, val stepStatus: String, val lockedUntil: Any?,
        val itemStatus: String, val hookRuns: Int, val input: ItemRequest, val cancelRequested: Boolean,
    )

    private fun checkInvariants() {
        val problems = violations.toMutableList()
        val rows = pg.query(
            """
            SELECT s.type, s.business_key, s.state, s.step_status, s.locked_until, i.status, i.hook_runs, s.input,
                   s.cancel_requested_at IS NOT NULL
            FROM saga s JOIN item i ON i.key = s.business_key
            """.trimIndent(),
        ) {
            Row(
                it.getString(1), it.getString(2), it.getString(3), it.getString(4), it.getObject(5), it.getString(6),
                it.getInt(7), Json.decodeFromString(it.getString(8)), it.getBoolean(9),
            )
        }
        // step results by key: step name -> (ref, compensated)
        val results = pg.query(
            """
            SELECT s.business_key, r.step_name, r.output, r.compensated_at IS NOT NULL
            FROM saga_step_result r JOIN saga s ON s.id = r.saga_id
            """.trimIndent(),
        ) { listOf(it.getString(1), it.getString(2), it.getString(3), it.getBoolean(4).toString()) }
            .groupBy({ it[0] }, { it[1] to (it[2] to it[3].toBoolean()) })
            .mapValues { (_, v) -> v.toMap() }

        if (rows.size != sagaCount) problems += "expected $sagaCount sagas, found ${rows.size}"
        for (r in rows) {
            val stored = results[r.key].orEmpty()
            val steps = stepsByType.getValue(r.type)
            if (r.lockedUntil != null || r.stepStatus == StepStatus.IN_PROGRESS.name) {
                problems += "${r.key}: finished with a lock (${r.stepStatus})"
            }
            if (r.hookRuns != 1) problems += "${r.key}: hook ran ${r.hookRuns} times, expected exactly once"
            if (r.cancelRequested && r.state != SagaState.FAILED.name) problems += "${r.key}: cancelled but ${r.state}"
            when (r.state) {
                SagaState.COMPLETED.name -> {
                    if (r.itemStatus != "DONE") problems += "${r.key}: COMPLETED but item is ${r.itemStatus}"
                    for (step in steps) {
                        val truth = step.system.lookup(r.key)
                        val (text, compensated) = stored[step.name] ?: (null to false)
                        val output = text?.let { Json.decodeFromString<Effect>(it) }
                        if (truth == null) problems += "${r.key}: COMPLETED but ${step.name} is not applied"
                        if (output != truth) problems += "${r.key}: stored ${step.name}=$output, system has $truth"
                        if (truth != null && truth.customer != r.input.customer) {
                            problems += "${r.key}: ${step.name} was applied for ${truth.customer}, the saga's input is ${r.input.customer}"
                        }
                        if (compensated) problems += "${r.key}: COMPLETED but ${step.name} is marked compensated"
                    }
                    for (wait in waitsByType[r.type].orEmpty()) {
                        val event = stored[wait.name]?.first?.let { Json.decodeFromString<PaymentEvent>(it) }
                        val truth = wait.provider.lookup(r.key)
                        if (event == null || event != truth || !event.succeeded) {
                            problems += "${r.key}: COMPLETED with ${wait.signal} $event, the provider has $truth"
                        }
                    }
                }
                SagaState.FAILED.name -> {
                    if (r.itemStatus != "FAILED") problems += "${r.key}: FAILED but item is ${r.itemStatus}"
                    for (step in steps.filter { it.system.undoable }) {
                        step.system.lookup(r.key)?.let { problems += "${r.key}: FAILED but ${step.name} is still applied ($it)" }
                    }
                    stored.filterValues { !it.second }.keys.forEach {
                        problems += "${r.key}: FAILED but the result of $it is not marked compensated"
                    }
                }
                else -> problems += "${r.key}: not finished (${r.state} / ${r.stepStatus})"
            }
        }

        // Inputs and outputs may hold personal data: none of it may reach the history or the errors.
        val leaks = pg.long("SELECT count(*) FROM saga_log WHERE detail LIKE '%PII-%'") +
            pg.long("SELECT count(*) FROM saga WHERE last_error LIKE '%PII-%'")
        if (leaks > 0) problems += "$leaks log entries or errors contain an input or output value (PII-)"

        val known = setOf(
            SagaEvents.CREATED, SagaEvents.LOCK_EXPIRED, SagaEvents.LATE_RESULT_IGNORED, SagaEvents.RECORD_FAILED,
            SagaEvents.RESUME_FAILED,
            SagaEvents.MANUAL_RETRY, SagaEvents.MANUAL_DONE, SagaEvents.MANUAL_ABORT, SagaEvents.MANUAL_COMPENSATED,
            SagaEvents.WAIT_STARTED, SagaEvents.WAIT_TIMED_OUT, SagaEvents.SIGNAL_RECEIVED, SagaEvents.CANCEL_REQUESTED,
            SagaEvents.KEY_EXPIRED, SagaEvents.FINISHED, SagaEvents.HOOK_FAILED, SagaEvents.CANCELLED, SagaEvents.NOT_CALLED, SagaEvents.NOTHING_TO_UNDO,
        )
        val outcome = Regex("(EXECUTE|CHECK|COMPENSATE):[A-Za-z]+")
        pg.query("SELECT DISTINCT event FROM saga_log") { it.getString(1) }
            .filterNot { it in known || outcome.matches(it) }
            .forEach { problems += "unknown event in saga_log: $it" }
        // The listeners were told only what was committed: each event is a row of saga_log, at most once.
        val logRows = pg.query("SELECT saga_id, at, step_index, event FROM saga_log") {
            val at = it.getObject(2, java.time.OffsetDateTime::class.java).toInstant()
            "${it.getString(1)}|${at.epochSecond * 1_000_000 + at.nano / 1_000}|${it.getInt(3)}|${it.getString(4)}"
        }.groupingBy { it }.eachCount()
        reported.map { e ->
            "${e.entry.sagaId}|${e.entry.at.epochSeconds * 1_000_000 + e.entry.at.nanosecondsOfSecond / 1_000}|${e.entry.stepIndex}|${e.entry.event}"
        }.groupingBy { it }.eachCount().forEach { (key, n) ->
            if (n > (logRows[key] ?: 0)) problems += "reported $n times, but ${logRows[key] ?: 0} rows in saga_log: $key"
        }
        if (reported.any { "PII-" in it.entry.detail.orEmpty() }) problems += "a reported event contains a value (PII-)"
        if (reported.any { it.entry.event == SagaEvents.CREATED }) problems += "a creation was reported"

        if (sagaCount >= 300) {
            // The shapes above must have been reached, or they prove nothing.
            mapOf(
                "a check that finds a refusal" to "event = 'CHECK:Rejected' AND detail LIKE '%found refused%'",
                "a step with nothing to undo" to "event = 'NOTHING_TO_UNDO' AND step_name = 'notify'",
                "a bug in onSignal" to "detail LIKE '%onSignal failed%'",
                "an error with a NUL" to "detail LIKE '%request lost, not applied%'",
            ).forEach { (what, where) ->
                if (pg.long("SELECT count(*) FROM saga_log WHERE $where") == 0L) problems += "never happened, a hole in the test: $what"
            }
        }

        val created = pg.long("SELECT count(*) FROM saga_log WHERE event = 'CREATED'")
        if (created != sagaCount.toLong()) problems += "expected $sagaCount CREATED events, found $created"

        // To investigate a failed run: the history of the first sagas that broke an invariant.
        problems.mapNotNull { Regex("^(ITEM_[A-Z]+-[0-9]+):").find(it)?.groupValues?.get(1) }.distinct().take(2).forEach { k ->
            println("History of $k:\n" + pg.query(
                "SELECT l.at, l.step_index, l.event, l.detail, l.actor FROM saga_log l JOIN saga s ON s.id = l.saga_id WHERE s.business_key = '$k' ORDER BY l.id",
            ) { "  ${it.getString(2)} ${it.getString(3)} ${it.getString(5) ?: ""} ${it.getString(4)?.take(150)}" }.joinToString("\n"))
        }
        if (problems.isNotEmpty()) {
            fail("${problems.size} invariant violations (seed $seed):\n" + problems.take(50).joinToString("\n"))
        }
    }

    // ---- Report ---------------------------------------------------------------------------------

    private fun stateHistogram() = pg.query(
        "SELECT state, step_status, count(*), max(last_error) FROM saga GROUP BY 1, 2 ORDER BY 3 DESC",
    ) { "  ${it.getString(1)} / ${it.getString(2)}: ${it.getLong(3)} (e.g. ${it.getString(4)})" }.joinToString("\n")

    private fun printSummary(seconds: Double) {
        val events = pg.query("SELECT event, count(*) FROM saga_log GROUP BY 1 ORDER BY 2 DESC") {
            "  %-32s %6d".format(it.getString(1), it.getLong(2))
        }
        println(
            buildString {
                appendLine("SagaStressIT finished in %.1f s (seed $seed)".format(seconds))
                appendLine("Sagas:\n${stateHistogram()}")
                appendLine("Pod crashes: ${crashes.get()}")
                appendLine("Injected database failures: ${dbFailures.mapValues { it.value.get() }}")
                appendLine("Sagas stuck by a check that kept failing: " +
                    pg.long("SELECT count(DISTINCT saga_id) FROM saga_log WHERE detail LIKE 'Check keeps failing%'"))
                appendLine("Calls that threw a JVM Error: " +
                    pg.long("SELECT count(*) FROM saga_log WHERE detail LIKE '%NotImplementedError%'"))
                appendLine("Manual actions: ${manualActions.mapValues { it.value.get() }}")
                appendLine("Stuck reasons seen by the operator: ${stuckReasons.mapValues { it.value.get() }}")
                appendLine("Events reported to the listeners: ${reported.size} (saga_log: ${pg.long("SELECT count(*) FROM saga_log")})")
                appendLine("Payment events: ${deliveries.mapValues { it.value.get() }}")
                appendLine("Cancellations: ${cancellations.mapValues { it.value.get() }}")
                appendLine("Events:")
                events.forEach { appendLine(it) }
            },
        )
    }
}
