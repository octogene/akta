package com.example.saga.exposed

import com.example.saga.CheckResult
import com.example.saga.SagaConfig
import com.example.saga.SagaContext
import com.example.saga.SagaDefinition
import com.example.saga.SagaEngine
import com.example.saga.SagaEvents
import com.example.saga.SagaState
import com.example.saga.SagaStep
import com.example.saga.StepNotSent
import com.example.saga.StepRejected
import com.example.saga.SagaTransactions
import com.example.saga.StepStatus
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.awaitCancellation
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
import kotlin.test.fail
import kotlin.time.Clock
import kotlin.time.Duration
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.minutes
import kotlin.time.Duration.Companion.seconds

/**
 * Runs many sagas against PostgreSQL while everything that can go wrong does, at random: rejections,
 * requests not sent, lost requests and responses, calls outliving their lock, failing checks and
 * compensations, hooks that throw, and pods dying mid-call. Several pods resume sagas concurrently,
 * and an operator resolves stuck sagas by looking at the external systems, as a human would.
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

    /** An external system without idempotency keys. It knows what is really applied, for each item. */
    private inner class ExternalSystem(val name: String) {
        private val applied = HashMap<String, String>()
        private var counter = 0

        @Synchronized fun apply(key: String): String {
            applied[key]?.let { violations += "$name: applied a second time for $key (already $it): duplicate" }
            return "$name-${++counter}".also { applied[key] = it }
        }
        @Synchronized fun remove(key: String) { applied.remove(key) }
        @Synchronized fun lookup(key: String): String? = applied[key]
    }

    /** The coroutine of a pod (or of a request): a crash cancels all of it, as when a process dies. */
    private class Pod(val job: Job) : AbstractCoroutineContextElement(Pod) {
        companion object Key : CoroutineContext.Key<Pod>
    }

    private suspend fun crash(): Nothing {
        crashes.incrementAndGet()
        currentCoroutineContext()[Pod]!!.job.cancel()
        awaitCancellation()
    }

    private suspend fun pause() = delay(dice.millis(5))

    private enum class Call { OK, REJECTED, NOT_SENT, LOST_REQUEST, LOST_RESPONSE, OUTLIVES_LOCK, CRASH, JVM_ERROR }
    private enum class Undo { OK, FAILS_BEFORE, FAILS_AFTER, CRASH }
    private enum class Lookup { TRUTH, FAILS, CANNOT_TELL, CRASH }

    private val callWeights = mapOf(
        Call.OK to 70, Call.REJECTED to 3, Call.NOT_SENT to 8, Call.LOST_REQUEST to 5,
        Call.LOST_RESPONSE to 6, Call.OUTLIVES_LOCK to 3, Call.CRASH to 5, Call.JVM_ERROR to 2,
    )

    /** About 1 item in 25 per system has a lookup that always throws: its check can never answer. */
    private fun lookupBroken(system: ExternalSystem, key: String) = Math.floorMod("${system.name}/$key".hashCode(), 25) == 0
    private val undoWeights = mapOf(Undo.OK to 80, Undo.FAILS_BEFORE to 8, Undo.FAILS_AFTER to 7, Undo.CRASH to 5)
    private val lookupWeights = mapOf(Lookup.TRUTH to 80, Lookup.FAILS to 10, Lookup.CANNOT_TELL to 7, Lookup.CRASH to 3)
    private val hookFailure = 0.1

    /** A step calling [system]. It also checks that it sees the reference of the previous step. */
    private inner class ChaosStep(val system: ExternalSystem, private val previous: ChaosStep?) : SagaStep {
        override val name = system.name

        override suspend fun execute(ctx: SagaContext): String? {
            previous?.let {
                val seen = ctx.result(it.name)
                val truth = it.system.lookup(ctx.key)
                if (seen != truth) violations += "$name for ${ctx.key}: saw ${it.name}=$seen, but the system has $truth"
            }
            pause()
            return when (dice.pick(callWeights)) {
                Call.OK -> system.apply(ctx.key).also { pause() }
                Call.REJECTED -> throw StepRejected("refused")
                Call.NOT_SENT -> throw StepNotSent("connection refused")
                Call.LOST_REQUEST -> throw IOException("read timeout (request lost, not applied)")
                Call.LOST_RESPONSE -> { system.apply(ctx.key); pause(); throw IOException("read timeout (applied)") }
                Call.OUTLIVES_LOCK -> system.apply(ctx.key).also { delay(config.callLock + 200.milliseconds) }
                Call.CRASH -> { if (dice.chance(0.5)) system.apply(ctx.key); crash() }
                // A JVM Error, not an Exception (a TODO() or an assertion in a client), applied or not.
                Call.JVM_ERROR -> { if (dice.chance(0.5)) system.apply(ctx.key); throw NotImplementedError("client bug") }
            }
        }

        override suspend fun compensate(ctx: SagaContext) {
            pause()
            when (dice.pick(undoWeights)) {
                Undo.OK -> system.remove(ctx.key)
                Undo.FAILS_BEFORE -> throw IOException("undo failed (nothing done)")
                Undo.FAILS_AFTER -> { system.remove(ctx.key); throw IOException("undo failed (after removing)") }
                Undo.CRASH -> { if (dice.chance(0.5)) system.remove(ctx.key); crash() }
            }
        }

        override suspend fun check(ctx: SagaContext): CheckResult {
            pause()
            if (lookupBroken(system, ctx.key)) throw IOException("lookup broken for ${ctx.key}")
            return when (dice.pick(lookupWeights)) {
                Lookup.TRUTH -> system.lookup(ctx.key)?.let { CheckResult.Done(it) } ?: CheckResult.NotDone
                Lookup.FAILS -> throw IOException("lookup unavailable")
                Lookup.CANNOT_TELL -> CheckResult.CannotTell
                Lookup.CRASH -> crash()
            }
        }
    }

    /** Steps calling one system each; each step checks the reference of the one before it. */
    private fun chain(vararg systems: String) =
        systems.map(::ExternalSystem).fold(emptyList<ChaosStep>()) { acc, s -> acc + ChaosStep(s, acc.lastOrNull()) }

    private val stepsByType = mapOf(
        "ITEM_SETUP" to chain("stock", "booking", "promo"),
        "ITEM_EXIT" to chain("unstock", "archive"),      // the type an old pod does not know (rolling deploy)
    )

    /** The business change of the hooks, in the engine's transaction; throws sometimes, after writing. */
    private fun hook(key: String, status: String) {
        TransactionManager.current().exec(
            "UPDATE item SET status = '$status', hook_runs = hook_runs + 1 WHERE key = '$key'",
        )
        if (dice.chance(hookFailure)) error("hook failed for $key")
    }

    private val definitions = stepsByType.map { (type, steps) ->
        SagaDefinition(type, steps, onCompleted = { hook(it.key, "DONE") }, onFailed = { hook(it.key, "FAILED") })
    }

    /** An engine as deployed in one pod: [types] are the saga types its version knows. */
    private fun newEngine(types: Set<String> = stepsByType.keys) = SagaEngine(
        FlakyTransactions(ExposedSagaTransactions(pg.database)), ExposedSagaRepository(),
        definitions.filter { it.type in types }, config,
    )

    /** A database failure injected by [FlakyTransactions]. */
    private class InjectedDatabaseFailure(message: String) : SQLException(message)

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
                    launch { restarting("operator") { operator(newEngine()) } }
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
            val pod = launch { withContext(Pod(coroutineContext.job)) { body() } }
            pod.join()
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
                    val type = if (dice.chance(0.25)) "ITEM_EXIT" else "ITEM_SETUP"
                    val key = "$type-$i"
                    val id = transactions.inTransaction {
                        TransactionManager.current().exec("INSERT INTO item (key, status) VALUES ('$key', 'PENDING')")
                        engine.create(type, key)
                    }
                    val request = launch {
                        withContext(Pod(coroutineContext.job)) {
                            try {
                                engine.run(id, Clock.System.now() + dice.millis(100).milliseconds)
                            } catch (e: CancellationException) {
                                throw e
                            } catch (e: Exception) {
                                if (!e.isInjected()) violations += "request for $key failed: $e"
                            }
                        }
                    }
                    request.join()
                } finally {
                    permits.release()
                }
            }
        }
    }

    /** A pod's recovery job. */
    private suspend fun pod(engine: SagaEngine) {
        while (true) {
            try {
                val result = engine.resumeDue(limit = 20, budgetPerSaga = 5.seconds, concurrency = 8)
                result.failures.filterValues { !it.isInjected() }
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
            val stuck = pg.sql {
                SagaTable.selectAll().where { SagaTable.stepStatus eq StepStatus.STUCK.name }.map {
                    Triple(it[SagaTable.id], SagaState.valueOf(it[SagaTable.state]), it[SagaTable.stepIndex]) to
                        (it[SagaTable.type] to it[SagaTable.businessKey])
                }
            }
            for ((saga, typeAndKey) in stuck) {
                val (id, state, stepIndex) = saga
                val (type, key) = typeAndKey
                val truth = stepsByType.getValue(type)[stepIndex].system.lookup(key)
                val action = when {
                    state == SagaState.COMPENSATING -> "retry"
                    truth != null -> if (dice.chance(0.7)) "markDone" else "abort(applied)"
                    else -> if (dice.chance(0.7)) "retry" else "abort"
                }
                manualActions.computeIfAbsent(action) { AtomicInteger() }.incrementAndGet()
                try {
                    when (action) {
                        "retry" -> engine.retry(id, "operator")
                        "markDone" -> engine.markDone(id, truth, "operator")
                        "abort(applied)" -> engine.abort(id, "operator", currentStepApplied = true, ref = truth)
                        else -> engine.abort(id, "operator", currentStepApplied = false)
                    }
                } catch (e: CancellationException) {
                    throw e
                } catch (e: Exception) {
                    // Expected: a hook threw inside the manual action (rolled back, still stuck: try again later),
                    // or an injected database failure. Anything else is an engine error.
                    val hook = e is IllegalStateException && e.message.orEmpty().startsWith("hook failed")
                    val expected = hook || e.isInjected()
                    val label = when { hook -> "failed (hook)"; e.isInjected() -> "failed (database)"; else -> "failed ($e)" }
                    manualActions.computeIfAbsent(label) { AtomicInteger() }.incrementAndGet()
                    if (!expected) violations += "$action on $key failed: $e"
                }
            }
            delay(100)
        }
    }

    private fun unfinished() = pg.long("SELECT count(*) FROM saga WHERE state IN ('RUNNING', 'COMPENSATING')") +
        (sagaCount - pg.long("SELECT count(*) FROM saga"))

    // ---- Invariants -----------------------------------------------------------------------------

    private class Row(
        val type: String, val key: String, val state: String, val stepStatus: String, val lockedUntil: Any?,
        val itemStatus: String, val hookRuns: Int,
    )

    private fun checkInvariants() {
        val problems = violations.toMutableList()
        val rows = pg.query(
            """
            SELECT s.type, s.business_key, s.state, s.step_status, s.locked_until, i.status, i.hook_runs
            FROM saga s JOIN item i ON i.key = s.business_key
            """.trimIndent(),
        ) {
            Row(it.getString(1), it.getString(2), it.getString(3), it.getString(4), it.getObject(5), it.getString(6), it.getInt(7))
        }
        // step results by key: step name -> (ref, compensated)
        val results = pg.query(
            """
            SELECT s.business_key, r.step_name, r.external_ref, r.compensated_at IS NOT NULL
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
            when (r.state) {
                SagaState.COMPLETED.name -> {
                    if (r.itemStatus != "DONE") problems += "${r.key}: COMPLETED but item is ${r.itemStatus}"
                    for (step in steps) {
                        val truth = step.system.lookup(r.key)
                        val (ref, compensated) = stored[step.name] ?: (null to false)
                        if (truth == null) problems += "${r.key}: COMPLETED but ${step.name} is not applied"
                        if (ref != truth) problems += "${r.key}: stored ${step.name}=$ref, system has $truth"
                        if (compensated) problems += "${r.key}: COMPLETED but ${step.name} is marked compensated"
                    }
                }
                SagaState.FAILED.name -> {
                    if (r.itemStatus != "FAILED") problems += "${r.key}: FAILED but item is ${r.itemStatus}"
                    for (step in steps) {
                        step.system.lookup(r.key)?.let { problems += "${r.key}: FAILED but ${step.name} is still applied ($it)" }
                    }
                    stored.filterValues { !it.second }.keys.forEach {
                        problems += "${r.key}: FAILED but the result of $it is not marked compensated"
                    }
                }
                else -> problems += "${r.key}: not finished (${r.state} / ${r.stepStatus})"
            }
        }

        val known = setOf(
            SagaEvents.CREATED, SagaEvents.LOCK_EXPIRED, SagaEvents.LATE_RESULT_IGNORED, SagaEvents.RECORD_FAILED,
            SagaEvents.RESUME_FAILED,
            SagaEvents.MANUAL_RETRY, SagaEvents.MANUAL_DONE, SagaEvents.MANUAL_ABORT,
        )
        val outcome = Regex("(EXECUTE|CHECK|COMPENSATE):[A-Za-z]+")
        pg.query("SELECT DISTINCT event FROM saga_log") { it.getString(1) }
            .filterNot { it in known || outcome.matches(it) }
            .forEach { problems += "unknown event in saga_log: $it" }
        val created = pg.long("SELECT count(*) FROM saga_log WHERE event = 'CREATED'")
        if (created != sagaCount.toLong()) problems += "expected $sagaCount CREATED events, found $created"

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
                appendLine("Events:")
                events.forEach { appendLine(it) }
            },
        )
    }
}
