package dev.octogene.akta

import dev.octogene.akta.memory.InMemorySagaRepository
import dev.octogene.akta.memory.InMemorySagaTransactions
import dev.octogene.akta.reference.ReferenceSagaEngine
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Timeout
import java.io.IOException
import kotlin.random.Random
import kotlin.reflect.typeOf
import kotlin.test.assertEquals
import kotlin.test.fail
import kotlin.time.Clock
import kotlin.time.Duration
import kotlin.time.Duration.Companion.hours
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.microseconds
import kotlin.time.Duration.Companion.minutes
import kotlin.time.Duration.Companion.seconds
import kotlin.time.Instant
import kotlin.uuid.Uuid

/**
 * The safety net of the engine's refactoring: [SagaEngine] against [ReferenceSagaEngine], the engine as it was
 * before. Each script makes a random saga and configuration, then random events (runs, signals, cancellations,
 * manual actions, time passing, faults turned on and off), played on both engines, each with its own store, clock and
 * external systems, which answer from the same seed. After every event, everything that can be observed must be the
 * same: the row, the results, the signals, the history, the listeners' events, the calls made with what they were
 * given, and what the API returned or threw.
 *
 * A difference is a change of behaviour. One made on purpose is applied to both engines' observations in [World.state],
 * with a comment saying why, or excluded from the scripts; never by editing the reference.
 *
 * Options: -Dakta.scripts=20000 -Dakta.seed=42 (the first seed; each script takes the next one).
 */
@Timeout(300)
class DifferentialTest {
    private val scripts = System.getProperty("akta.scripts")?.toInt() ?: 8000
    private val firstSeed = System.getProperty("akta.seed")?.toLong() ?: 1

    private val events = sortedMapOf<String, Int>()
    private val reasons = sortedMapOf<StuckReason, Int>()
    private val ends = sortedMapOf<SagaState, Int>()

    @Test
    fun `the engine behaves as the reference does, on random scripts`() {
        for (seed in firstSeed until firstSeed + scripts) play(seed)

        println("Differential: $scripts scripts from seed $firstSeed. Ends: $ends\nStuck: $reasons\nEvents: $events")
        // A net with holes proves nothing: every kind of event and every stuck reason must have been reached.
        if (scripts >= 8000) {
            // HOOK_FAILED needs a hook: sagas with one are left out (see Plan.hooks).
            assertEquals(emptySet(), StuckReason.entries.toSet() - reasons.keys - StuckReason.HOOK_FAILED, "stuck reasons never reached")
            assertEquals(emptySet(), EXPECTED_EVENTS - events.keys, "events never reached")
        }
    }

    private fun play(seed: Long) {
        val plan = Plan(Random(seed))
        val reference = World(plan, seed, ::referenceEngine, reference = true)
        val engine = World(plan, seed, ::currentEngine, reference = false)
        val played = mutableListOf<String>()
        compare(seed, played, reference, engine)
        val random = Random(seed xor 0x5DEECE66DL)
        repeat(6 + random.nextInt(22)) {
            val event = nextEvent(random, reference.record(), plan)
            played += event.toString()
            reference.play(event)
            engine.play(event)
            compare(seed, played, reference, engine)
        }
        reference.repository.log.forEach { events.merge(it.event, 1, Int::plus) }
        reference.stuckReasons.forEach { reasons.merge(it, 1, Int::plus) }
        ends.merge(reference.record().state, 1, Int::plus)
    }

    private fun compare(seed: Long, played: List<String>, reference: World, engine: World) {
        val (expected, actual) = reference.state() to engine.state()
        if (expected == actual) return
        val differences = expected.keys.filter { expected[it] != actual[it] }.joinToString("\n") {
            val (e, a) = expected.getValue(it) to actual.getValue(it)
            val at = e.zip(a).indexOfFirst { (x, y) -> x != y }.let { i -> if (i < 0) minOf(e.size, a.size) else i }
            "  $it, entry $at:\n    reference: ${e.getOrNull(at)}\n    engine:    ${a.getOrNull(at)}"
        }
        fail("The engine differs from the reference (replay with -Dakta.seed=$seed -Dakta.scripts=1) after:\n  " +
            (listOf(reference.plan.toString()) + played).joinToString("\n  ") + "\n" + differences)
    }

    // ---- The saga and its configuration -----------------------------------------------------------

    private class StepPlan(
        val name: String, val signal: Boolean, val idempotent: Boolean, val compensates: Boolean, val checks: Boolean,
        val output: Boolean, val timeout: Duration,
    ) {
        override fun toString() = if (signal) "signal(timeout=$timeout, checks=$checks)"
        else "call(idempotent=$idempotent, compensates=$compensates, checks=$checks, output=$output)"
    }

    private class Plan(r: Random) {
        val steps = List(1 + r.nextInt(4)) { i ->
            StepPlan(
                "s$i", signal = r.nextInt(100) < 35, idempotent = r.nextInt(100) < 35, compensates = r.nextInt(100) < 70,
                checks = r.nextInt(100) < 70, output = r.nextInt(100) < 70, timeout = listOf(5.minutes, 30.minutes, 24.hours).random(r),
            )
        }
        // Intended difference: a saga with a hook runs it in a FINISHING transaction of its own (step 4 of the
        // refactoring), which the reference does not. Sagas without hooks must behave as before; those with hooks are
        // tested on their own (SagaFinishingTest, the stress test). The draw is kept, so that the scripts stay the same.
        val hooks = r.nextInt(100) < 70 && false
        /** The short one ends within a call's lock of the times the scripts reach: the key's window has an edge to miss. */
        val keyRetention = listOf(190.seconds, 1.hours).random(r)
        /** The database fails now and then, under either engine at the same transactions. */
        val databaseFails = r.nextInt(100) < 25
        /** Signals come during the timeout checks, in which the pod dies; then onSignal has a bug: rare otherwise. */
        val signalsDuringDeaths = r.nextInt(100) < 12
        /** Time jumps between a claim and its call now and then (GC). */
        val pauses = r.nextInt(100) < 20
        val config = SagaConfig(
            settleDelay = listOf(Duration.ZERO, 2.minutes).random(r),
            checkRetryDelay = listOf(Duration.ZERO, 1.minutes).random(r),
            maxAttempts = 1 + r.nextInt(4),
            maxBackoff = listOf(1.milliseconds, 5.minutes, 1.hours).random(r),
            resumeFailureDelay = listOf(Duration.ZERO, 1.minutes).random(r),
            maxDataBytes = 100,
        )

        override fun toString() = "$steps keyRetention=$keyRetention hooks=$hooks databaseFails=$databaseFails pauses=$pauses signalsDuringDeaths=$signalsDuringDeaths $config"
    }

    // ---- Events -----------------------------------------------------------------------------------

    private enum class Fault { UNREADABLE, HOOK_BUG, ON_SIGNAL_BUG }

    private sealed interface Event {
        data class Run(val budget: Duration, val skipLocked: Boolean) : Event
        data object Resume : Event
        data class Tick(val by: Duration) : Event
        data class Signal(val name: String, val value: String) : Event
        data class Cancel(val reason: String, val runFor: Duration) : Event
        data class Retry(val newKey: Boolean, val runFor: Duration) : Event
        data class MarkDone(val output: String?, val runFor: Duration) : Event
        data class Abort(val applied: Boolean, val output: String?, val runFor: Duration) : Event
        data class MarkCompensated(val runFor: Duration) : Event
        data class Toggle(val fault: Fault) : Event
    }

    /** Mostly what makes sense for the saga as it is, so that scripts go far; sometimes anything. */
    private fun nextEvent(r: Random, saga: SagaRecord, plan: Plan): Event {
        val runFor = if (r.nextInt(3) == 0) Duration.ZERO else 10.seconds
        val output = listOf("\"by-hand\"", "\"by-hand\"", null, "not json", "\"${"x".repeat(120)}\"").random(r)
        fun manual(): Event = when (r.nextInt(if (saga.state == SagaState.COMPENSATING) 3 else 8)) {
            0 -> Event.Retry(newKey = r.nextBoolean(), runFor)
            1 -> Event.MarkCompensated(runFor)
            2 -> Event.Retry(newKey = false, runFor)
            3, 4 -> Event.MarkDone(output, runFor)
            5 -> Event.Abort(applied = true, output, runFor)
            6 -> Event.Abort(applied = false, null, runFor)
            else -> Event.Retry(newKey = false, runFor)
        }
        fun signal(): Event {
            val waiting = plan.steps.filter { it.signal }
            val name = if (waiting.isEmpty() || r.nextInt(20) == 0) "nobody" else waiting.random(r).name
            return Event.Signal(name, listOf("ok", "ok", "ok", "no", "x".repeat(120)).random(r))
        }
        val n = r.nextInt(100)
        return when {
            saga.stepStatus == StepStatus.STUCK && n < 45 -> manual()
            saga.stepStatus == StepStatus.WAITING && !plan.signalsDuringDeaths && n < 25 -> signal()
            // Its pod died, or a call left it so: time passes, and things change before its lock is seen expired.
            saga.stepStatus == StepStatus.WAITING && plan.signalsDuringDeaths && n < 60 -> Event.Tick(25.hours)
            saga.stepStatus == StepStatus.IN_PROGRESS && plan.signalsDuringDeaths && n < 30 -> Event.Toggle(Fault.ON_SIGNAL_BUG)
            saga.stepStatus == StepStatus.IN_PROGRESS && n < 12 -> Event.Toggle(Fault.entries.random(r))
            saga.stepStatus == StepStatus.IN_PROGRESS && n < 35 -> Event.Tick(listOf(36.seconds, 3.minutes, 11.minutes).random(r))
            n < 50 -> Event.Run(listOf(10.seconds, 10.seconds, 10.seconds, Duration.ZERO).random(r), r.nextBoolean())
            n < 64 -> Event.Resume
            n < 82 -> Event.Tick(listOf(1.seconds, 36.seconds, 61.seconds, 3.minutes, 11.minutes, 2.hours, 25.hours).random(r))
            n < 87 -> signal()
            n < 90 -> Event.Cancel("changed\u0000 my mind", runFor)
            n < 94 -> manual()
            else -> Event.Toggle(Fault.entries.random(r))
        }
    }

    // ---- The two engines, behind one interface ----------------------------------------------------

    private interface Engine {
        fun create(key: String): Uuid
        suspend fun run(id: Uuid, deadline: Instant, skipLocked: Boolean): SagaRecord
        suspend fun resume(): Pair<Int, Map<Uuid, Throwable>>
        suspend fun signal(id: Uuid, name: String, value: String): SignalResult
        suspend fun cancel(id: Uuid, reason: String, runFor: Duration): SagaRecord
        suspend fun retry(id: Uuid, newKey: Boolean, runFor: Duration): SagaRecord
        suspend fun markDone(id: Uuid, output: String?, runFor: Duration): SagaRecord
        suspend fun abort(id: Uuid, applied: Boolean, output: String?, runFor: Duration): SagaRecord
        suspend fun markCompensated(id: Uuid, runFor: Duration): SagaRecord
        suspend fun stats(): SagaStats
    }

    private fun currentEngine(w: World, tx: SagaTransactions, repository: SagaRepository, def: SagaDefinition<Unit>) = object : Engine {
        val e = SagaEngine(
            tx, repository, listOf(def), KotlinxJsonSagaSerializer(), w.plan.config, w.clock, { "trace" }, listOf(SagaListener { w.heard(it) }),
        )
        override fun create(key: String) = e.create(def, key)
        override suspend fun run(id: Uuid, deadline: Instant, skipLocked: Boolean) = e.run(id, deadline, skipLocked)
        override suspend fun resume() = e.resumeDue().let { it.sagas to it.failures }
        override suspend fun signal(id: Uuid, name: String, value: String) = e.signal(id, name, value)
        override suspend fun cancel(id: Uuid, reason: String, runFor: Duration) = e.cancel(id, reason, "actor", runFor)
        override suspend fun retry(id: Uuid, newKey: Boolean, runFor: Duration) = e.retry(id, "actor", newKey, runFor)
        override suspend fun markDone(id: Uuid, output: String?, runFor: Duration) = e.markDone(id, output, "actor", runFor)
        override suspend fun abort(id: Uuid, applied: Boolean, output: String?, runFor: Duration) = e.abort(id, "actor", applied, output, runFor)
        override suspend fun markCompensated(id: Uuid, runFor: Duration) = e.markCompensated(id, "actor", runFor)
        override suspend fun stats() = e.stats()
    }

    private fun referenceEngine(w: World, tx: SagaTransactions, repository: SagaRepository, def: SagaDefinition<Unit>) = object : Engine {
        val e = ReferenceSagaEngine(
            tx, repository, listOf(def), KotlinxJsonSagaSerializer(), w.plan.config, w.clock, { "trace" }, listOf(SagaListener { w.heard(it) }),
        )
        override fun create(key: String) = e.create(def, key)
        override suspend fun run(id: Uuid, deadline: Instant, skipLocked: Boolean) = e.run(id, deadline, skipLocked)
        override suspend fun resume() = e.resumeDue().let { it.sagas to it.failures }
        override suspend fun signal(id: Uuid, name: String, value: String) = e.signal(id, name, value, typeOf<String>())
        override suspend fun cancel(id: Uuid, reason: String, runFor: Duration) = e.cancel(id, reason, "actor", runFor)
        override suspend fun retry(id: Uuid, newKey: Boolean, runFor: Duration) = e.retry(id, "actor", newKey, runFor)
        override suspend fun markDone(id: Uuid, output: String?, runFor: Duration) = e.markDone(id, output, "actor", runFor)
        override suspend fun abort(id: Uuid, applied: Boolean, output: String?, runFor: Duration) = e.abort(id, "actor", applied, output, runFor)
        override suspend fun markCompensated(id: Uuid, runFor: Duration) = e.markCompensated(id, "actor", runFor)
        override suspend fun stats() = e.stats()
    }

    private class MutableClock(var now: Instant = Instant.parse("2026-11-21T10:00:00Z")) : Clock {
        override fun now(): Instant = now
    }

    // ---- One engine's world -----------------------------------------------------------------------

    private class World(
        val plan: Plan,
        seed: Long,
        newEngine: (World, SagaTransactions, SagaRepository, SagaDefinition<Unit>) -> Engine,
        reference: Boolean,
    ) {
        val clock = MutableClock()
        val repository = InMemorySagaRepository()
        val stuckReasons = mutableSetOf<StuckReason>()
        private val faults = mutableSetOf<Fault>()
        private val replies = Random(seed * 31 + 7)
        private val database = Random(seed * 37 + 11)
        /** What the steps, hooks and API calls saw and answered. */
        private val trace = mutableListOf<String>()
        private val heard = mutableListOf<String>()
        private val statuses = mutableListOf<String>()
        private var nested = 0
        private val engine: Engine
        private val id: Uuid

        init {
            val store = object : SagaRepository by repository {
                override fun results(sagaId: Uuid): Map<String, String?> =
                    if (Fault.UNREADABLE in faults && !(reference && readForNoHook())) throw IllegalStateException("corrupted row")
                    else repository.results(sagaId)

                // Intended difference (PERF-02): the reference reads a saga's data as it ends, to run its hooks, even
                // when it has none (the default hook does nothing); the engine runs no hook then, and reads nothing.
                // So, where the reference reads it for that alone, its data is not unreadable.
                private fun readForNoHook() = Thread.currentThread().stackTrace.any { it.methodName == "finishIfNeeded" }

                // The reference does not record the call in flight, which the schema now requires: one that fits the
                // state will do, the comparison leaves it out.
                override fun save(record: SagaRecord, now: Instant) = repository.save(record.withCall(), now)
                override fun insert(record: SagaRecord, now: Instant) = repository.insert(record.withCall(), now)
                private fun SagaRecord.withCall() = if (!reference) this else
                    copy(callAction = if (stepStatus != StepStatus.IN_PROGRESS) null
                        else if (state == SagaState.COMPENSATING) CallAction.COMPENSATE else CallAction.EXECUTE)
            }
            engine = newEngine(this, Transactions(InMemorySagaTransactions(repository)), store, definition())
            id = engine.create("key")
        }

        fun record(): SagaRecord = repository.find(id)!!
        fun heard(event: SagaEvent) { heard += text("$event") }
        /**
         * Without the saga's random id, and the name of an engine's private exception (it is in error messages).
         * Intended differences: callAction, which the reference does not record (the engine records the call in flight).
         */
        private fun text(s: String) = s.replace(id.toString(), "<id>").replace(PRIVATE_EXCEPTION, "SignalFailed")
            .replace(CALL_ACTION, ", callAction=null")

        /** Everything observable, without the saga's random id. */
        fun state(): Map<String, List<String>> = mapOf(
            "row" to listOf(text("${record()}")),
            "results" to repository.storedResults(id).map { text("$it") },
            "signals" to plan.steps.filter { it.signal }.map { "${it.name}=${repository.signal(id, it.name)}" },
            "history" to repository.log.map { text("$it") },
            "listeners" to heard.toList(),
            "calls and answers" to trace.toList(),
            "statuses" to statuses.toList(),
        )

        fun play(event: Event) {
            val answer = runBlocking {
                try {
                    when (event) {
                        is Event.Run -> engine.run(id, clock.now + event.budget, event.skipLocked)
                        Event.Resume -> engine.resume()
                        is Event.Tick -> { clock.now += event.by; engine.stats() }
                        is Event.Signal -> engine.signal(id, event.name, event.value)
                        is Event.Cancel -> engine.cancel(id, event.reason, event.runFor)
                        is Event.Retry -> engine.retry(id, event.newKey, event.runFor)
                        is Event.MarkDone -> engine.markDone(id, event.output, event.runFor)
                        is Event.Abort -> engine.abort(id, event.applied, event.output, event.runFor)
                        is Event.MarkCompensated -> engine.markCompensated(id, event.runFor)
                        is Event.Toggle -> if (!faults.remove(event.fault)) faults += event.fault else Unit
                    }
                } catch (e: Throwable) {
                    "threw ${e::class.simpleName}: ${e.message}"
                }
            }
            trace += text("answer: $answer")
            record().stuckReason?.let { stuckReasons += it }
        }

        /** The database under the engine: fails before or after a commit; and pauses. */
        private inner class Transactions(private val inner: SagaTransactions) : SagaTransactions {
            override suspend fun <T> inTransaction(block: () -> T): T {
                val fails = if (plan.databaseFails) database.nextInt(100) else 99
                val result = inner.inTransaction {
                    val result = block()
                    if (fails < 4) throw IOException("rolled back")
                    result
                }
                statuses += record().run { "$state/$stepIndex/$stepStatus" }       // each commit, not only the last one
                clock.now += 1.microseconds                                      // as a real one: two commits, two times
                if (fails in 4..6) throw IOException("lost after commit")
                if (plan.pauses && database.nextInt(100) < 10) clock.now += 34.seconds
                return result
            }
        }

        /** What others do while a call is in flight: a signal, a cancellation, or the lock expiring and another pod's run. */
        private suspend fun meanwhile(chance: Int = 15, signals: Boolean = false) {
            if (nested > 0 || replies.nextInt(100) >= chance) return
            nested++
            val what = if (signals) 0 else replies.nextInt(4)
            val answer = try {
                when (what) {
                    0 -> plan.steps.filter { it.signal }.map { engine.signal(id, it.name, if (replies.nextInt(4) == 0) "no" else "ok") }
                    1 -> engine.cancel(id, "meanwhile", Duration.ZERO)
                    2 -> { clock.now += 36.seconds; engine.run(id, clock.now + 10.seconds, skipLocked = false) }
                    else -> { clock.now += 3.minutes; engine.resume() }
                }
            } catch (e: IOException) {
                "threw $e"
            } catch (e: SimulatedCrash) {
                "the other pod died"                                       // not this one
            } finally {
                nested--
            }
            trace += text("meanwhile $what: $answer")
        }

        private fun pick(vararg weights: Int): Int {
            var n = replies.nextInt(weights.sum())
            return weights.indexOfFirst { n -= it; n < 0 }
        }

        private fun SagaContext<Unit>.called(what: String, step: StepPlan) {
            val seen = plan.steps.map { it.name to result(StepRef<Any?>(it.name)) }
            trace += text("$what ${step.name} $idempotencyKey sees $seen")
        }

        private suspend fun execute(ctx: SagaContext<Unit>, step: StepPlan): String {
            ctx.called("execute", step)
            meanwhile()
            return when (pick(58, 8, 10, 12, 4, 4, 4)) {
                0 -> "out-${replies.nextInt(1000)}"
                1 -> throw StepRejected("refused")
                2 -> throw StepNotSent("not sent")
                3 -> throw IOException("read timeout")
                4 -> throw IOException("binary\u0000body")
                5 -> "x".repeat(120)                                       // over maxDataBytes
                else -> throw SimulatedCrash()
            }
        }

        private suspend fun check(ctx: SagaContext<Unit>, step: StepPlan): CheckResult<String> {
            ctx.called("check", step)
            val dies = step.signal && plan.signalsDuringDeaths
            meanwhile(chance = if (dies) 80 else if (step.signal) 40 else 15, signals = dies)
            if (dies && replies.nextBoolean()) throw SimulatedCrash()
            return when (pick(30, 25, 12, 8, 13, 4, 8)) {
                0 -> CheckResult.Done(if (step.signal && replies.nextInt(4) == 0) "no" else "found-${replies.nextInt(1000)}")
                1 -> CheckResult.NotDone
                2 -> CheckResult.CannotTell
                3 -> throw StepRejected("found refused")
                4 -> throw IOException("lookup down")
                5 -> CheckResult.Done("x".repeat(120))
                else -> throw SimulatedCrash()
            }
        }

        private suspend fun compensate(ctx: SagaContext<Unit>, step: StepPlan, own: Any?) {
            ctx.called("compensate($own)", step)
            meanwhile()
            when (pick(70, 22, 8)) {
                0 -> Unit
                1 -> throw IOException("undo failed")
                else -> throw SimulatedCrash()
            }
        }

        private fun hook(name: String, ctx: SagaContext<Unit>) {
            trace += text("$name sees ${plan.steps.map { it.name to ctx.result(StepRef<Any?>(it.name)) }}")
            if (Fault.HOOK_BUG in faults) error("hook bug")
        }

        private fun definition() = saga("TEST") {
            for (step in plan.steps) {
                if (step.signal) {
                    awaitSignal<String>(step.name, signal = step.name, timeout = step.timeout) {
                        onSignal {
                            trace += "onSignal ${step.name} $it"
                            if (Fault.ON_SIGNAL_BUG in faults) error("onSignal bug")
                            if (it == "no") throw StepRejected("declined")
                        }
                        if (step.checks) check { check(this, step) }
                        logAs { it.take(8) }
                    }
                } else if (step.output) {
                    step<String>(step.name) {
                        if (step.idempotent) idempotent(plan.keyRetention)
                        execute { execute(this, step) }
                        if (step.compensates) compensate { own -> compensate(this, step, own) }
                        if (step.checks) check { check(this, step) }
                        logAs { it.take(8) }
                    }
                } else {
                    step(step.name) {
                        if (step.idempotent) idempotent(plan.keyRetention)
                        execute { execute(this, step) }
                        if (step.compensates) compensate { own -> compensate(this, step, own) }
                        if (step.checks) check {
                            when (val c = check(this, step)) {
                                is CheckResult.Done -> CheckResult.Done(Unit)
                                CheckResult.NotDone -> CheckResult.NotDone
                                CheckResult.CannotTell -> CheckResult.CannotTell
                            }
                        }
                    }
                }
            }
            if (plan.hooks) {
                onCompleted { hook("onCompleted", this) }
                onFailed { hook("onFailed", this) }
            }
        }
    }

    private companion object {
        val CALL_ACTION = Regex(""", callAction=\w+""")
        val PRIVATE_EXCEPTION = Regex("""dev\.octogene\.akta\.[\w.$]*SignalFailed""")
        val EXPECTED_EVENTS = setOf(
            "EXECUTE:Done", "EXECUTE:Rejected", "EXECUTE:NotSent", "EXECUTE:Unknown", "EXECUTE:OutputUnstorable",
            "CHECK:Done", "CHECK:NotDone", "CHECK:CannotTell", "CHECK:CheckFailed", "CHECK:Rejected", "CHECK:OutputUnstorable",
            "COMPENSATE:Compensated", "COMPENSATE:CompensationFailed",
            SagaEvents.CREATED, SagaEvents.LOCK_EXPIRED, SagaEvents.LATE_RESULT_IGNORED, SagaEvents.RECORD_FAILED,
            SagaEvents.RESUME_FAILED, SagaEvents.MANUAL_RETRY, SagaEvents.MANUAL_DONE, SagaEvents.MANUAL_ABORT,
            SagaEvents.MANUAL_COMPENSATED, SagaEvents.WAIT_STARTED, SagaEvents.WAIT_TIMED_OUT, SagaEvents.SIGNAL_RECEIVED,
            SagaEvents.KEY_EXPIRED, SagaEvents.NOT_CALLED, SagaEvents.NOTHING_TO_UNDO, SagaEvents.CANCEL_REQUESTED,
            SagaEvents.CANCELLED,
        )
    }
}
