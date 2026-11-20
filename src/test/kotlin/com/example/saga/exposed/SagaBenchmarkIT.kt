package com.example.saga.exposed

import com.example.saga.SagaContext
import com.example.saga.SagaDefinition
import com.example.saga.SagaEngine
import com.example.saga.SagaStep
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.sync.Semaphore
import kotlinx.coroutines.sync.withPermit
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import java.io.File
import java.util.Locale
import kotlin.test.assertEquals
import kotlin.time.Clock
import kotlin.time.Duration.Companion.seconds

/**
 * Measures what a saga costs the database, with steps that return immediately: what is left is the
 * engine's own overhead. Run with `./gradlew benchmark` (Docker required), optionally with
 * `-Dsaga.sagas=2000 -Dsaga.concurrency=16`. The report is printed and written to
 * build/reports/saga-benchmark.md.
 */
@Tag("benchmark")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class SagaBenchmarkIT {
    private val sagas = System.getProperty("saga.sagas")?.toInt() ?: 1000
    private val concurrency = System.getProperty("saga.concurrency")?.toInt() ?: 8
    private val pg = PostgresFixture(durable = true)
    private val report = StringBuilder()

    private val definition = SagaDefinition(
        "ITEM_SETUP",
        listOf(instantStep("stock"), instantStep("booking")),
    )

    private fun instantStep(name: String) = object : SagaStep {
        override val name = name
        override suspend fun execute(ctx: SagaContext) = "$name-${ctx.key}"
    }

    private lateinit var transactions: ExposedSagaTransactions
    private lateinit var engine: SagaEngine

    @BeforeAll
    fun start() {
        pg.start(poolSize = concurrency * 2 + 2)
        transactions = ExposedSagaTransactions(pg.database)
        engine = SagaEngine(transactions, ExposedSagaRepository(), listOf(definition))
        pg.resetSchema()
        runBlocking { inline(count = 200, parallelism = concurrency, prefix = "warmup") }   // JIT, pool, caches
        report.appendLine("# Saga benchmark\n")
        report.appendLine("$sagas sagas of ${definition.steps.size} instant steps per scenario, PostgreSQL 16 (fsync on), pool of ${concurrency * 2 + 2}.\n")
    }

    @AfterAll
    fun stop() {
        pg.close()
        println(report)
        File("build/reports").mkdirs()
        File("build/reports/saga-benchmark.md").writeText(report.toString())
    }

    @Test
    fun `inline, one request at a time`() = scenario("Inline (HTTP path), sequential") {
        inline(sagas, parallelism = 1, prefix = "seq")
    }

    @Test
    fun `inline, concurrent requests`() = scenario("Inline (HTTP path), $concurrency concurrent requests") {
        inline(sagas, parallelism = concurrency, prefix = "par")
    }

    @Test
    fun `recovery through resumeDue`() = scenario("Recovery: resumeDue(concurrency = $concurrency)") {
        val ids = (1..sagas).map { i -> transactions.inTransaction { engine.create("ITEM_SETUP", "rec-$i") } }
        val start = System.nanoTime()
        do {
            val result = engine.resumeDue(limit = 100, concurrency = concurrency)
            assertEquals(emptyMap(), result.failures)
        } while (result.sagas > 0)
        check(ids.isNotEmpty())
        Timing(total = System.nanoTime() - start, perSaga = emptyList())
    }

    // ---- Scenarios ------------------------------------------------------------------------------

    private class Timing(val total: Long, val perSaga: List<Long>)

    /** Like an HTTP handler: create in its own transaction, then run inline. */
    private suspend fun inline(count: Int, parallelism: Int, prefix: String): Timing {
        val permits = Semaphore(parallelism)
        val start = System.nanoTime()
        val latencies = coroutineScope {
            (1..count).map { i ->
                async {
                    permits.withPermit {
                        val t0 = System.nanoTime()
                        val id = transactions.inTransaction { engine.create("ITEM_SETUP", "$prefix-$i") }
                        engine.run(id, Clock.System.now() + 10.seconds)
                        System.nanoTime() - t0
                    }
                }
            }.awaitAll()
        }
        return Timing(System.nanoTime() - start, latencies)
    }

    private fun scenario(title: String, body: suspend () -> Timing) {
        pg.resetSchema()
        pg.sql { exec("SELECT pg_stat_statements_reset()") }
        val timing = runBlocking { body() }

        val completed = pg.long("SELECT count(*) FROM saga WHERE state = 'COMPLETED'")
        assertEquals(sagas.toLong(), completed, "every saga must complete")

        val statements = pg.query(
            """
            SELECT query, calls, total_exec_time FROM pg_stat_statements
            WHERE dbid = (SELECT oid FROM pg_database WHERE datname = current_database())
              AND query NOT ILIKE '%pg_stat%' AND query NOT ILIKE '%pg_total_relation_size%'
            ORDER BY calls DESC
            """.trimIndent(),
        ) { Statement(it.getString(1), it.getLong(2), it.getDouble(3)) }
        val sizes = listOf("saga", "saga_step_result", "saga_log").associateWith {
            pg.long("SELECT pg_total_relation_size('$it')")
        }
        report(title, timing, statements, sizes)
    }

    private class Statement(val query: String, val calls: Long, val totalMs: Double)

    // ---- Report ---------------------------------------------------------------------------------

    private fun report(title: String, timing: Timing, statements: List<Statement>, sizes: Map<String, Long>) {
        fun String.f(vararg args: Any) = format(Locale.ROOT, *args)
        fun ms(nanos: Long) = "%.2f ms".f(nanos / 1e6)
        fun perSaga(n: Long) = "%.1f".f(n.toDouble() / sagas)

        report.appendLine("## $title\n")
        report.appendLine("| Metric | Value |\n|---|---|")
        report.appendLine("| Total | ${ms(timing.total)} (%.0f sagas/s) |".f(sagas / (timing.total / 1e9)))
        if (timing.perSaga.isNotEmpty()) {
            val sorted = timing.perSaga.sorted()
            fun pct(p: Double) = sorted[((sorted.size - 1) * p).toInt()]
            report.appendLine("| Latency per saga (create + run) | avg ${ms(sorted.average().toLong())}, p50 ${ms(pct(0.5))}, p95 ${ms(pct(0.95))}, p99 ${ms(pct(0.99))} |")
        }
        val isTx = { s: Statement -> s.query.trim().uppercase().let { it == "BEGIN" || it == "COMMIT" || it == "ROLLBACK" } }
        // The driver commits at protocol level (no COMMIT statement), but sends one BEGIN per transaction.
        val transactions = statements.filter { it.query.trim().equals("BEGIN", ignoreCase = true) }.sumOf { it.calls }
        val work = statements.filterNot(isTx)
        report.appendLine("| Statements per saga (excluding BEGIN) | ${perSaga(work.sumOf { it.calls })} |")
        report.appendLine("| Transactions (commits) per saga | ${perSaga(transactions)} |")
        report.appendLine("| Database time per saga (sum of statements, excluding commits) | %.2f ms |".f(statements.sumOf { it.totalMs } / sagas))
        sizes.forEach { (table, bytes) ->
            report.appendLine("| Size of `$table` (with indexes) per saga | ${bytes / sagas} B |")
        }
        report.appendLine("\n| Calls per saga | Mean | Statement |\n|---|---|---|")
        statements.forEach {
            val text = it.query.replace(Regex("\\s+"), " ").take(110).replace("|", "\\|")
            report.appendLine("| ${perSaga(it.calls)} | %.3f ms | `$text` |".f(it.totalMs / it.calls))
        }
        report.appendLine()
    }
}
