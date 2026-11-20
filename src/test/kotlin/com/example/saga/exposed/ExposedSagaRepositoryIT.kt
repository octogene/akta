package com.example.saga.exposed

import com.example.saga.DuplicateSagaException
import com.example.saga.SagaContext
import com.example.saga.SagaDefinition
import com.example.saga.SagaEngine
import com.example.saga.SagaState
import com.example.saga.SagaStep
import com.example.saga.StepNotSent
import com.example.saga.StepRejected
import kotlinx.coroutines.runBlocking
import org.jetbrains.exposed.v1.core.DatabaseConfig
import org.jetbrains.exposed.v1.jdbc.Database
import org.jetbrains.exposed.v1.jdbc.transactions.transaction
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.testcontainers.postgresql.PostgreSQLContainer
import java.sql.Connection
import java.util.concurrent.CountDownLatch
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.time.Clock
import kotlin.time.Duration.Companion.days
import kotlin.time.Duration.Companion.seconds

/** Runs the engine against a real PostgreSQL (Testcontainers: Docker required). */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class ExposedSagaRepositoryIT {

    private val postgres = PostgreSQLContainer("postgres:16-alpine")
    private lateinit var database: Database
    private val repository = ExposedSagaRepository()

    @BeforeAll
    fun start() {
        postgres.start()
        database = Database.connect(
            url = postgres.jdbcUrl, user = postgres.username, password = postgres.password,
            databaseConfig = DatabaseConfig { defaultMaxAttempts = 1 },
        )
    }

    @AfterAll
    fun stop() = postgres.stop()

    @BeforeEach
    fun resetSchema() {
        val schema = javaClass.getResource("/com/example/saga/schema.sql")!!.readText()
        transaction(database) {
            exec("DROP TABLE IF EXISTS saga_log, saga_step_result, saga")
            exec(schema)
        }
    }

    private fun step(name: String, block: () -> String?) = object : SagaStep {
        override val name = name
        override suspend fun execute(ctx: SagaContext) = block()
    }

    private fun engine(vararg steps: SagaStep, onCompleted: (SagaContext) -> Unit = {}) = SagaEngine(
        ExposedSagaTransactions(database), repository,
        listOf(SagaDefinition("TEST", steps.toList(), onCompleted = onCompleted)),
    )

    private fun SagaEngine.start(key: String) = runBlocking {
        val id = ExposedSagaTransactions(database).inTransaction { create("TEST", key) }
        run(id, Clock.System.now() + 10.seconds)
    }

    @Test
    fun `completes, stores results and runs the completion hook in the final transaction`() {
        var hookKey: String? = null
        val saga = engine(step("stock") { "STK-1" }, step("booking") { "RES-1" }, onCompleted = { hookKey = it.key })
            .start("item-1")

        assertEquals(SagaState.COMPLETED, saga.state)
        assertEquals("item-1", hookKey)
        val results = runBlocking { ExposedSagaTransactions(database).inTransaction { repository.results(saga.id) } }
        assertEquals(mapOf("stock" to "STK-1", "booking" to "RES-1"), results)
    }

    @Test
    fun `compensation marks results as compensated`() {
        val saga = engine(step("stock") { "STK-1" }, step("booking") { throw StepRejected("no") }).start("item-2")

        assertEquals(SagaState.FAILED, saga.state)
        val results = runBlocking { ExposedSagaTransactions(database).inTransaction { repository.results(saga.id) } }
        assertEquals(emptyMap<String, String?>(), results)
    }

    @Test
    fun `refuses a second unfinished saga for the same key`() {
        val engine = engine(step("stock") { throw StepNotSent("down") })
        engine.start("item-3")
        assertFailsWith<DuplicateSagaException> { engine.start("item-3") }
    }

    @Test
    fun `a duplicate saga leaves the caller's transaction usable`() {
        val engine = engine(step("stock") { throw StepNotSent("down") })
        engine.start("item-5")
        val after = transaction(database) {
            assertFailsWith<DuplicateSagaException> { engine.create("TEST", "item-5") }
            exec("SELECT 42") { it.next(); it.getInt(1) }                       // still usable: no aborted transaction
        }
        assertEquals(42, after)
    }

    @Test
    fun `waiting for a locked saga works even when the application defaults to REPEATABLE READ`() {
        val repeatableRead = Database.connect(
            url = postgres.jdbcUrl, user = postgres.username, password = postgres.password,
            databaseConfig = DatabaseConfig {
                defaultMaxAttempts = 1
                defaultIsolationLevel = Connection.TRANSACTION_REPEATABLE_READ
            },
        )
        val engine = SagaEngine(
            ExposedSagaTransactions(repeatableRead), repository,
            listOf(SagaDefinition("TEST", listOf(step("stock") { "STK-1" }))),
        )
        val id = transaction(repeatableRead) { engine.create("TEST", "item-6") }

        // Another transaction locks and updates the saga row, then commits while run() waits for the lock.
        val locked = CountDownLatch(1)
        val other = Thread {
            transaction(database) {
                val r = repository.lock(id)!!
                repository.save(r, Clock.System.now())
                locked.countDown()
                Thread.sleep(300)
            }
        }
        other.start()
        locked.await()
        val saga = runBlocking { engine.run(id, Clock.System.now() + 10.seconds) }
        other.join()
        assertEquals(SagaState.COMPLETED, saga.state)
    }

    @Test
    fun `purgeFinished deletes old finished sagas with their results and log, and keeps the rest`() {
        val done = engine(step("stock") { "STK-1" }, step("booking") { "RES-1" }).start("item-7")
        val pending = engine(step("stock") { throw StepNotSent("down") }).start("item-8")
        val recent = engine(step("stock") { "STK-2" }).start("item-9")
        // Age the first two by 40 days.
        transaction(database) {
            exec("UPDATE saga SET updated_at = updated_at - interval '40 days' WHERE id IN ('${done.id}', '${pending.id}')")
        }

        val deleted = runBlocking { engine(step("stock") { null }).purgeFinished(olderThan = 30.days) }

        assertEquals(1, deleted)
        transaction(database) {
            fun count(sql: String) = exec(sql) { it.next(); it.getLong(1) }
            assertEquals(0, count("SELECT count(*) FROM saga WHERE id = '${done.id}'"))
            assertEquals(0, count("SELECT count(*) FROM saga_step_result WHERE saga_id = '${done.id}'"))
            assertEquals(0, count("SELECT count(*) FROM saga_log WHERE saga_id = '${done.id}'"))
            assertEquals(2, count("SELECT count(*) FROM saga WHERE id IN ('${pending.id}', '${recent.id}')"))
        }
    }

    @Test
    fun `skip locked ignores a saga locked by another transaction`() {
        val saga = engine(step("stock") { throw StepNotSent("down") }).start("item-4")
        transaction(database) {
            assertNotNull(repository.lock(saga.id))                          // holds the row lock
            val other = Thread {
                transaction(database) { assertNull(repository.lock(saga.id, skipLocked = true)) }
            }
            other.start()
            other.join()
        }
    }
}
