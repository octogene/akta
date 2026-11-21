package com.example.saga.exposed

import com.example.saga.DuplicateSagaException
import com.example.saga.KotlinxJsonSagaSerializer
import com.example.saga.SagaContext
import com.example.saga.SagaDefinition
import com.example.saga.SagaEngine
import com.example.saga.SagaState
import com.example.saga.StepNotSent
import com.example.saga.StepRejected

import com.example.saga.saga
import kotlinx.coroutines.runBlocking
import kotlinx.serialization.Serializable
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

    @Serializable
    data class Request(val customerId: String, val start: String)

    @Serializable
    data class Hold(val id: String, val warehouse: String)

    private val serializer = KotlinxJsonSagaSerializer()

    /** A saga of steps returning a reference each, by step name. */
    private fun definition(vararg steps: Pair<String, () -> String>, onCompleted: SagaContext<Unit>.() -> Unit = {}) =
        saga("TEST") {
            for ((name, block) in steps) step<String>(name) { execute { block() } }
            onCompleted(onCompleted)
        }

    private var current: SagaDefinition<Unit>? = null

    private fun engine(vararg steps: Pair<String, () -> String>, onCompleted: SagaContext<Unit>.() -> Unit = {}): SagaEngine {
        val definition = definition(*steps, onCompleted = onCompleted).also { current = it }
        return SagaEngine(ExposedSagaTransactions(database), repository, listOf(definition), serializer)
    }

    private fun SagaEngine.start(key: String) = runBlocking {
        val id = ExposedSagaTransactions(database).inTransaction { create(current!!, key) }
        run(id, Clock.System.now() + 10.seconds)
    }

    private fun storedResults(id: kotlin.uuid.Uuid) =
        runBlocking { ExposedSagaTransactions(database).inTransaction { repository.results(id) } }

    @Test
    fun `completes, stores results and runs the completion hook in the final transaction`() {
        var hookKey: String? = null
        val saga = engine("stock" to { "STK-1" }, "booking" to { "RES-1" }, onCompleted = { hookKey = key })
            .start("item-1")

        assertEquals(SagaState.COMPLETED, saga.state)
        assertEquals("item-1", hookKey)
        assertEquals(mapOf("stock" to "\"STK-1\"", "booking" to "\"RES-1\""), storedResults(saga.id))
    }

    @Test
    fun `compensation marks results as compensated`() {
        val saga = engine("stock" to { "STK-1" }, "booking" to { throw StepRejected("no") }).start("item-2")

        assertEquals(SagaState.FAILED, saga.state)
        assertEquals(emptyMap<String, String?>(), storedResults(saga.id))
    }

    @Test
    fun `refuses a second unfinished saga for the same key`() {
        val engine = engine("stock" to { throw StepNotSent("down") })
        engine.start("item-3")
        assertFailsWith<DuplicateSagaException> { engine.start("item-3") }
    }

    @Test
    fun `a duplicate saga leaves the caller's transaction usable`() {
        val engine = engine("stock" to { throw StepNotSent("down") })
        engine.start("item-5")
        val after = transaction(database) {
            assertFailsWith<DuplicateSagaException> { engine.create(current!!, "item-5") }
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
        val definition = definition("stock" to { "STK-1" })
        val engine = SagaEngine(ExposedSagaTransactions(repeatableRead), repository, listOf(definition), serializer)
        val id = transaction(repeatableRead) { engine.create(definition, "item-6") }

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
        val done = engine("stock" to { "STK-1" }, "booking" to { "RES-1" }).start("item-7")
        val pending = engine("stock" to { throw StepNotSent("down") }).start("item-8")
        val recent = engine("stock" to { "STK-2" }).start("item-9")
        // Age the first two by 40 days.
        transaction(database) {
            exec("UPDATE saga SET updated_at = updated_at - interval '40 days' WHERE id IN ('${done.id}', '${pending.id}')")
        }

        val deleted = runBlocking { engine("stock" to { "unused" }).purgeFinished(olderThan = 30.days) }

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
    fun `a typed input and typed outputs round-trip through PostgreSQL`() {
        var confirmed: Pair<Request, Hold>? = null
        val definition = saga<Request>("BOOKING") {
            val hold = step<Hold>("hold") { execute { Hold("H-1", "north") } }
            step<String>("booking") { execute { "${input.customerId}@${requireResult(hold).warehouse}" } }
            onCompleted { confirmed = input to requireResult(hold) }
        }
        val engine = SagaEngine(ExposedSagaTransactions(database), repository, listOf(definition), serializer)
        val id = runBlocking {
            ExposedSagaTransactions(database).inTransaction { engine.create(definition, "item-10", Request("C-1", "2026-12-01")) }
        }

        val saga = runBlocking { engine.run(id, Clock.System.now() + 10.seconds) }

        assertEquals(SagaState.COMPLETED, saga.state)
        assertEquals(Request("C-1", "2026-12-01") to Hold("H-1", "north"), confirmed)
        assertEquals("""{"customerId":"C-1","start":"2026-12-01"}""", saga.input)
        assertEquals(
            mapOf("hold" to """{"id":"H-1","warehouse":"north"}""", "booking" to "\"C-1@north\""),
            storedResults(id),
        )
    }

    @Test
    fun `skip locked ignores a saga locked by another transaction`() {
        val saga = engine("stock" to { throw StepNotSent("down") }).start("item-4")
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
