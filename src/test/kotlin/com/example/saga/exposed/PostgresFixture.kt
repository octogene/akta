package com.example.saga.exposed

import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import org.jetbrains.exposed.v1.core.DatabaseConfig
import org.jetbrains.exposed.v1.jdbc.Database
import org.jetbrains.exposed.v1.jdbc.JdbcTransaction
import org.jetbrains.exposed.v1.jdbc.transactions.transaction
import org.testcontainers.postgresql.PostgreSQLContainer

/**
 * PostgreSQL in Docker for the benchmark and stress tests: a connection pool (as in production),
 * pg_stat_statements to count statements, and the engine's schema reset on demand.
 *
 * @param durable true keeps PostgreSQL's real commit cost (fsync); false is faster, for correctness tests.
 */
class PostgresFixture(private val durable: Boolean) : AutoCloseable {
    private val container = PostgreSQLContainer("postgres:16-alpine").withCommand(
        "postgres",
        "-c", "shared_preload_libraries=pg_stat_statements",
        "-c", "max_connections=200",
        "-c", "fsync=${if (durable) "on" else "off"}",
        "-c", "synchronous_commit=${if (durable) "on" else "off"}",
    )
    private lateinit var dataSource: HikariDataSource
    lateinit var database: Database
        private set

    fun start(poolSize: Int): PostgresFixture {
        container.start()
        dataSource = HikariDataSource(HikariConfig().apply {
            jdbcUrl = container.jdbcUrl
            username = container.username
            password = container.password
            maximumPoolSize = poolSize
        })
        database = Database.connect(dataSource, databaseConfig = DatabaseConfig { defaultMaxAttempts = 1 })
        sql { exec("CREATE EXTENSION IF NOT EXISTS pg_stat_statements") }
        return this
    }

    /** Drops and recreates the engine's tables, plus [extraDdl] (e.g. a business table used by hooks). */
    fun resetSchema(extraDdl: String = "") {
        val schema = javaClass.getResource("/com/example/saga/schema.sql")!!.readText()
        sql {
            exec("DROP TABLE IF EXISTS saga_log, saga_signal, saga_step_result, saga, item")
            exec(schema)
            if (extraDdl.isNotBlank()) exec(extraDdl)
        }
    }

    fun <T> sql(block: JdbcTransaction.() -> T): T = transaction(database) { block() }

    /** Runs [query] and maps every row with [row]. */
    fun <T> query(query: String, row: (java.sql.ResultSet) -> T): List<T> = sql {
        exec(query) { rs -> buildList { while (rs.next()) add(row(rs)) } } ?: emptyList()
    }

    fun long(query: String): Long = query(query) { it.getLong(1) }.single()

    override fun close() {
        if (::dataSource.isInitialized) dataSource.close()
        container.stop()
    }
}
