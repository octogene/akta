package dev.octogene.akta.exposed

import dev.octogene.akta.SagaTransactions
import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import org.jetbrains.exposed.v1.jdbc.Database
import org.jetbrains.exposed.v1.jdbc.transactions.transaction
import java.sql.Connection

/**
 * Short Exposed transactions on [dispatcher], [Dispatchers.IO] by default, so Ktor threads are never blocked.
 *
 * Exposed retries a transaction up to 3 times on any SQLException by default. Connect the
 * [database] with `DatabaseConfig { defaultMaxAttempts = 1 }` to keep failures visible.
 * If your application already has its own transaction helper, implement [SagaTransactions] with it instead.
 *
 * [dispatcher] is where the blocking JDBC work runs. On Java 21+, an application that runs on virtual threads
 * can pass `Executors.newVirtualThreadPerTaskExecutor().asCoroutineDispatcher()`. Unlike [Dispatchers.IO], it
 * sets no limit on threads: under load, transactions then queue for a connection in the pool (and fail after its
 * connection timeout) instead of for a thread, so keep the engine's callers bounded, as `resumeDue` is. The engine
 * needs each transaction's block to run on one thread, which any dispatcher does: the block does not suspend.
 */
public class ExposedSagaTransactions @JvmOverloads constructor(
    private val database: Database? = null,
    private val dispatcher: CoroutineDispatcher = Dispatchers.IO,
) : SagaTransactions {
    override suspend fun <T> inTransaction(block: () -> T): T =
        withContext(dispatcher) {
            // The engine's row locking relies on READ COMMITTED: under REPEATABLE READ, a SELECT FOR UPDATE
            // that waits for another transaction fails with a serialization error instead of waiting.
            transaction(db = database, transactionIsolation = Connection.TRANSACTION_READ_COMMITTED) { block() }
        }
}
