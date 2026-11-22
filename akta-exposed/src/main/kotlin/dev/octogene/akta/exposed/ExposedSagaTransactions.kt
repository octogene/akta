package dev.octogene.akta.exposed

import dev.octogene.akta.SagaTransactions
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import org.jetbrains.exposed.v1.jdbc.Database
import org.jetbrains.exposed.v1.jdbc.transactions.transaction
import java.sql.Connection

/**
 * Short Exposed transactions on the IO dispatcher, so Ktor threads are never blocked.
 *
 * Exposed retries a transaction up to 3 times on any SQLException by default. Connect the
 * [database] with `DatabaseConfig { defaultMaxAttempts = 1 }` to keep failures visible.
 * If your application already has its own transaction helper, implement [SagaTransactions] with it instead.
 */
class ExposedSagaTransactions(private val database: Database? = null) : SagaTransactions {
    override suspend fun <T> inTransaction(block: () -> T): T =
        withContext(Dispatchers.IO) {
            // The engine's row locking relies on READ COMMITTED: under REPEATABLE READ, a SELECT FOR UPDATE
            // that waits for another transaction fails with a serialization error instead of waiting.
            transaction(db = database, transactionIsolation = Connection.TRANSACTION_READ_COMMITTED) { block() }
        }
}
