package dev.octogene.akta

import dev.octogene.akta.memory.InMemorySagaRepository
import java.sql.SQLException

/**
 * A store that refuses to log [event] while [refusing], from its [from]-th time on: the transaction recording the
 * outcome with that event fails, as one a constraint refused would. What RECORD_FAILED is for. Build the engine's
 * transactions on [inner], so that a refused transaction is rolled back.
 */
internal class RefusingLog(
    private val inner: InMemorySagaRepository,
    private val event: String,
    private val from: Int = 1,
) : SagaRepository by inner {
    var refusing = true
    private var seen = 0

    override fun log(entry: SagaLogEntry) {
        if (refusing && entry.event == event && ++seen >= from) throw SQLException("a constraint refused the $event entry")
        inner.log(entry)
    }
}
