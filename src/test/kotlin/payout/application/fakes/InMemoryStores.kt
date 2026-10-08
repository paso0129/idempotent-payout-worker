package payout.application.fakes

import payout.application.DuplicateIdempotencyKeyException
import payout.application.port.PayoutEventOutbox
import payout.application.port.PayoutRepository
import payout.application.port.ProcessedMessageRepository
import payout.application.port.TransactionRunner
import payout.domain.IdempotencyKey
import payout.domain.Payout
import payout.domain.PayoutId
import java.time.Instant

/** A store whose state the fake transaction can roll back. */
interface Snapshotting {
    fun snapshot(): Any

    fun restore(snapshot: Any)
}

/**
 * Runs the block and restores every registered store if it throws, which is all the unit tests
 * need from a database transaction. Locking is covered by the PostgreSQL integration tests.
 */
class FakeTransactionRunner(private vararg val stores: Snapshotting) : TransactionRunner {
    override fun <T> inTransaction(block: () -> T): T {
        val snapshots = stores.map { it to it.snapshot() }
        return try {
            block()
        } catch (e: Throwable) {
            snapshots.forEach { (store, snapshot) -> store.restore(snapshot) }
            throw e
        }
    }
}

class InMemoryPayoutRepository : PayoutRepository, Snapshotting {
    private var rows: Map<PayoutId, Payout> = emptyMap()

    val all: List<Payout> get() = rows.values.toList()

    fun save(payout: Payout) {
        rows = rows + (payout.id to payout)
    }

    override fun findById(id: PayoutId): Payout? = rows[id]

    override fun findByIdForUpdate(id: PayoutId): Payout? = rows[id]

    override fun findByIdempotencyKey(key: IdempotencyKey): Payout? = rows.values.firstOrNull { it.idempotencyKey == key }

    override fun insert(payout: Payout) {
        if (findByIdempotencyKey(payout.idempotencyKey) != null) throw DuplicateIdempotencyKeyException(payout.idempotencyKey)
        save(payout)
    }

    override fun update(payout: Payout) {
        check(rows.containsKey(payout.id)) { "no payout ${payout.id}" }
        save(payout)
    }

    override fun findCreatedBetween(from: Instant, until: Instant): List<Payout> =
        rows.values.filter { !it.createdAt.isBefore(from) && it.createdAt.isBefore(until) }

    override fun snapshot(): Any = rows

    @Suppress("UNCHECKED_CAST")
    override fun restore(snapshot: Any) {
        rows = snapshot as Map<PayoutId, Payout>
    }
}

class InMemoryProcessedMessages : ProcessedMessageRepository, Snapshotting {
    private var keys: Set<Pair<String, String>> = emptySet()

    override fun exists(consumer: String, messageId: String): Boolean = (consumer to messageId) in keys

    override fun markProcessed(consumer: String, messageId: String, at: Instant): Boolean {
        if (exists(consumer, messageId)) return false
        keys = keys + (consumer to messageId)
        return true
    }

    override fun snapshot(): Any = keys

    @Suppress("UNCHECKED_CAST")
    override fun restore(snapshot: Any) {
        keys = snapshot as Set<Pair<String, String>>
    }
}

class RecordingOutbox : PayoutEventOutbox, Snapshotting {
    var requested: List<PayoutId> = emptyList()
        private set

    override fun payoutRequested(payout: Payout) {
        requested = requested + payout.id
    }

    override fun snapshot(): Any = requested

    @Suppress("UNCHECKED_CAST")
    override fun restore(snapshot: Any) {
        requested = snapshot as List<PayoutId>
    }
}
