package payout.application.port

import payout.domain.IdempotencyKey
import payout.domain.Payout
import payout.domain.PayoutId
import java.time.Instant

interface PayoutRepository {
    fun findById(id: PayoutId): Payout?

    /** `SELECT ... FOR UPDATE`: the row stays locked until the surrounding transaction ends. */
    fun findByIdForUpdate(id: PayoutId): Payout?

    fun findByIdempotencyKey(key: IdempotencyKey): Payout?

    /** @throws payout.application.DuplicateIdempotencyKeyException when another payout already owns the key. */
    fun insert(payout: Payout)

    fun update(payout: Payout)

    fun findCreatedBetween(from: Instant, until: Instant): List<Payout>
}

/** Records integration events in the caller's transaction; a relay publishes them later. */
interface PayoutEventOutbox {
    fun payoutRequested(payout: Payout)
}

/** Per-consumer record of handled message ids, backed by a unique constraint. */
interface ProcessedMessageRepository {
    fun exists(consumer: String, messageId: String): Boolean

    /** Joins the caller's transaction. Returns false when the message was already recorded. */
    fun markProcessed(consumer: String, messageId: String, at: Instant): Boolean
}

interface TransactionRunner {
    fun <T> inTransaction(block: () -> T): T
}
