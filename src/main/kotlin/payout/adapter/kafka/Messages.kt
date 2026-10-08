package payout.adapter.kafka

import java.util.UUID

object Topics {
    const val PAYOUT_REQUESTED = "payout.requested"
    const val PAYOUT_RESULT = "payout.result"
}

object MessageHeaders {
    /** Stable id of the logical message. Survives outbox re-publishing and retry-topic hops. */
    const val MESSAGE_ID = "message-id"
}

/** Deliberately thin: the worker reloads the payout from the database, so no account data travels on Kafka. */
data class PayoutRequestedMessage(
    val payoutId: UUID,
    val amountMinor: Long,
    val currency: String,
)

data class PayoutResultMessage(
    val payoutId: UUID,
    val outcome: String,
    val bankReference: String?,
    val reasonCode: String?,
)

/** Malformed record (no message id, unparseable payload). Never retried. */
class InvalidMessageException(message: String, cause: Throwable? = null) : RuntimeException(message, cause)
