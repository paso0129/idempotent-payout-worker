package payout.application

import payout.application.port.PayoutRepository
import payout.application.port.TransactionRunner
import payout.domain.Payout
import payout.domain.PayoutId
import payout.domain.PayoutStatus
import java.time.Clock

/**
 * Why delivery attempts stopped, named after the last error. Stored as the payout's reason code and
 * shown by the API; it does not decide the status.
 */
enum class GiveUpReason {
    RETRIES_EXHAUSTED_BANK_UNAVAILABLE,
    BANK_OUTCOME_UNKNOWN,
    INVALID_MESSAGE,
    PROCESSING_FAILED,
}

/** Used by the dead-letter handler once retries are exhausted. */
class GiveUpPayoutService(
    private val payouts: PayoutRepository,
    private val tx: TransactionRunner,
    private val clock: Clock,
) {
    /**
     * REQUESTED never reached the bank, so it is FAILED. SENDING may have, whatever the last error
     * says (an earlier attempt can have moved the money before the bank went down), so it is UNKNOWN
     * until the bank's result or an operator settles it. Payouts further along are returned unchanged.
     */
    fun giveUp(payoutId: PayoutId, reason: GiveUpReason): Payout = tx.inTransaction {
        val payout = payouts.findByIdForUpdate(payoutId) ?: throw PayoutNotFoundException(payoutId)
        val now = clock.instant()
        val updated = when (payout.status) {
            PayoutStatus.REQUESTED -> payout.markFailed(reason.name, now)
            PayoutStatus.SENDING -> payout.markOutcomeUnknown(reason.name, now)
            PayoutStatus.SENT, PayoutStatus.SETTLED, PayoutStatus.REJECTED, PayoutStatus.FAILED, PayoutStatus.UNKNOWN ->
                return@inTransaction payout
        }
        updated.also(payouts::update)
    }
}
