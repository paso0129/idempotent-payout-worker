package payout.application

import payout.application.port.PayoutRepository
import payout.application.port.ProcessedMessageRepository
import payout.application.port.TransactionRunner
import payout.domain.Payout
import payout.domain.PayoutId
import payout.domain.PayoutStatus
import java.time.Clock

enum class BankResultOutcome(val targetStatus: PayoutStatus) {
    SETTLED(PayoutStatus.SETTLED),
    REJECTED(PayoutStatus.REJECTED),
}

/** What the bank finally did with a transfer it accepted earlier. */
data class PayoutResult(
    val payoutId: PayoutId,
    val outcome: BankResultOutcome,
    val bankReference: String?,
    val reasonCode: String?,
)

sealed interface ApplyResultOutcome {
    /** This result message id was handled before. */
    data object Duplicate : ApplyResultOutcome

    /** A different message carried the same outcome; the payout already reflects it. */
    data object AlreadyApplied : ApplyResultOutcome

    data class Applied(val payout: Payout) : ApplyResultOutcome
}

/** Consumes `payout.result`. The message id record and the transition commit together. */
class ApplyPayoutResultService(
    private val payouts: PayoutRepository,
    private val processedMessages: ProcessedMessageRepository,
    private val tx: TransactionRunner,
    private val clock: Clock,
) {
    fun apply(messageId: String, result: PayoutResult): ApplyResultOutcome = tx.inTransaction {
        val payout = payouts.findByIdForUpdate(result.payoutId) ?: throw PayoutNotFoundException(result.payoutId)
        if (!processedMessages.markProcessed(CONSUMER, messageId, clock.instant())) return@inTransaction ApplyResultOutcome.Duplicate
        if (payout.status == result.outcome.targetStatus) return@inTransaction ApplyResultOutcome.AlreadyApplied
        if (payout.status == PayoutStatus.REQUESTED || payout.status == PayoutStatus.SENDING) {
            throw PayoutNotSentYetException(payout.id, payout.status)
        }
        // SENT or UNKNOWN: for UNKNOWN this result is what finally tells us whether money moved.
        val updated = when (result.outcome) {
            BankResultOutcome.SETTLED -> payout.markSettled(clock.instant(), result.bankReference)
            BankResultOutcome.REJECTED -> payout.markRejected(result.reasonCode ?: UNSPECIFIED_REASON, clock.instant())
        }
        payouts.update(updated)
        ApplyResultOutcome.Applied(updated)
    }

    companion object {
        const val CONSUMER = "payout-result-applier"
        private const val UNSPECIFIED_REASON = "UNSPECIFIED"
    }
}
