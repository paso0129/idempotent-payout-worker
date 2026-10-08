package payout.application

import payout.application.port.BankGateway
import payout.application.port.PayoutRepository
import payout.application.port.ProcessedMessageRepository
import payout.application.port.TransactionRunner
import payout.application.port.TransferOutcome
import payout.application.port.TransferRequest
import payout.domain.Payout
import payout.domain.PayoutId
import payout.domain.PayoutStatus
import java.time.Clock

sealed interface ProcessPayoutOutcome {
    /** This message id was handled before. */
    data object Duplicate : ProcessPayoutOutcome

    /** The payout is already past SENDING (another delivery finished it, or it was failed). */
    data class NothingToDo(val status: PayoutStatus) : ProcessPayoutOutcome

    /** The bank answered and the payout is now SENT or REJECTED. */
    data class Completed(val payout: Payout) : ProcessPayoutOutcome
}

/**
 * Handles one `payout.requested` delivery. Two transactions:
 *
 * 1. claim: REQUESTED -> SENDING and commit, so a crash during the bank call leaves a visible
 *    in-doubt state instead of a silent REQUESTED.
 * 2. send: lock the payout row, record the message id, call the bank, store SENT/REJECTED, commit.
 *    The row lock is held across the bank call on purpose: concurrent deliveries of the same payout
 *    queue behind it and then see the message (or the SENT state) already recorded.
 *
 * If step 2 dies after the bank moved the money, everything in it rolls back and the payout stays
 * SENDING. The redelivery calls the bank again with the same idempotency key (the payout id), and
 * the bank answers with the original transfer instead of making a new one.
 */
class ProcessPayoutService(
    private val payouts: PayoutRepository,
    private val processedMessages: ProcessedMessageRepository,
    private val bank: BankGateway,
    private val tx: TransactionRunner,
    private val clock: Clock,
) {
    fun process(messageId: String, payoutId: PayoutId): ProcessPayoutOutcome {
        claim(messageId, payoutId)?.let { return it }
        return tx.inTransaction { sendAndRecord(messageId, payoutId) }
    }

    /** Returns an outcome when there is nothing left to send, null when the payout is SENDING. */
    private fun claim(messageId: String, payoutId: PayoutId): ProcessPayoutOutcome? {
        if (processedMessages.exists(CONSUMER, messageId)) return ProcessPayoutOutcome.Duplicate
        return tx.inTransaction {
            val payout = lockPayout(payoutId)
            when (payout.status) {
                PayoutStatus.REQUESTED -> {
                    payouts.update(payout.startSending(clock.instant()))
                    null
                }
                // Earlier attempt crashed or timed out mid-call: resend with the same bank key.
                PayoutStatus.SENDING -> null
                // UNKNOWN included: retries were given up on; only the bank's result may close it now.
                PayoutStatus.SENT, PayoutStatus.SETTLED, PayoutStatus.REJECTED, PayoutStatus.FAILED, PayoutStatus.UNKNOWN ->
                    if (processedMessages.markProcessed(CONSUMER, messageId, clock.instant())) {
                        ProcessPayoutOutcome.NothingToDo(payout.status)
                    } else {
                        ProcessPayoutOutcome.Duplicate
                    }
            }
        }
    }

    private fun sendAndRecord(messageId: String, payoutId: PayoutId): ProcessPayoutOutcome {
        val payout = lockPayout(payoutId)
        if (!processedMessages.markProcessed(CONSUMER, messageId, clock.instant())) return ProcessPayoutOutcome.Duplicate
        if (payout.status != PayoutStatus.SENDING) return ProcessPayoutOutcome.NothingToDo(payout.status)

        val outcome = bank.transfer(TransferRequest(payout.id.toString(), payout.beneficiary, payout.money))
        val completed = when (outcome) {
            is TransferOutcome.Accepted -> payout.markSent(outcome.bankReference, clock.instant())
            is TransferOutcome.Rejected -> payout.markRejected(outcome.reasonCode, clock.instant())
        }
        payouts.update(completed)
        return ProcessPayoutOutcome.Completed(completed)
    }

    private fun lockPayout(payoutId: PayoutId): Payout =
        payouts.findByIdForUpdate(payoutId) ?: throw PayoutNotFoundException(payoutId)

    companion object {
        const val CONSUMER = "payout-worker"
    }
}
