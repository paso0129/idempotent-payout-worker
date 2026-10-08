package payout.domain

import java.time.Instant

/** One instruction to move money to a beneficiary. Immutable: every transition returns a copy. */
data class Payout(
    val id: PayoutId,
    val idempotencyKey: IdempotencyKey,
    val requestFingerprint: String,
    val beneficiary: BeneficiaryAccount,
    val money: Money,
    val status: PayoutStatus,
    val bankReference: String?,
    val failureReason: String?,
    val createdAt: Instant,
    val updatedAt: Instant,
) {
    init {
        require(money.amountMinor > 0) { "payout amount must be positive: $money" }
    }

    fun startSending(at: Instant): Payout = transitionTo(PayoutStatus.SENDING, at)

    fun markSent(bankReference: String, at: Instant): Payout =
        transitionTo(PayoutStatus.SENT, at).copy(bankReference = bankReference)

    /**
     * Settling an UNKNOWN payout is how the doubt gets resolved: the reason is cleared and the bank
     * reference from the result is taken, since we never learned it ourselves.
     */
    fun markSettled(at: Instant, bankReference: String? = null): Payout =
        transitionTo(PayoutStatus.SETTLED, at).copy(
            bankReference = this.bankReference ?: bankReference,
            failureReason = null,
        )

    fun markRejected(reason: String, at: Instant): Payout =
        transitionTo(PayoutStatus.REJECTED, at).copy(failureReason = reason)

    fun markFailed(reason: String, at: Instant): Payout =
        transitionTo(PayoutStatus.FAILED, at).copy(failureReason = reason)

    /** Retries stopped without a definite bank answer: the money may or may not have moved. */
    fun markOutcomeUnknown(reason: String, at: Instant): Payout =
        transitionTo(PayoutStatus.UNKNOWN, at).copy(failureReason = reason)

    private fun transitionTo(next: PayoutStatus, at: Instant): Payout {
        if (!status.canTransitionTo(next)) throw IllegalStatusTransitionException(id, status, next)
        return copy(status = next, updatedAt = at)
    }

    companion object {
        fun request(
            id: PayoutId,
            idempotencyKey: IdempotencyKey,
            requestFingerprint: String,
            beneficiary: BeneficiaryAccount,
            money: Money,
            at: Instant,
        ): Payout = Payout(
            id = id,
            idempotencyKey = idempotencyKey,
            requestFingerprint = requestFingerprint,
            beneficiary = beneficiary,
            money = money,
            status = PayoutStatus.REQUESTED,
            bankReference = null,
            failureReason = null,
            createdAt = at,
            updatedAt = at,
        )
    }
}

class IllegalStatusTransitionException(
    val payoutId: PayoutId,
    val from: PayoutStatus,
    val to: PayoutStatus,
) : RuntimeException("payout $payoutId cannot move from $from to $to")
