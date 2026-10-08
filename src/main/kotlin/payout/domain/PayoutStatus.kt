package payout.domain

/**
 * REQUESTED -> SENDING -> SENT -> SETTLED
 *                 |         \-> REJECTED (returned by the bank after acceptance)
 *                 |-> REJECTED (bank refused the transfer)
 *                 \-> UNKNOWN  (retries exhausted without a definite answer) -> SETTLED / REJECTED
 * REQUESTED -> FAILED          (given up before the bank was ever called)
 *
 * SENDING is the in-doubt marker while retries are still running: it is committed before the bank
 * call, so a crash mid-call leaves a visible state that the next delivery resumes with the same
 * bank idempotency key. Once a payout reached SENDING, no failure we can observe proves the money
 * did not move, so giving up leads to UNKNOWN, never FAILED. Only the bank's result (or an operator
 * after reconciliation) closes it.
 */
enum class PayoutStatus {
    REQUESTED,
    SENDING,
    SENT,
    SETTLED,
    REJECTED,
    FAILED,
    UNKNOWN,
    ;

    val isTerminal: Boolean
        get() = allowedNext().isEmpty()

    fun canTransitionTo(next: PayoutStatus): Boolean = next in allowedNext()

    private fun allowedNext(): Set<PayoutStatus> = when (this) {
        REQUESTED -> setOf(SENDING, FAILED)
        SENDING -> setOf(SENT, REJECTED, UNKNOWN)
        SENT -> setOf(SETTLED, REJECTED)
        UNKNOWN -> setOf(SETTLED, REJECTED)
        SETTLED, REJECTED, FAILED -> emptySet()
    }
}
