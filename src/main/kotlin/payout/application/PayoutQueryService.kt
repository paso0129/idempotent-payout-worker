package payout.application

import payout.application.port.PayoutRepository
import payout.domain.Payout
import payout.domain.PayoutId

class PayoutQueryService(private val payouts: PayoutRepository) {
    fun get(id: PayoutId): Payout = payouts.findById(id) ?: throw PayoutNotFoundException(id)
}
