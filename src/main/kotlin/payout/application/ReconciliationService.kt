package payout.application

import payout.application.port.BankStatementSource
import payout.application.port.PayoutRepository
import payout.domain.reconciliation.Reconciler
import payout.domain.reconciliation.ReconciliationReport
import java.time.LocalDate
import java.time.ZoneId

/**
 * Compares one business day of internal payouts with the bank's executed transfers.
 * Both sides are cut at the same midnight, so a payout requested at 23:59 and executed at 00:01
 * shows up as a mismatch on both days; a real job would read a cut-off overlap window.
 */
class ReconciliationService(
    private val payouts: PayoutRepository,
    private val bankStatement: BankStatementSource,
    private val zone: ZoneId,
) {
    fun reconcile(businessDate: LocalDate): ReconciliationReport {
        val from = businessDate.atStartOfDay(zone).toInstant()
        val until = businessDate.plusDays(1).atStartOfDay(zone).toInstant()
        return Reconciler.reconcile(payouts.findCreatedBetween(from, until), bankStatement.statement(from, until))
    }
}
