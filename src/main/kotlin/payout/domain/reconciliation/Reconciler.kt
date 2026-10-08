package payout.domain.reconciliation

import payout.domain.Money
import payout.domain.Payout
import payout.domain.PayoutStatus
import java.time.Instant

/** One executed transfer as the bank reports it. `payoutReference` is the idempotency key we sent. */
data class BankStatementLine(
    val bankReference: String,
    val payoutReference: String,
    val money: Money,
    val executedAt: Instant,
)

enum class DiscrepancyType {
    /** We recorded SENT/SETTLED, the bank has no transfer. */
    MISSING_IN_BANK,

    /** The bank executed a transfer we have no payout for. */
    MISSING_INTERNALLY,

    /** Both sides have it, with a different amount or currency. */
    AMOUNT_MISMATCH,

    /** The bank executed the same payout more than once. */
    DUPLICATE_IN_BANK,

    /** Money moved, but we hold the payout as REQUESTED, SENDING, REJECTED or FAILED. */
    STATUS_MISMATCH,

    /**
     * We gave up without knowing whether money moved. A clean bank record makes it a candidate to
     * confirm as SETTLED; no record means an operator checks (the bank may still be processing).
     */
    OUTCOME_UNKNOWN,
}

data class Discrepancy(
    val type: DiscrepancyType,
    val payoutReference: String,
    val detail: String,
    /** Only for OUTCOME_UNKNOWN: exactly one bank transfer with the right amount was found. */
    val settlementCandidate: Boolean = false,
)

data class ReconciliationReport(
    val matched: Int,
    val discrepancies: List<Discrepancy>,
) {
    val isClean: Boolean get() = discrepancies.isEmpty()
}

object Reconciler {
    private val EXPECTED_AT_BANK = setOf(PayoutStatus.SENT, PayoutStatus.SETTLED)

    fun reconcile(payouts: List<Payout>, statement: List<BankStatementLine>): ReconciliationReport {
        val linesByReference = statement.groupBy { it.payoutReference }
        val comparisons = payouts.map { it to compare(it, linesByReference[it.id.toString()].orEmpty()) }
        val internalReferences = payouts.map { it.id.toString() }.toSet()
        val unknown = linesByReference
            .filterKeys { it !in internalReferences }
            .map { (reference, lines) ->
                Discrepancy(DiscrepancyType.MISSING_INTERNALLY, reference, "bank executed ${lines.describe()}")
            }

        val matched = comparisons.count { (payout, found) -> found.isEmpty() && payout.status in EXPECTED_AT_BANK }
        return ReconciliationReport(matched, comparisons.flatMap { it.second } + unknown)
    }

    private fun compare(payout: Payout, lines: List<BankStatementLine>): List<Discrepancy> = when {
        payout.status == PayoutStatus.UNKNOWN -> compareUnknown(payout, lines)
        payout.status in EXPECTED_AT_BANK && lines.isEmpty() ->
            listOf(Discrepancy(DiscrepancyType.MISSING_IN_BANK, payout.id.toString(), "internal ${payout.status}, no bank transfer"))
        payout.status in EXPECTED_AT_BANK -> lineProblems(payout, lines)
        lines.isEmpty() -> emptyList()
        else -> listOf(
            Discrepancy(DiscrepancyType.STATUS_MISMATCH, payout.id.toString(), "internal ${payout.status}, bank executed ${lines.describe()}"),
        )
    }

    private fun compareUnknown(payout: Payout, lines: List<BankStatementLine>): List<Discrepancy> {
        val problems = lineProblems(payout, lines)
        val candidate = lines.isNotEmpty() && problems.isEmpty()
        val detail = when {
            lines.isEmpty() -> "no bank record yet; operator review, the bank may still be processing"
            candidate -> "bank executed ${lines.describe()}; confirm as SETTLED"
            else -> "bank records do not match cleanly: ${lines.describe()}"
        }
        return listOf(Discrepancy(DiscrepancyType.OUTCOME_UNKNOWN, payout.id.toString(), detail, settlementCandidate = candidate)) +
            problems
    }

    /** Duplicates and wrong amounts among the bank lines for one payout. */
    private fun lineProblems(payout: Payout, lines: List<BankStatementLine>): List<Discrepancy> {
        val reference = payout.id.toString()
        val duplicate = if (lines.size > 1) {
            Discrepancy(DiscrepancyType.DUPLICATE_IN_BANK, reference, "executed ${lines.size} times: ${lines.describe()}")
        } else {
            null
        }
        val wrongAmount = lines.firstOrNull { it.money != payout.money }?.let {
            Discrepancy(DiscrepancyType.AMOUNT_MISMATCH, reference, "internal ${payout.money}, bank ${it.money} (${it.bankReference})")
        }
        return listOfNotNull(duplicate, wrongAmount)
    }

    private fun List<BankStatementLine>.describe(): String = joinToString { "${it.bankReference} ${it.money}" }
}
