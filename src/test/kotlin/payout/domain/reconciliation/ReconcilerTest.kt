package payout.domain.reconciliation

import io.kotest.matchers.collections.shouldBeEmpty
import io.kotest.matchers.collections.shouldContainExactly
import io.kotest.matchers.shouldBe
import io.kotest.matchers.string.shouldContain
import org.junit.jupiter.api.Test
import payout.domain.Money
import payout.domain.Payout
import payout.domain.PayoutFixtures
import payout.domain.PayoutFixtures.NOW
import payout.domain.reconciliation.DiscrepancyType.AMOUNT_MISMATCH
import payout.domain.reconciliation.DiscrepancyType.DUPLICATE_IN_BANK
import payout.domain.reconciliation.DiscrepancyType.MISSING_IN_BANK
import payout.domain.reconciliation.DiscrepancyType.MISSING_INTERNALLY
import payout.domain.reconciliation.DiscrepancyType.OUTCOME_UNKNOWN
import payout.domain.reconciliation.DiscrepancyType.STATUS_MISMATCH

class ReconcilerTest {
    private fun sent(money: Money = Money.of(150_000, "KRW")): Payout =
        PayoutFixtures.requestedPayout(money = money).startSending(NOW).markSent("BANK-${NOW.epochSecond}", NOW)

    private fun lineFor(payout: Payout, money: Money = payout.money, bankReference: String = "BANK-1") =
        BankStatementLine(bankReference, payout.id.toString(), money, NOW)

    private fun Collection<Discrepancy>.types() = map { it.type }

    @Test
    fun `sent and settled payouts that the bank executed for the same amount match`() {
        val sent = sent()
        val settled = sent().markSettled(NOW)

        val report = Reconciler.reconcile(listOf(sent, settled), listOf(lineFor(sent), lineFor(settled)))

        report.matched shouldBe 2
        report.discrepancies.shouldBeEmpty()
        report.isClean shouldBe true
    }

    @Test
    fun `a payout we think was sent but the bank never executed is missing in bank`() {
        val sent = sent()

        val report = Reconciler.reconcile(listOf(sent), emptyList())

        report.discrepancies.types() shouldContainExactly listOf(MISSING_IN_BANK)
        report.discrepancies.single().payoutReference shouldBe sent.id.toString()
    }

    @Test
    fun `a bank transfer we have no payout for is missing internally`() {
        val stranger = BankStatementLine("BANK-9", "unknown-reference", Money.of(1_000, "KRW"), NOW)

        val report = Reconciler.reconcile(emptyList(), listOf(stranger))

        report.discrepancies.types() shouldContainExactly listOf(MISSING_INTERNALLY)
    }

    @Test
    fun `a different amount or currency on the bank side is an amount mismatch`() {
        val krw = sent()
        val usd = sent(Money.of(1_050, "USD"))

        val report = Reconciler.reconcile(
            listOf(krw, usd),
            listOf(lineFor(krw, money = Money.of(149_000, "KRW")), lineFor(usd, money = Money.of(1_050, "EUR"))),
        )

        report.discrepancies.types() shouldContainExactly listOf(AMOUNT_MISMATCH, AMOUNT_MISMATCH)
        report.matched shouldBe 0
    }

    @Test
    fun `the same payout executed twice by the bank is a duplicate and not counted as matched`() {
        val sent = sent()

        val report = Reconciler.reconcile(listOf(sent), listOf(lineFor(sent, bankReference = "A"), lineFor(sent, bankReference = "B")))

        report.discrepancies.types() shouldContainExactly listOf(DUPLICATE_IN_BANK)
        report.matched shouldBe 0
    }

    @Test
    fun `a wrong amount on any of several bank lines is reported`() {
        val sent = sent()

        val report = Reconciler.reconcile(
            listOf(sent),
            listOf(lineFor(sent, bankReference = "A"), lineFor(sent, money = Money.of(1, "KRW"), bankReference = "B")),
        )

        report.discrepancies.types() shouldContainExactly listOf(DUPLICATE_IN_BANK, AMOUNT_MISMATCH)
    }

    @Test
    fun `an unknown reference executed several times is reported once`() {
        val first = BankStatementLine("BANK-8", "unknown-reference", Money.of(1_000, "KRW"), NOW)
        val second = first.copy(bankReference = "BANK-9")

        val report = Reconciler.reconcile(emptyList(), listOf(first, second))

        report.discrepancies.types() shouldContainExactly listOf(MISSING_INTERNALLY)
        report.discrepancies.single().detail shouldContain "BANK-8"
        report.discrepancies.single().detail shouldContain "BANK-9"
    }

    @Test
    fun `money moved for a payout we consider failed, rejected or still in doubt is a status mismatch`() {
        val failed = PayoutFixtures.requestedPayout().markFailed("INVALID_MESSAGE", NOW)
        val rejected = PayoutFixtures.requestedPayout().startSending(NOW).markRejected("ACCOUNT_CLOSED", NOW)
        val inDoubt = PayoutFixtures.requestedPayout().startSending(NOW)

        val report = Reconciler.reconcile(
            listOf(failed, rejected, inDoubt),
            listOf(lineFor(failed), lineFor(rejected), lineFor(inDoubt)),
        )

        report.discrepancies.types() shouldContainExactly listOf(STATUS_MISMATCH, STATUS_MISMATCH, STATUS_MISMATCH)
    }

    private fun unknown(): Payout =
        PayoutFixtures.requestedPayout().startSending(NOW).markOutcomeUnknown("BANK_OUTCOME_UNKNOWN", NOW)

    @Test
    fun `an unknown payout the bank executed once for the same amount is a candidate to confirm as settled`() {
        val payout = unknown()

        val report = Reconciler.reconcile(listOf(payout), listOf(lineFor(payout)))

        report.discrepancies.types() shouldContainExactly listOf(OUTCOME_UNKNOWN)
        report.discrepancies.single().settlementCandidate shouldBe true
        report.matched shouldBe 0
    }

    @Test
    fun `an unknown payout without a bank record is left for an operator, never failed`() {
        val payout = unknown()

        val report = Reconciler.reconcile(listOf(payout), emptyList())

        report.discrepancies.types() shouldContainExactly listOf(OUTCOME_UNKNOWN)
        report.discrepancies.single().settlementCandidate shouldBe false
    }

    @Test
    fun `an unknown payout with a suspicious bank record is not a settlement candidate`() {
        val payout = unknown()

        val report = Reconciler.reconcile(listOf(payout), listOf(lineFor(payout, money = Money.of(1, "KRW"))))

        report.discrepancies.types() shouldContainExactly listOf(OUTCOME_UNKNOWN, AMOUNT_MISMATCH)
        report.discrepancies.first().settlementCandidate shouldBe false
    }

    @Test
    fun `payouts that never reached the bank and have no bank line are consistent`() {
        val requested = PayoutFixtures.requestedPayout()
        val failed = PayoutFixtures.requestedPayout().markFailed("retries exhausted", NOW)
        val inDoubt = PayoutFixtures.requestedPayout().startSending(NOW)

        Reconciler.reconcile(listOf(requested, failed, inDoubt), emptyList()).isClean shouldBe true
    }
}
