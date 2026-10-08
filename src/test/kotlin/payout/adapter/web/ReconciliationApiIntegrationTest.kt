package payout.adapter.web

import com.fasterxml.jackson.databind.ObjectMapper
import io.kotest.matchers.collections.shouldContainAll
import io.kotest.matchers.ints.shouldBeGreaterThanOrEqual
import io.kotest.matchers.shouldBe
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.test.web.servlet.MockMvc
import org.springframework.test.web.servlet.get
import payout.adapter.bank.FakeBankGateway
import payout.application.port.PayoutRepository
import payout.application.port.TransactionRunner
import payout.application.port.TransferOutcome
import payout.application.port.TransferRequest
import payout.config.ReconciliationProperties
import payout.domain.Money
import payout.domain.Payout
import payout.domain.reconciliation.BankStatementLine
import payout.support.IntegrationTest
import payout.support.TestData
import java.time.LocalDate
import java.util.UUID

@IntegrationTest
class ReconciliationApiIntegrationTest @Autowired constructor(
    private val mockMvc: MockMvc,
    private val objectMapper: ObjectMapper,
    private val payouts: PayoutRepository,
    private val bank: FakeBankGateway,
    private val tx: TransactionRunner,
    private val properties: ReconciliationProperties,
) {
    private fun stored(transform: (Payout) -> Payout): Payout {
        val requested = TestData.requestedPayout()
        tx.inTransaction { payouts.insert(requested) }
        val target = transform(requested)
        if (target != requested) tx.inTransaction { payouts.findByIdForUpdate(requested.id); payouts.update(target) }
        return target
    }

    private fun sentThroughBank(): Payout = stored {
        val sending = it.startSending(TestData.now())
        val accepted = bank.transfer(TransferRequest(it.id.toString(), it.beneficiary, it.money))
        sending.markSent((accepted as TransferOutcome.Accepted).bankReference, TestData.now())
    }

    private fun bankLine(reference: String, money: Money = Money.of(150_000, "KRW")) =
        BankStatementLine("INJECTED-${UUID.randomUUID()}", reference, money, TestData.now())

    @Test
    fun `the daily report matches clean payouts and lists every kind of mismatch`() {
        val matched = sentThroughBank()
        val missingInBank = stored { it.startSending(TestData.now()).markSent("NEVER-EXECUTED", TestData.now()) }
        val wrongAmount = stored { it.startSending(TestData.now()).markSent("BANK-X", TestData.now()) }
            .also { bank.injectStatementLine(bankLine(it.id.toString(), Money.of(149_000, "KRW"))) }
        val failedButPaid = stored { it.markFailed("INVALID_MESSAGE", TestData.now()) }
            .also { bank.injectStatementLine(bankLine(it.id.toString())) }
        val paidTwice = sentThroughBank().also { bank.injectStatementLine(bankLine(it.id.toString())) }
        val unknownReference = "unknown-${UUID.randomUUID()}".also { bank.injectStatementLine(bankLine(it)) }

        val today = LocalDate.now(properties.zone)
        val body = mockMvc.get("/reconciliation?date=$today").andExpect {
            status { isOk() }
            jsonPath("$.date") { value(today.toString()) }
            jsonPath("$.zone") { value(properties.zone.id) }
        }.andReturn().response.contentAsString

        val found = objectMapper.readTree(body)["discrepancies"].map { it["type"].asText() to it["payoutReference"].asText() }
        found shouldContainAll listOf(
            "MISSING_IN_BANK" to missingInBank.id.toString(),
            "AMOUNT_MISMATCH" to wrongAmount.id.toString(),
            "STATUS_MISMATCH" to failedButPaid.id.toString(),
            "DUPLICATE_IN_BANK" to paidTwice.id.toString(),
            "MISSING_INTERNALLY" to unknownReference,
        )
        found.none { it.second == matched.id.toString() } shouldBe true
        objectMapper.readTree(body)["matched"].asInt() shouldBeGreaterThanOrEqual 1
    }

    @Test
    fun `a missing or malformed date is a 400`() {
        mockMvc.get("/reconciliation").andExpect { status { isBadRequest() } }
        mockMvc.get("/reconciliation?date=2026-13-40").andExpect { status { isBadRequest() } }
    }
}
