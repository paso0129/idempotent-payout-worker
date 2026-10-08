package payout.application

import io.kotest.assertions.throwables.shouldThrow
import io.kotest.matchers.collections.shouldHaveSize
import io.kotest.matchers.shouldBe
import org.junit.jupiter.api.Test
import payout.adapter.bank.FakeBankBehavior.TIMEOUT_AFTER_EXECUTION
import payout.adapter.bank.FakeBankBehavior.TRANSIENT_ERROR
import payout.adapter.bank.FakeBankGateway
import payout.application.GiveUpReason.BANK_OUTCOME_UNKNOWN
import payout.application.GiveUpReason.RETRIES_EXHAUSTED_BANK_UNAVAILABLE
import payout.application.fakes.FakeTransactionRunner
import payout.application.fakes.InMemoryPayoutRepository
import payout.application.fakes.InMemoryProcessedMessages
import payout.domain.Payout
import payout.domain.PayoutFixtures
import payout.domain.PayoutFixtures.NOW
import payout.domain.PayoutId
import payout.domain.PayoutStatus
import java.time.Clock
import java.time.ZoneOffset

class GiveUpPayoutServiceTest {
    private val clock = Clock.fixed(NOW, ZoneOffset.UTC)
    private val payouts = InMemoryPayoutRepository()
    private val service = GiveUpPayoutService(payouts, FakeTransactionRunner(payouts), clock)

    private fun sending(): Payout = PayoutFixtures.requestedPayout().startSending(NOW).also(payouts::save)

    @Test
    fun `a timeout leaves the outcome unknown instead of failing the payout`() {
        val payout = sending()

        val result = service.giveUp(payout.id, BANK_OUTCOME_UNKNOWN)

        result.status shouldBe PayoutStatus.UNKNOWN
        result.failureReason shouldBe "BANK_OUTCOME_UNKNOWN"
        payouts.findById(payout.id) shouldBe result
    }

    @Test
    fun `a payout that reached SENDING always ends UNKNOWN, whatever the last error was`() {
        GiveUpReason.entries.forEach { reason ->
            val result = service.giveUp(sending().id, reason)

            result.status shouldBe PayoutStatus.UNKNOWN
            result.failureReason shouldBe reason.name
        }
    }

    @Test
    fun `money moved by an early timed-out attempt is not lost when later attempts only see the bank down`() {
        val processed = InMemoryProcessedMessages()
        val tx = FakeTransactionRunner(payouts, processed)
        val bank = FakeBankGateway(clock)
        val worker = ProcessPayoutService(payouts, processed, bank, tx, clock)
        val results = ApplyPayoutResultService(payouts, processed, tx, clock)
        val payout = PayoutFixtures.requestedPayout().also(payouts::save)
        bank.script(payout.id.toString(), TIMEOUT_AFTER_EXECUTION, TRANSIENT_ERROR, TRANSIENT_ERROR, TRANSIENT_ERROR)

        repeat(DELIVERY_ATTEMPTS) { runCatching { worker.process("msg-1", payout.id) } }
        val givenUp = GiveUpPayoutService(payouts, tx, clock).giveUp(payout.id, RETRIES_EXHAUSTED_BANK_UNAVAILABLE)

        givenUp.status shouldBe PayoutStatus.UNKNOWN
        val transfer = bank.executedTransfers().single()
        results.apply("settlement-1", PayoutResult(payout.id, BankResultOutcome.SETTLED, transfer.bankReference, null))
        payouts.findById(payout.id)?.status shouldBe PayoutStatus.SETTLED
        payouts.findById(payout.id)?.bankReference shouldBe transfer.bankReference
        bank.executedTransfers() shouldHaveSize 1
    }

    @Test
    fun `a payout that never reached SENDING never reached the bank and is failed whatever the reason`() {
        GiveUpReason.entries.forEach { reason ->
            val requested = PayoutFixtures.requestedPayout().also(payouts::save)

            service.giveUp(requested.id, reason).status shouldBe PayoutStatus.FAILED
        }
    }

    @Test
    fun `payouts past SENDING are left alone`() {
        val sent = PayoutFixtures.requestedPayout().startSending(NOW).markSent("BANK-REF-1", NOW).also(payouts::save)
        val unknown = PayoutFixtures.requestedPayout().startSending(NOW).markOutcomeUnknown("BANK_OUTCOME_UNKNOWN", NOW)
            .also(payouts::save)

        service.giveUp(sent.id, BANK_OUTCOME_UNKNOWN) shouldBe sent
        service.giveUp(unknown.id, RETRIES_EXHAUSTED_BANK_UNAVAILABLE) shouldBe unknown
    }

    @Test
    fun `giving up on an unknown payout is reported as not found`() {
        shouldThrow<PayoutNotFoundException> { service.giveUp(PayoutId.random(), BANK_OUTCOME_UNKNOWN) }
    }

    private companion object {
        const val DELIVERY_ATTEMPTS = 4
    }
}
