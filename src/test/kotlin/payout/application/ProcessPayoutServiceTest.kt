package payout.application

import io.kotest.assertions.throwables.shouldThrow
import io.kotest.matchers.collections.shouldHaveSize
import io.kotest.matchers.shouldBe
import io.kotest.matchers.types.shouldBeInstanceOf
import org.junit.jupiter.api.Test
import payout.adapter.bank.FakeBankBehavior.REJECT
import payout.adapter.bank.FakeBankBehavior.SUCCESS
import payout.adapter.bank.FakeBankBehavior.TIMEOUT_AFTER_EXECUTION
import payout.adapter.bank.FakeBankBehavior.TRANSIENT_ERROR
import payout.adapter.bank.FakeBankGateway
import payout.application.fakes.FakeTransactionRunner
import payout.application.fakes.InMemoryPayoutRepository
import payout.application.fakes.InMemoryProcessedMessages
import payout.application.port.BankGateway
import payout.application.port.BankTimeoutException
import payout.application.port.TransferOutcome
import payout.application.port.TransferRequest
import payout.application.port.TransientBankException
import payout.domain.Payout
import payout.domain.PayoutFixtures
import payout.domain.PayoutFixtures.NOW
import payout.domain.PayoutId
import payout.domain.PayoutStatus
import java.time.Clock
import java.time.ZoneOffset

class ProcessPayoutServiceTest {
    private val clock = Clock.fixed(NOW, ZoneOffset.UTC)
    private val payouts = InMemoryPayoutRepository()
    private val processed = InMemoryProcessedMessages()
    private val bank = FakeBankGateway(clock)
    private val tx = FakeTransactionRunner(payouts, processed)
    private val service = ProcessPayoutService(payouts, processed, bank, tx, clock)

    private fun givenRequestedPayout(): Payout = PayoutFixtures.requestedPayout().also(payouts::save)

    private fun current(id: PayoutId): Payout = checkNotNull(payouts.findById(id))

    @Test
    fun `first delivery sends the payout and marks the message processed`() {
        val payout = givenRequestedPayout()

        val outcome = service.process("msg-1", payout.id)

        outcome.shouldBeInstanceOf<ProcessPayoutOutcome.Completed>()
        current(payout.id).status shouldBe PayoutStatus.SENT
        current(payout.id).bankReference shouldBe outcome.payout.bankReference
        processed.exists(ProcessPayoutService.CONSUMER, "msg-1") shouldBe true
        bank.callCount(payout.id.toString()) shouldBe 1
    }

    @Test
    fun `the bank idempotency key is the payout id, not the message id`() {
        val payout = givenRequestedPayout()

        service.process("msg-1", payout.id)

        bank.receivedRequests().single().idempotencyKey shouldBe payout.id.toString()
    }

    @Test
    fun `duplicate delivery of the same message calls the bank once`() {
        val payout = givenRequestedPayout()

        service.process("msg-1", payout.id)
        val second = service.process("msg-1", payout.id)

        second shouldBe ProcessPayoutOutcome.Duplicate
        bank.callCount(payout.id.toString()) shouldBe 1
    }

    @Test
    fun `a different message for an already sent payout does not call the bank again`() {
        val payout = givenRequestedPayout()
        service.process("msg-1", payout.id)

        val outcome = service.process("msg-2", payout.id)

        outcome shouldBe ProcessPayoutOutcome.NothingToDo(PayoutStatus.SENT)
        bank.callCount(payout.id.toString()) shouldBe 1
        processed.exists(ProcessPayoutService.CONSUMER, "msg-2") shouldBe true
    }

    @Test
    fun `a permanent bank rejection marks the payout REJECTED and is not retried`() {
        val payout = givenRequestedPayout()
        bank.script(payout.id.toString(), REJECT)

        val outcome = service.process("msg-1", payout.id)

        outcome.shouldBeInstanceOf<ProcessPayoutOutcome.Completed>()
        current(payout.id).status shouldBe PayoutStatus.REJECTED
        current(payout.id).failureReason shouldBe FakeBankGateway.REJECT_REASON
        processed.exists(ProcessPayoutService.CONSUMER, "msg-1") shouldBe true
    }

    @Test
    fun `a transient bank error leaves the payout SENDING and the message unprocessed`() {
        val payout = givenRequestedPayout()
        bank.script(payout.id.toString(), TRANSIENT_ERROR, SUCCESS)

        shouldThrow<TransientBankException> { service.process("msg-1", payout.id) }

        current(payout.id).status shouldBe PayoutStatus.SENDING
        processed.exists(ProcessPayoutService.CONSUMER, "msg-1") shouldBe false

        service.process("msg-1", payout.id)

        current(payout.id).status shouldBe PayoutStatus.SENT
        bank.callCount(payout.id.toString()) shouldBe 2
    }

    @Test
    fun `a timeout after the bank already moved the money is resolved without a second transfer`() {
        val payout = givenRequestedPayout()
        bank.script(payout.id.toString(), TIMEOUT_AFTER_EXECUTION)

        shouldThrow<BankTimeoutException> { service.process("msg-1", payout.id) }
        current(payout.id).status shouldBe PayoutStatus.SENDING

        service.process("msg-1", payout.id)

        current(payout.id).status shouldBe PayoutStatus.SENT
        bank.callCount(payout.id.toString()) shouldBe 2
        bank.executedTransfers() shouldHaveSize 1
        current(payout.id).bankReference shouldBe bank.executedTransfers().single().bankReference
    }

    @Test
    fun `a crash after the bank call but before commit does not move money twice on redelivery`() {
        val payout = givenRequestedPayout()
        val crashingOnce = CrashAfterBankCall(bank)
        val crashingService = ProcessPayoutService(payouts, processed, crashingOnce, tx, clock)

        shouldThrow<SimulatedCrash> { crashingService.process("msg-1", payout.id) }
        current(payout.id).status shouldBe PayoutStatus.SENDING
        processed.exists(ProcessPayoutService.CONSUMER, "msg-1") shouldBe false

        crashingService.process("msg-1", payout.id)

        current(payout.id).status shouldBe PayoutStatus.SENT
        bank.callCount(payout.id.toString()) shouldBe 2
        bank.executedTransfers() shouldHaveSize 1
    }

    @Test
    fun `a payout already failed by the dead letter handler is never sent`() {
        val payout = PayoutFixtures.requestedPayout().markFailed("retries exhausted", NOW).also(payouts::save)

        val outcome = service.process("msg-1", payout.id)

        outcome shouldBe ProcessPayoutOutcome.NothingToDo(PayoutStatus.FAILED)
        bank.callCount(payout.id.toString()) shouldBe 0
    }

    @Test
    fun `a payout whose outcome is unknown is not resent by a late redelivery`() {
        val payout = PayoutFixtures.requestedPayout().startSending(NOW).markOutcomeUnknown("BANK_OUTCOME_UNKNOWN", NOW)
            .also(payouts::save)

        val outcome = service.process("msg-1", payout.id)

        outcome shouldBe ProcessPayoutOutcome.NothingToDo(PayoutStatus.UNKNOWN)
        bank.callCount(payout.id.toString()) shouldBe 0
    }

    @Test
    fun `an unknown payout is reported as not found`() {
        shouldThrow<PayoutNotFoundException> { service.process("msg-1", PayoutId.random()) }
    }

    private class SimulatedCrash : RuntimeException("process died before the database commit")

    /** Lets the bank do its work, then dies once before our transaction commits. */
    private class CrashAfterBankCall(private val delegate: BankGateway) : BankGateway {
        private var crashed = false

        override fun transfer(request: TransferRequest): TransferOutcome {
            val outcome = delegate.transfer(request)
            if (!crashed) {
                crashed = true
                throw SimulatedCrash()
            }
            return outcome
        }
    }
}
