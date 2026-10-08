package payout.application

import io.kotest.assertions.throwables.shouldThrow
import io.kotest.matchers.shouldBe
import io.kotest.matchers.types.shouldBeInstanceOf
import org.junit.jupiter.api.Test
import payout.application.fakes.FakeTransactionRunner
import payout.application.fakes.InMemoryPayoutRepository
import payout.application.fakes.InMemoryProcessedMessages
import payout.domain.IllegalStatusTransitionException
import payout.domain.Payout
import payout.domain.PayoutFixtures
import payout.domain.PayoutFixtures.NOW
import payout.domain.PayoutId
import payout.domain.PayoutStatus
import java.time.Clock
import java.time.Duration
import java.time.ZoneOffset

class ApplyPayoutResultServiceTest {
    private val clock = Clock.fixed(NOW.plus(Duration.ofMinutes(1)), ZoneOffset.UTC)
    private val payouts = InMemoryPayoutRepository()
    private val processed = InMemoryProcessedMessages()
    private val service = ApplyPayoutResultService(payouts, processed, FakeTransactionRunner(payouts, processed), clock)

    private fun givenSentPayout(): Payout =
        PayoutFixtures.requestedPayout().startSending(NOW).markSent("BANK-REF-1", NOW).also(payouts::save)

    private fun current(id: PayoutId): Payout = checkNotNull(payouts.findById(id))

    private fun settled(id: PayoutId, bankReference: String? = "BANK-REF-1") =
        PayoutResult(id, BankResultOutcome.SETTLED, bankReference = bankReference, reasonCode = null)

    private fun rejected(id: PayoutId, reasonCode: String) =
        PayoutResult(id, BankResultOutcome.REJECTED, bankReference = null, reasonCode = reasonCode)

    private fun givenUnknownPayout(): Payout =
        PayoutFixtures.requestedPayout().startSending(NOW).markOutcomeUnknown("BANK_OUTCOME_UNKNOWN", NOW).also(payouts::save)

    @Test
    fun `a settled result moves a sent payout to SETTLED`() {
        val payout = givenSentPayout()

        val outcome = service.apply("result-1", settled(payout.id))

        outcome.shouldBeInstanceOf<ApplyResultOutcome.Applied>()
        current(payout.id).status shouldBe PayoutStatus.SETTLED
    }

    @Test
    fun `the same result message delivered twice is applied once`() {
        val payout = givenSentPayout()
        service.apply("result-1", settled(payout.id))
        val afterFirst = current(payout.id)

        val second = service.apply("result-1", settled(payout.id))

        second shouldBe ApplyResultOutcome.Duplicate
        current(payout.id) shouldBe afterFirst
    }

    @Test
    fun `the same outcome under a new message id is acknowledged without changing anything`() {
        val payout = givenSentPayout()
        service.apply("result-1", settled(payout.id))
        val afterFirst = current(payout.id)

        val second = service.apply("result-2", settled(payout.id))

        second shouldBe ApplyResultOutcome.AlreadyApplied
        current(payout.id) shouldBe afterFirst
    }

    @Test
    fun `a rejected result records the bank reason`() {
        val payout = givenSentPayout()

        service.apply("result-1", rejected(payout.id, "ACCOUNT_CLOSED"))

        current(payout.id).status shouldBe PayoutStatus.REJECTED
        current(payout.id).failureReason shouldBe "ACCOUNT_CLOSED"
    }

    @Test
    fun `a result that overtakes our own SENT commit is retried later instead of dropped`() {
        val payout = PayoutFixtures.requestedPayout().startSending(NOW).also(payouts::save)

        shouldThrow<PayoutNotSentYetException> { service.apply("result-1", settled(payout.id)) }

        current(payout.id).status shouldBe PayoutStatus.SENDING
        processed.exists(ApplyPayoutResultService.CONSUMER, "result-1") shouldBe false
    }

    @Test
    fun `a result that contradicts a terminal state is refused and not marked processed`() {
        val payout = givenSentPayout()
        service.apply("result-1", settled(payout.id))

        shouldThrow<IllegalStatusTransitionException> {
            service.apply("result-2", rejected(payout.id, "LATE_RETURN"))
        }

        current(payout.id).status shouldBe PayoutStatus.SETTLED
        processed.exists(ApplyPayoutResultService.CONSUMER, "result-2") shouldBe false
    }

    @Test
    fun `a late settlement resolves a payout whose outcome was unknown`() {
        val payout = givenUnknownPayout()

        val outcome = service.apply("result-1", settled(payout.id, bankReference = "BANK-REF-9"))

        outcome.shouldBeInstanceOf<ApplyResultOutcome.Applied>()
        current(payout.id).status shouldBe PayoutStatus.SETTLED
        current(payout.id).bankReference shouldBe "BANK-REF-9"
        current(payout.id).failureReason shouldBe null
    }

    @Test
    fun `a late rejection resolves a payout whose outcome was unknown`() {
        val payout = givenUnknownPayout()

        service.apply("result-1", rejected(payout.id, "ACCOUNT_CLOSED"))

        current(payout.id).status shouldBe PayoutStatus.REJECTED
        current(payout.id).failureReason shouldBe "ACCOUNT_CLOSED"
    }

    @Test
    fun `a settlement for a payout that never reached the bank is refused`() {
        val failed = PayoutFixtures.requestedPayout().markFailed("INVALID_MESSAGE", NOW).also(payouts::save)

        shouldThrow<IllegalStatusTransitionException> { service.apply("result-1", settled(failed.id)) }
    }

    @Test
    fun `a result for an unknown payout is reported as not found`() {
        shouldThrow<PayoutNotFoundException> { service.apply("result-1", settled(PayoutId.random())) }
    }
}
