package payout.domain

import io.kotest.assertions.throwables.shouldThrow
import io.kotest.matchers.shouldBe
import io.kotest.matchers.string.shouldContain
import org.junit.jupiter.api.Test
import payout.domain.PayoutFixtures.NOW
import payout.domain.PayoutFixtures.requestedPayout
import java.time.Duration

class PayoutTest {
    private val later = NOW.plus(Duration.ofSeconds(5))

    @Test
    fun `a new payout starts as REQUESTED`() {
        val payout = requestedPayout()

        payout.status shouldBe PayoutStatus.REQUESTED
        payout.bankReference shouldBe null
        payout.createdAt shouldBe NOW
        payout.updatedAt shouldBe NOW
    }

    @Test
    fun `transitions return a new instance and leave the original untouched`() {
        val requested = requestedPayout()

        val sending = requested.startSending(later)

        sending.status shouldBe PayoutStatus.SENDING
        sending.updatedAt shouldBe later
        requested.status shouldBe PayoutStatus.REQUESTED
        requested.updatedAt shouldBe NOW
    }

    @Test
    fun `happy path REQUESTED - SENDING - SENT - SETTLED keeps the bank reference`() {
        val settled = requestedPayout()
            .startSending(later)
            .markSent("BANK-REF-1", later)
            .markSettled(later)

        settled.status shouldBe PayoutStatus.SETTLED
        settled.bankReference shouldBe "BANK-REF-1"
    }

    @Test
    fun `bank rejection while sending records the reason`() {
        val rejected = requestedPayout().startSending(later).markRejected("ACCOUNT_CLOSED", later)

        rejected.status shouldBe PayoutStatus.REJECTED
        rejected.failureReason shouldBe "ACCOUNT_CLOSED"
    }

    @Test
    fun `a sent payout can still be returned by the bank`() {
        val returned = requestedPayout().startSending(later).markSent("BANK-REF-1", later)
            .markRejected("RETURNED_BY_BENEFICIARY_BANK", later)

        returned.status shouldBe PayoutStatus.REJECTED
        returned.bankReference shouldBe "BANK-REF-1"
    }

    @Test
    fun `only a payout that never reached the bank can fail`() {
        requestedPayout().markFailed("INVALID_MESSAGE", later).status shouldBe PayoutStatus.FAILED
        shouldThrow<IllegalStatusTransitionException> {
            requestedPayout().startSending(later).markFailed("RETRIES_EXHAUSTED_BANK_UNAVAILABLE", later)
        }
    }

    @Test
    fun `a payout whose bank outcome is unknown records why`() {
        val unknown = requestedPayout().startSending(later).markOutcomeUnknown("BANK_OUTCOME_UNKNOWN", later)

        unknown.status shouldBe PayoutStatus.UNKNOWN
        unknown.failureReason shouldBe "BANK_OUTCOME_UNKNOWN"
    }

    @Test
    fun `a late settlement resolves an unknown payout, taking the bank reference and clearing the reason`() {
        val unknown = requestedPayout().startSending(later).markOutcomeUnknown("BANK_OUTCOME_UNKNOWN", later)

        val settled = unknown.markSettled(later, bankReference = "BANK-REF-7")

        settled.status shouldBe PayoutStatus.SETTLED
        settled.bankReference shouldBe "BANK-REF-7"
        settled.failureReason shouldBe null
    }

    @Test
    fun `a late rejection resolves an unknown payout`() {
        val unknown = requestedPayout().startSending(later).markOutcomeUnknown("BANK_OUTCOME_UNKNOWN", later)

        val rejected = unknown.markRejected("ACCOUNT_CLOSED", later)

        rejected.status shouldBe PayoutStatus.REJECTED
        rejected.failureReason shouldBe "ACCOUNT_CLOSED"
    }

    @Test
    fun `settling a sent payout keeps its own bank reference`() {
        val sent = requestedPayout().startSending(later).markSent("BANK-REF-1", later)

        sent.markSettled(later, bankReference = "OTHER").bankReference shouldBe "BANK-REF-1"
    }

    @Test
    fun `only a payout handed to the bank can become unknown, and unknown is never failed automatically`() {
        shouldThrow<IllegalStatusTransitionException> { requestedPayout().markOutcomeUnknown("x", later) }
        val unknown = requestedPayout().startSending(later).markOutcomeUnknown("x", later)
        shouldThrow<IllegalStatusTransitionException> { unknown.markFailed("x", later) }
        shouldThrow<IllegalStatusTransitionException> { unknown.markSent("BANK-REF-1", later) }
    }

    @Test
    fun `settling a payout that was never sent is rejected`() {
        val payout = requestedPayout()

        val error = shouldThrow<IllegalStatusTransitionException> { payout.markSettled(later) }

        error.from shouldBe PayoutStatus.REQUESTED
        error.to shouldBe PayoutStatus.SETTLED
        error.message shouldContain payout.id.toString()
    }

    @Test
    fun `sending twice is rejected`() {
        val sending = requestedPayout().startSending(later)

        shouldThrow<IllegalStatusTransitionException> { sending.startSending(later) }
    }

    @Test
    fun `terminal payouts cannot move again`() {
        val settled = requestedPayout().startSending(later).markSent("BANK-REF-1", later).markSettled(later)

        shouldThrow<IllegalStatusTransitionException> { settled.markRejected("late", later) }
        shouldThrow<IllegalStatusTransitionException> { settled.markFailed("late", later) }
    }

    @Test
    fun `a payout must move a positive amount`() {
        shouldThrow<IllegalArgumentException> { requestedPayout(money = Money.of(0, "KRW")) }
    }
}
