package payout.application

import io.kotest.assertions.throwables.shouldThrow
import io.kotest.matchers.shouldBe
import org.junit.jupiter.api.Test
import payout.application.fakes.InMemoryPayoutRepository
import payout.domain.PayoutFixtures
import payout.domain.PayoutId

class PayoutQueryServiceTest {
    private val payouts = InMemoryPayoutRepository()
    private val service = PayoutQueryService(payouts)

    @Test
    fun `returns the stored payout`() {
        val payout = PayoutFixtures.requestedPayout().also(payouts::save)

        service.get(payout.id) shouldBe payout
    }

    @Test
    fun `an unknown id is reported as not found`() {
        shouldThrow<PayoutNotFoundException> { service.get(PayoutId.random()) }
    }
}
