package payout.domain

import io.kotest.matchers.shouldBe
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.Arguments
import org.junit.jupiter.params.provider.MethodSource
import payout.domain.PayoutStatus.FAILED
import payout.domain.PayoutStatus.REJECTED
import payout.domain.PayoutStatus.REQUESTED
import payout.domain.PayoutStatus.SENDING
import payout.domain.PayoutStatus.SENT
import payout.domain.PayoutStatus.SETTLED
import payout.domain.PayoutStatus.UNKNOWN

class PayoutStatusTest {

    @ParameterizedTest(name = "{0} -> {1} allowed={2}")
    @MethodSource("everyPair")
    fun `only the documented transitions are allowed`(from: PayoutStatus, to: PayoutStatus, allowed: Boolean) {
        from.canTransitionTo(to) shouldBe allowed
    }

    @ParameterizedTest(name = "{0} terminal={1}")
    @MethodSource("terminality")
    fun `settled, rejected and failed are terminal, unknown still waits for the bank`(status: PayoutStatus, terminal: Boolean) {
        status.isTerminal shouldBe terminal
    }

    companion object {
        private val ALLOWED = setOf(
            REQUESTED to SENDING,
            REQUESTED to FAILED,
            SENDING to SENT,
            SENDING to REJECTED,
            SENDING to UNKNOWN,
            SENT to SETTLED,
            SENT to REJECTED,
            UNKNOWN to SETTLED,
            UNKNOWN to REJECTED,
        )

        @JvmStatic
        fun everyPair(): List<Arguments> =
            PayoutStatus.entries.flatMap { from ->
                PayoutStatus.entries.map { to -> Arguments.of(from, to, (from to to) in ALLOWED) }
            }

        @JvmStatic
        fun terminality(): List<Arguments> = listOf(
            Arguments.of(REQUESTED, false),
            Arguments.of(SENDING, false),
            Arguments.of(SENT, false),
            Arguments.of(UNKNOWN, false),
            Arguments.of(SETTLED, true),
            Arguments.of(REJECTED, true),
            Arguments.of(FAILED, true),
        )
    }
}
