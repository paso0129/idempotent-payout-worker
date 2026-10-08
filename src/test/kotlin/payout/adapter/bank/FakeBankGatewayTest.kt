package payout.adapter.bank

import io.kotest.assertions.throwables.shouldThrow
import io.kotest.matchers.collections.shouldBeEmpty
import io.kotest.matchers.collections.shouldHaveSize
import io.kotest.matchers.shouldBe
import io.kotest.matchers.types.shouldBeInstanceOf
import org.junit.jupiter.api.Test
import payout.adapter.bank.FakeBankBehavior.REJECT
import payout.adapter.bank.FakeBankBehavior.SUCCESS
import payout.adapter.bank.FakeBankBehavior.TIMEOUT_AFTER_EXECUTION
import payout.adapter.bank.FakeBankBehavior.TIMEOUT_BEFORE_EXECUTION
import payout.adapter.bank.FakeBankBehavior.TRANSIENT_ERROR
import payout.application.port.BankTimeoutException
import payout.application.port.TransferOutcome
import payout.application.port.TransferRequest
import payout.application.port.TransientBankException
import payout.domain.Money
import payout.domain.PayoutFixtures
import payout.domain.PayoutFixtures.NOW
import java.time.Clock
import java.time.ZoneOffset
import java.util.concurrent.Callable
import java.util.concurrent.Executors

class FakeBankGatewayTest {
    private val bank = FakeBankGateway(Clock.fixed(NOW, ZoneOffset.UTC))

    private fun request(key: String = "payout-1", amountMinor: Long = 150_000) =
        TransferRequest(key, PayoutFixtures.beneficiary(), Money.of(amountMinor, "KRW"))

    @Test
    fun `accepts a transfer by default and records it in the ledger`() {
        val outcome = bank.transfer(request())

        outcome.shouldBeInstanceOf<TransferOutcome.Accepted>()
        bank.executedTransfers().single().bankReference shouldBe outcome.bankReference
        bank.executedTransfers().single().executedAt shouldBe NOW
    }

    @Test
    fun `a repeated idempotency key returns the original outcome without moving money again`() {
        val first = bank.transfer(request())
        val second = bank.transfer(request())

        second shouldBe first
        bank.callCount("payout-1") shouldBe 2
        bank.executedTransfers() shouldHaveSize 1
    }

    @Test
    fun `reusing a key for a different transfer is refused`() {
        bank.transfer(request(amountMinor = 150_000))

        bank.transfer(request(amountMinor = 990_000)) shouldBe
            TransferOutcome.Rejected(FakeBankGateway.KEY_REUSED_REASON)
        bank.executedTransfers() shouldHaveSize 1
    }

    @Test
    fun `scripted outcomes are played in order and then fall back to success`() {
        bank.script("payout-1", TRANSIENT_ERROR, TIMEOUT_BEFORE_EXECUTION)

        shouldThrow<TransientBankException> { bank.transfer(request()) }
        shouldThrow<BankTimeoutException> { bank.transfer(request()) }
        bank.executedTransfers().shouldBeEmpty()

        bank.transfer(request()).shouldBeInstanceOf<TransferOutcome.Accepted>()
    }

    @Test
    fun `behaviour can be scripted per beneficiary account before the payout id is known`() {
        bank.scriptForAccount(PayoutFixtures.beneficiary().accountNumber, TRANSIENT_ERROR)

        shouldThrow<TransientBankException> { bank.transfer(request(key = "unknown-in-advance")) }
        bank.transfer(request(key = "unknown-in-advance")).shouldBeInstanceOf<TransferOutcome.Accepted>()
    }

    @Test
    fun `a key script wins over an account script`() {
        bank.scriptForAccount(PayoutFixtures.beneficiary().accountNumber, TRANSIENT_ERROR)
        bank.script("payout-1", REJECT)

        bank.transfer(request()) shouldBe TransferOutcome.Rejected(FakeBankGateway.REJECT_REASON)
    }

    @Test
    fun `a rejection is remembered for the key`() {
        bank.script("payout-1", REJECT)

        bank.transfer(request()) shouldBe TransferOutcome.Rejected(FakeBankGateway.REJECT_REASON)
        bank.transfer(request()) shouldBe TransferOutcome.Rejected(FakeBankGateway.REJECT_REASON)
        bank.executedTransfers().shouldBeEmpty()
    }

    @Test
    fun `a timeout after execution moves the money but loses the response`() {
        bank.script("payout-1", TIMEOUT_AFTER_EXECUTION)

        shouldThrow<BankTimeoutException> { bank.transfer(request()) }

        bank.executedTransfers() shouldHaveSize 1
        bank.transfer(request()) shouldBe TransferOutcome.Accepted(bank.executedTransfers().single().bankReference)
    }

    @Test
    fun `a request can time out on the way even when the bank already executed that key`() {
        bank.script("payout-1", TIMEOUT_AFTER_EXECUTION, TIMEOUT_BEFORE_EXECUTION, TRANSIENT_ERROR)

        shouldThrow<BankTimeoutException> { bank.transfer(request()) }
        shouldThrow<BankTimeoutException> { bank.transfer(request()) }
        shouldThrow<TransientBankException> { bank.transfer(request()) }

        bank.executedTransfers() shouldHaveSize 1
        bank.transfer(request()) shouldBe TransferOutcome.Accepted(bank.executedTransfers().single().bankReference)
    }

    @Test
    fun `concurrent calls with the same key execute one transfer`() {
        bank.script("payout-1", SUCCESS)
        val pool = Executors.newFixedThreadPool(CONCURRENT_CALLS)

        val outcomes = pool.invokeAll((1..CONCURRENT_CALLS).map { Callable { bank.transfer(request()) } }).map { it.get() }
        pool.shutdown()

        outcomes.toSet() shouldHaveSize 1
        bank.executedTransfers() shouldHaveSize 1
    }

    private companion object {
        const val CONCURRENT_CALLS = 8
    }
}
