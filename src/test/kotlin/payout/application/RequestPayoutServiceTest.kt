package payout.application

import io.kotest.assertions.throwables.shouldThrow
import io.kotest.matchers.collections.shouldContainExactly
import io.kotest.matchers.collections.shouldHaveSize
import io.kotest.matchers.shouldBe
import io.kotest.matchers.shouldNotBe
import io.kotest.matchers.types.shouldBeInstanceOf
import io.mockk.every
import io.mockk.mockk
import org.junit.jupiter.api.Test
import payout.application.fakes.FakeTransactionRunner
import payout.application.fakes.InMemoryPayoutRepository
import payout.application.fakes.RecordingOutbox
import payout.application.port.PayoutRepository
import payout.domain.IdempotencyKey
import payout.domain.Money
import payout.domain.PayoutFixtures
import payout.domain.PayoutFixtures.NOW
import payout.domain.PayoutStatus
import java.time.Clock
import java.time.ZoneOffset

class RequestPayoutServiceTest {
    private val clock = Clock.fixed(NOW, ZoneOffset.UTC)
    private val payouts = InMemoryPayoutRepository()
    private val outbox = RecordingOutbox()
    private val service = RequestPayoutService(payouts, outbox, FakeTransactionRunner(payouts, outbox), clock)

    private fun command(key: String = "key-1", amountMinor: Long = 150_000) = RequestPayoutCommand(
        idempotencyKey = IdempotencyKey(key),
        beneficiary = PayoutFixtures.beneficiary(),
        money = Money.of(amountMinor, "KRW"),
    )

    @Test
    fun `a new key creates a REQUESTED payout and one outbox event in the same transaction`() {
        val result = service.request(command())

        result.shouldBeInstanceOf<RequestPayoutResult.Created>()
        result.payout.status shouldBe PayoutStatus.REQUESTED
        result.payout.createdAt shouldBe NOW
        payouts.all shouldHaveSize 1
        outbox.requested shouldContainExactly listOf(result.payout.id)
    }

    @Test
    fun `the same key with the same body replays the original payout without a second event`() {
        val first = service.request(command())

        val second = service.request(command())

        second.shouldBeInstanceOf<RequestPayoutResult.Replayed>()
        second.payout shouldBe first.payout
        payouts.all shouldHaveSize 1
        outbox.requested shouldHaveSize 1
    }

    @Test
    fun `the same key with a different body is a conflict`() {
        service.request(command(amountMinor = 150_000))

        shouldThrow<IdempotencyKeyConflictException> { service.request(command(amountMinor = 990_000)) }

        payouts.all shouldHaveSize 1
        outbox.requested shouldHaveSize 1
    }

    @Test
    fun `different keys create different payouts`() {
        val a = service.request(command(key = "key-a"))
        val b = service.request(command(key = "key-b"))

        a.payout.id shouldNotBe b.payout.id
        outbox.requested shouldHaveSize 2
    }

    @Test
    fun `losing an insert race to an identical request replays the winner`() {
        val winner = PayoutFixtures.requestedPayout(
            idempotencyKey = IdempotencyKey("key-1"),
            requestFingerprint = command().fingerprint(),
        )
        val racingRepository = racingRepository(winner)
        val racingService = RequestPayoutService(racingRepository, outbox, FakeTransactionRunner(outbox), clock)

        val result = racingService.request(command())

        result shouldBe RequestPayoutResult.Replayed(winner)
        outbox.requested shouldHaveSize 0
    }

    @Test
    fun `losing an insert race to a different body is a conflict`() {
        val winner = PayoutFixtures.requestedPayout(idempotencyKey = IdempotencyKey("key-1"), requestFingerprint = "other")
        val racingService = RequestPayoutService(racingRepository(winner), outbox, FakeTransactionRunner(outbox), clock)

        shouldThrow<IdempotencyKeyConflictException> { racingService.request(command()) }
    }

    /** First lookup sees nothing, the insert then hits the unique constraint because a concurrent request won. */
    private fun racingRepository(winner: payout.domain.Payout): PayoutRepository = mockk {
        every { findByIdempotencyKey(IdempotencyKey("key-1")) } returnsMany listOf(null, winner)
        every { insert(any()) } throws DuplicateIdempotencyKeyException(IdempotencyKey("key-1"))
    }
}

class RequestFingerprintTest {
    private fun command(
        accountNumber: String = "1234567890",
        amountMinor: Long = 150_000,
        currency: String = "KRW",
    ) = RequestPayoutCommand(
        idempotencyKey = IdempotencyKey("key-1"),
        beneficiary = PayoutFixtures.beneficiary(accountNumber),
        money = Money.of(amountMinor, currency),
    )

    @Test
    fun `identical requests have identical fingerprints`() {
        command().fingerprint() shouldBe command().fingerprint()
    }

    @Test
    fun `fingerprint is a sha-256 hex digest`() {
        command().fingerprint().length shouldBe 64
    }

    @Test
    fun `any business field change changes the fingerprint`() {
        val base = command().fingerprint()

        command(accountNumber = "1234567891").fingerprint() shouldNotBe base
        command(amountMinor = 150_001).fingerprint() shouldNotBe base
        command(currency = "USD").fingerprint() shouldNotBe base
    }

    @Test
    fun `the idempotency key itself is not part of the fingerprint`() {
        val other = command().copy(idempotencyKey = IdempotencyKey("key-2"))

        other.fingerprint() shouldBe command().fingerprint()
    }
}
