package payout.support

import payout.domain.BeneficiaryAccount
import payout.domain.IdempotencyKey
import payout.domain.Money
import payout.domain.Payout
import payout.domain.PayoutId
import java.time.Instant
import java.time.temporal.ChronoUnit
import java.util.concurrent.ThreadLocalRandom

/** Fresh identifiers per test so integration tests can share one database without cleanup. */
object TestData {
    private const val ACCOUNT_MIN = 100_000_000_000L
    private const val ACCOUNT_MAX = 999_999_999_999L

    /** 12 random digits: collisions across one test run are negligible. */
    fun uniqueAccountNumber(): String = ThreadLocalRandom.current().nextLong(ACCOUNT_MIN, ACCOUNT_MAX).toString()

    fun beneficiary(accountNumber: String = uniqueAccountNumber()) =
        BeneficiaryAccount(bankCode = "BANK01", accountNumber = accountNumber, holderName = "Jane Doe")

    /** PostgreSQL keeps microseconds; truncating keeps round-tripped domain objects equal. */
    fun now(): Instant = Instant.now().truncatedTo(ChronoUnit.MICROS)

    fun requestedPayout(
        accountNumber: String = uniqueAccountNumber(),
        money: Money = Money.of(150_000, "KRW"),
    ): Payout {
        val id = PayoutId.random()
        return Payout.request(
            id = id,
            idempotencyKey = IdempotencyKey("it-$id"),
            requestFingerprint = "fingerprint-$id",
            beneficiary = beneficiary(accountNumber),
            money = money,
            at = now(),
        )
    }
}
