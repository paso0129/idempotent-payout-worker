package payout.domain

import java.time.Instant

object PayoutFixtures {
    val NOW: Instant = Instant.parse("2026-10-08T03:00:00Z")

    fun beneficiary(accountNumber: String = "1234567890"): BeneficiaryAccount =
        BeneficiaryAccount(bankCode = "BANK01", accountNumber = accountNumber, holderName = "Jane Doe")

    fun requestedPayout(
        id: PayoutId = PayoutId.random(),
        idempotencyKey: IdempotencyKey = IdempotencyKey("key-${id.value}"),
        money: Money = Money.of(150_000, "KRW"),
        requestFingerprint: String = "fingerprint-${id.value}",
        at: Instant = NOW,
    ): Payout = Payout.request(
        id = id,
        idempotencyKey = idempotencyKey,
        requestFingerprint = requestFingerprint,
        beneficiary = beneficiary(),
        money = money,
        at = at,
    )
}
