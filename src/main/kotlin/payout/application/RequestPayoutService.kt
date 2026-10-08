package payout.application

import payout.application.port.PayoutEventOutbox
import payout.application.port.PayoutRepository
import payout.application.port.TransactionRunner
import payout.domain.BeneficiaryAccount
import payout.domain.IdempotencyKey
import payout.domain.Money
import payout.domain.Payout
import payout.domain.PayoutId
import java.security.MessageDigest
import java.time.Clock

data class RequestPayoutCommand(
    val idempotencyKey: IdempotencyKey,
    val beneficiary: BeneficiaryAccount,
    val money: Money,
) {
    /**
     * SHA-256 over the business fields in a fixed order, so JSON field order or whitespace in the
     * HTTP body does not turn an honest retry into a conflict. The key itself is excluded.
     */
    fun fingerprint(): String {
        val canonical = listOf(
            beneficiary.bankCode,
            beneficiary.accountNumber,
            beneficiary.holderName,
            money.amountMinor.toString(),
            money.currency.currencyCode,
        ).joinToString(FIELD_SEPARATOR)
        return MessageDigest.getInstance("SHA-256")
            .digest(canonical.toByteArray(Charsets.UTF_8))
            .joinToString("") { "%02x".format(it) }
    }

    private companion object {
        const val FIELD_SEPARATOR = "\u001F"
    }
}

sealed interface RequestPayoutResult {
    val payout: Payout

    data class Created(override val payout: Payout) : RequestPayoutResult

    data class Replayed(override val payout: Payout) : RequestPayoutResult
}

class RequestPayoutService(
    private val payouts: PayoutRepository,
    private val outbox: PayoutEventOutbox,
    private val tx: TransactionRunner,
    private val clock: Clock,
) {
    fun request(command: RequestPayoutCommand): RequestPayoutResult {
        val fingerprint = command.fingerprint()
        replayIfKnown(command.idempotencyKey, fingerprint)?.let { return it }
        return try {
            tx.inTransaction { create(command, fingerprint) }
        } catch (e: DuplicateIdempotencyKeyException) {
            // A concurrent request with the same key committed between our lookup and insert.
            replayIfKnown(command.idempotencyKey, fingerprint) ?: throw e
        }
    }

    private fun create(command: RequestPayoutCommand, fingerprint: String): RequestPayoutResult {
        val payout = Payout.request(
            id = PayoutId.random(),
            idempotencyKey = command.idempotencyKey,
            requestFingerprint = fingerprint,
            beneficiary = command.beneficiary,
            money = command.money,
            at = clock.instant(),
        )
        payouts.insert(payout)
        outbox.payoutRequested(payout)
        return RequestPayoutResult.Created(payout)
    }

    private fun replayIfKnown(key: IdempotencyKey, fingerprint: String): RequestPayoutResult? {
        val existing = payouts.findByIdempotencyKey(key) ?: return null
        if (existing.requestFingerprint != fingerprint) throw IdempotencyKeyConflictException(key)
        return RequestPayoutResult.Replayed(existing)
    }
}
