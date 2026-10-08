package payout.adapter.web

import jakarta.validation.constraints.NotBlank
import jakarta.validation.constraints.NotNull
import jakarta.validation.constraints.Pattern
import jakarta.validation.constraints.Positive
import jakarta.validation.constraints.Size
import payout.application.RequestPayoutCommand
import payout.domain.BeneficiaryAccount
import payout.domain.IdempotencyKey
import payout.domain.Money
import payout.domain.Payout
import java.time.Instant
import java.util.UUID

/** `amountMinor` is in the currency's minor unit: 150000 KRW, or 1050 for 10.50 USD. */
data class PayoutRequestBody(
    @field:NotBlank
    @field:Pattern(regexp = "^[A-Za-z0-9]{2,11}$")
    val bankCode: String?,
    @field:NotBlank
    @field:Pattern(regexp = "^[A-Za-z0-9-]{4,34}$")
    val accountNumber: String?,
    @field:NotBlank
    @field:Size(max = 70)
    val accountHolder: String?,
    @field:NotNull
    @field:Positive
    val amountMinor: Long?,
    @field:NotNull
    @field:Pattern(regexp = "^[A-Z]{3}$")
    val currency: String?,
) {
    /** Bean Validation already ran; domain constructors re-check what annotations cannot (ISO currency). */
    fun toCommand(idempotencyKey: String): RequestPayoutCommand = try {
        RequestPayoutCommand(
            idempotencyKey = IdempotencyKey(idempotencyKey),
            beneficiary = BeneficiaryAccount(
                bankCode = requireNotNull(bankCode),
                accountNumber = requireNotNull(accountNumber),
                holderName = requireNotNull(accountHolder),
            ),
            money = Money.of(requireNotNull(amountMinor), requireNotNull(currency)),
        )
    } catch (e: IllegalArgumentException) {
        throw InvalidPayoutRequestException(e.message ?: "invalid payout request", e)
    }
}

class InvalidPayoutRequestException(message: String, cause: Throwable) : RuntimeException(message, cause)

data class PayoutResponse(
    val payoutId: UUID,
    val status: String,
    val amountMinor: Long,
    val currency: String,
    val bankCode: String,
    val accountNumber: String,
    val bankReference: String?,
    val failureReason: String?,
    val createdAt: Instant,
    val updatedAt: Instant,
) {
    companion object {
        fun from(payout: Payout) = PayoutResponse(
            payoutId = payout.id.value,
            status = payout.status.name,
            amountMinor = payout.money.amountMinor,
            currency = payout.money.currency.currencyCode,
            bankCode = payout.beneficiary.bankCode,
            accountNumber = payout.beneficiary.maskedAccountNumber,
            bankReference = payout.bankReference,
            failureReason = payout.failureReason,
            createdAt = payout.createdAt,
            updatedAt = payout.updatedAt,
        )
    }
}
