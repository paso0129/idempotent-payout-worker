package payout.domain

import java.util.UUID

@JvmInline
value class PayoutId(val value: UUID) {
    override fun toString(): String = value.toString()

    companion object {
        fun random(): PayoutId = PayoutId(UUID.randomUUID())

        fun parse(text: String): PayoutId = PayoutId(UUID.fromString(text))
    }
}

/** Client-supplied key that makes `POST /payouts` safe to retry. */
@JvmInline
value class IdempotencyKey(val value: String) {
    init {
        require(PATTERN.matches(value)) { "Idempotency-Key must be 1..$MAX_LENGTH printable ASCII chars without spaces" }
    }

    override fun toString(): String = value

    companion object {
        const val MAX_LENGTH = 100
        private val PATTERN = Regex("^[\\x21-\\x7E]{1,$MAX_LENGTH}$")
    }
}

data class BeneficiaryAccount(
    val bankCode: String,
    val accountNumber: String,
    val holderName: String,
) {
    init {
        require(bankCode.isNotBlank()) { "bankCode must not be blank" }
        require(accountNumber.isNotBlank()) { "accountNumber must not be blank" }
        require(holderName.isNotBlank()) { "holderName must not be blank" }
    }

    val maskedAccountNumber: String
        get() = accountNumber.takeLast(VISIBLE_DIGITS).padStart(accountNumber.length, '*')

    // Account numbers are personal data: keep them out of logs and exception messages.
    override fun toString(): String = "BeneficiaryAccount(bankCode=$bankCode, accountNumber=$maskedAccountNumber)"

    private companion object {
        const val VISIBLE_DIGITS = 4
    }
}
