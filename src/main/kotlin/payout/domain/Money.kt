package payout.domain

import java.math.BigDecimal
import java.util.Currency

/**
 * An amount in the currency's minor unit (KRW: won, USD: cent) plus its ISO 4217 currency.
 *
 * Minor units as [Long] instead of [BigDecimal]: equality is exact (BigDecimal says 1.0 != 1.00),
 * there is no rounding mode to agree on, and the number maps 1:1 to what a bank transfer carries.
 */
data class Money(val amountMinor: Long, val currency: Currency) {
    init {
        require(amountMinor >= 0) { "amountMinor must not be negative: $amountMinor" }
    }

    override fun toString(): String =
        "${BigDecimal.valueOf(amountMinor, currency.defaultFractionDigits.coerceAtLeast(0)).toPlainString()} " +
            currency.currencyCode

    companion object {
        private val CURRENCY_CODE = Regex("^[A-Z]{3}$")

        fun of(amountMinor: Long, currencyCode: String): Money {
            require(CURRENCY_CODE.matches(currencyCode)) { "currency must be an upper-case ISO 4217 code: $currencyCode" }
            val currency = runCatching { Currency.getInstance(currencyCode) }
                .getOrElse { throw IllegalArgumentException("unknown currency: $currencyCode", it) }
            return Money(amountMinor, currency)
        }
    }
}
