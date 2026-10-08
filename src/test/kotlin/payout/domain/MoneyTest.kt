package payout.domain

import io.kotest.assertions.throwables.shouldThrow
import io.kotest.matchers.shouldBe
import io.kotest.matchers.shouldNotBe
import org.junit.jupiter.api.Test

class MoneyTest {

    @Test
    fun `creates money from minor units and ISO currency code`() {
        val money = Money.of(150_000, "KRW")

        money.amountMinor shouldBe 150_000
        money.currency.currencyCode shouldBe "KRW"
    }

    @Test
    fun `equal amounts in the same currency are equal regardless of how they were built`() {
        Money.of(1_050, "USD") shouldBe Money.of(1_050, "USD")
    }

    @Test
    fun `same amount in different currencies is not equal`() {
        Money.of(1_000, "USD") shouldNotBe Money.of(1_000, "EUR")
    }

    @Test
    fun `rejects negative amount`() {
        shouldThrow<IllegalArgumentException> { Money.of(-1, "KRW") }
    }

    @Test
    fun `rejects unknown currency code`() {
        shouldThrow<IllegalArgumentException> { Money.of(100, "XYZ") }
    }

    @Test
    fun `rejects lower case currency code`() {
        shouldThrow<IllegalArgumentException> { Money.of(100, "krw") }
    }

    @Test
    fun `renders major units using the currency fraction digits`() {
        Money.of(1_050, "USD").toString() shouldBe "10.50 USD"
        Money.of(150_000, "KRW").toString() shouldBe "150000 KRW"
    }
}
