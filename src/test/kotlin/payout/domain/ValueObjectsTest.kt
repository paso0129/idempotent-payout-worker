package payout.domain

import io.kotest.assertions.throwables.shouldThrow
import io.kotest.matchers.shouldBe
import io.kotest.matchers.string.shouldNotContain
import org.junit.jupiter.api.Test

class ValueObjectsTest {

    @Test
    fun `beneficiary account masks the account number when printed`() {
        val account = BeneficiaryAccount(bankCode = "BANK01", accountNumber = "1234567890", holderName = "Jane Doe")

        account.toString() shouldNotContain "1234567890"
        account.maskedAccountNumber shouldBe "******7890"
    }

    @Test
    fun `beneficiary account requires every field`() {
        shouldThrow<IllegalArgumentException> { BeneficiaryAccount("", "1234567890", "Jane Doe") }
        shouldThrow<IllegalArgumentException> { BeneficiaryAccount("BANK01", " ", "Jane Doe") }
        shouldThrow<IllegalArgumentException> { BeneficiaryAccount("BANK01", "1234567890", "") }
    }

    @Test
    fun `idempotency key accepts printable ascii up to 100 chars`() {
        IdempotencyKey("order-42:attempt-1").value shouldBe "order-42:attempt-1"
        IdempotencyKey("a".repeat(100)).value.length shouldBe 100
    }

    @Test
    fun `idempotency key rejects blank, whitespace and oversized values`() {
        shouldThrow<IllegalArgumentException> { IdempotencyKey("") }
        shouldThrow<IllegalArgumentException> { IdempotencyKey("has space") }
        shouldThrow<IllegalArgumentException> { IdempotencyKey("a".repeat(101)) }
    }

    @Test
    fun `payout id round-trips through its string form`() {
        val id = PayoutId.random()

        PayoutId.parse(id.toString()) shouldBe id
    }
}
