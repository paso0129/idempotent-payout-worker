package payout.adapter.persistence

import io.kotest.assertions.throwables.shouldThrow
import io.kotest.matchers.collections.shouldContain
import io.kotest.matchers.collections.shouldNotContain
import io.kotest.matchers.shouldBe
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.dao.DataIntegrityViolationException
import org.springframework.jdbc.core.JdbcTemplate
import payout.application.DuplicateIdempotencyKeyException
import payout.application.port.PayoutRepository
import payout.application.port.ProcessedMessageRepository
import payout.application.port.TransactionRunner
import payout.domain.Payout
import payout.domain.PayoutStatus
import payout.support.IntegrationTest
import payout.support.TestData
import java.time.Duration
import java.util.UUID

@IntegrationTest
class PersistenceAdapterIntegrationTest @Autowired constructor(
    private val payouts: PayoutRepository,
    private val processed: ProcessedMessageRepository,
    private val tx: TransactionRunner,
    private val jdbc: JdbcTemplate,
) {
    private fun insert(payout: Payout): Payout = payout.also { tx.inTransaction { payouts.insert(it) } }

    @Test
    fun `a payout round-trips through PostgreSQL unchanged`() {
        val payout = insert(TestData.requestedPayout())

        payouts.findById(payout.id) shouldBe payout
        payouts.findByIdempotencyKey(payout.idempotencyKey) shouldBe payout
    }

    @Test
    fun `the unique idempotency key constraint surfaces as DuplicateIdempotencyKeyException`() {
        val first = insert(TestData.requestedPayout())
        val sameKey = TestData.requestedPayout().copy(idempotencyKey = first.idempotencyKey)

        shouldThrow<DuplicateIdempotencyKeyException> { tx.inTransaction { payouts.insert(sameKey) } }

        payouts.findById(sameKey.id) shouldBe null
    }

    @Test
    fun `transitions are persisted through update`() {
        val payout = insert(TestData.requestedPayout())
        val sent = payout.startSending(TestData.now()).markSent("BANK-REF-9", TestData.now())

        tx.inTransaction {
            payouts.findByIdForUpdate(payout.id)
            payouts.update(sent)
        }

        payouts.findById(payout.id) shouldBe sent
    }

    @Test
    fun `a message id is recorded once per consumer`() {
        val messageId = UUID.randomUUID().toString()

        tx.inTransaction { processed.markProcessed("consumer-a", messageId, TestData.now()) } shouldBe true
        tx.inTransaction { processed.markProcessed("consumer-a", messageId, TestData.now()) } shouldBe false
        tx.inTransaction { processed.markProcessed("consumer-b", messageId, TestData.now()) } shouldBe true
        processed.exists("consumer-a", messageId) shouldBe true
    }

    @Test
    fun `a processed marker rolls back with its transaction`() {
        val messageId = UUID.randomUUID().toString()

        runCatching {
            tx.inTransaction {
                processed.markProcessed("consumer-a", messageId, TestData.now())
                error("bank call failed")
            }
        }

        processed.exists("consumer-a", messageId) shouldBe false
    }

    @Test
    fun `payouts are found by creation window`() {
        val payout = insert(TestData.requestedPayout())
        val window = Duration.ofSeconds(1)

        payouts.findCreatedBetween(payout.createdAt.minus(window), payout.createdAt.plus(window)) shouldContain payout
        payouts.findCreatedBetween(payout.createdAt.plus(window), payout.createdAt.plus(window.multipliedBy(2))) shouldNotContain payout
    }

    @Test
    fun `the database refuses a status outside the state machine and a non-positive amount`() {
        val payout = insert(TestData.requestedPayout())

        shouldThrow<DataIntegrityViolationException> {
            jdbc.update("update payout set status = 'PAID' where id = ?", payout.id.value)
        }
        shouldThrow<DataIntegrityViolationException> {
            jdbc.update("update payout set amount_minor = 0 where id = ?", payout.id.value)
        }
        payouts.findById(payout.id)?.status shouldBe PayoutStatus.REQUESTED
    }
}
