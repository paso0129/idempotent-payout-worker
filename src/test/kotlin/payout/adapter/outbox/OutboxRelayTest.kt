package payout.adapter.outbox

import io.kotest.assertions.throwables.shouldThrow
import io.kotest.matchers.shouldBe
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import org.junit.jupiter.api.Test
import payout.application.fakes.FakeTransactionRunner
import payout.domain.PayoutFixtures.NOW
import java.time.Clock
import java.time.Duration
import java.time.ZoneOffset
import java.util.UUID
import java.util.concurrent.CompletableFuture

class OutboxRelayTest {
    private val clock = Clock.fixed(NOW, ZoneOffset.UTC)
    private val outbox = mockk<OutboxJpaRepository>(relaxed = true)
    private val publisher = mockk<OutboxPublisher>()

    private fun relay(batchSize: Int = 2, sendTimeout: Duration = Duration.ofSeconds(1)) = OutboxRelay(
        outbox,
        publisher,
        FakeTransactionRunner(),
        clock,
        OutboxProperties(batchSize = batchSize, sendTimeout = sendTimeout),
    )

    private fun row() = OutboxMessageEntity(UUID.randomUUID(), "payout.requested", "key", "{}", NOW)

    @Test
    fun `rows are marked published only after the broker acked all of them`() {
        val rows = listOf(row(), row())
        every { outbox.lockUnpublished(2) } returns rows
        every { publisher.publish(any()) } returns CompletableFuture.completedFuture(Unit)

        relay().relayBatch() shouldBe 2

        verify { outbox.markPublished(rows.map { it.id }, NOW) }
    }

    @Test
    fun `only rows the broker acked are marked, a failing row stays for the next tick`() {
        val rows = listOf(row(), row())
        every { outbox.lockUnpublished(2) } returns rows
        every { publisher.publish(rows[0]) } returns CompletableFuture.completedFuture(Unit)
        every { publisher.publish(rows[1]) } returns CompletableFuture.failedFuture<Unit>(IllegalStateException("record too large"))

        relay().relayBatch() shouldBe 1

        verify { outbox.markPublished(listOf(rows[0].id), NOW) }
    }

    @Test
    fun `sends still pending at the batch deadline are not marked`() {
        val rows = listOf(row(), row())
        every { outbox.lockUnpublished(2) } returns rows
        every { publisher.publish(rows[0]) } returns CompletableFuture.completedFuture(Unit)
        every { publisher.publish(rows[1]) } returns CompletableFuture<Unit>()

        relay(sendTimeout = Duration.ofMillis(50)).relayBatch() shouldBe 1

        verify { outbox.markPublished(listOf(rows[0].id), NOW) }
    }

    @Test
    fun `no new send starts once the batch deadline has passed`() {
        val rows = listOf(row(), row(), row(), row())
        every { outbox.lockUnpublished(4) } returns rows
        // KafkaTemplate.send blocks the caller while broker metadata is missing.
        every { publisher.publish(any()) } answers {
            Thread.sleep(BLOCKING_SEND_MILLIS)
            CompletableFuture.completedFuture(Unit)
        }

        val published = relay(batchSize = 4, sendTimeout = Duration.ofMillis(DEADLINE_MILLIS)).relayBatch()

        published shouldBe 2
        verify(exactly = 2) { publisher.publish(any()) }
        verify { outbox.markPublished(rows.take(2).map { it.id }, NOW) }
    }

    @Test
    fun `a send that throws synchronously counts as not acked and the batch goes on`() {
        val rows = listOf(row(), row())
        every { outbox.lockUnpublished(2) } returns rows
        every { publisher.publish(rows[0]) } throws IllegalStateException("metadata not available")
        every { publisher.publish(rows[1]) } returns CompletableFuture.completedFuture(Unit)

        relay().relayBatch() shouldBe 1

        verify { outbox.markPublished(listOf(rows[1].id), NOW) }
    }

    @Test
    fun `an interrupt while waiting for acks is preserved for the caller`() {
        val rows = listOf(row())
        every { outbox.lockUnpublished(2) } returns rows
        every { publisher.publish(any()) } returns CompletableFuture<Unit>()

        Thread.currentThread().interrupt()
        val published = relay().relayBatch()

        published shouldBe 0
        Thread.interrupted() shouldBe true
    }

    @Test
    fun `a batch where nothing was acked marks nothing`() {
        val rows = listOf(row())
        every { outbox.lockUnpublished(2) } returns rows
        every { publisher.publish(any()) } returns CompletableFuture.failedFuture<Unit>(IllegalStateException("broker down"))

        relay().relayBatch() shouldBe 0

        verify(exactly = 0) { outbox.markPublished(any(), any()) }
    }

    @Test
    fun `an empty outbox publishes nothing`() {
        every { outbox.lockUnpublished(2) } returns emptyList()

        relay().relayBatch() shouldBe 0

        verify(exactly = 0) { publisher.publish(any()) }
        verify(exactly = 0) { outbox.markPublished(any(), any()) }
    }

    @Test
    fun `drain keeps going while batches come back full and survives a broker failure`() {
        val first = listOf(row(), row())
        val second = listOf(row())
        every { outbox.lockUnpublished(2) } returnsMany listOf(first, second)
        every { publisher.publish(any()) } returns CompletableFuture.completedFuture(Unit)

        relay().drain()

        verify(exactly = 2) { outbox.lockUnpublished(2) }

        every { outbox.lockUnpublished(2) } throws IllegalStateException("database unavailable")
        relay().drain()
    }

    @Test
    fun `batch size must be positive`() {
        shouldThrow<IllegalArgumentException> { OutboxProperties(batchSize = 0) }
    }

    private companion object {
        const val BLOCKING_SEND_MILLIS = 300L
        const val DEADLINE_MILLIS = 500L
    }
}
