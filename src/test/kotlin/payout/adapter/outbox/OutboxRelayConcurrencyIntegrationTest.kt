package payout.adapter.outbox

import io.kotest.matchers.shouldBe
import org.apache.kafka.clients.admin.NewTopic
import org.awaitility.kotlin.atMost
import org.awaitility.kotlin.await
import org.awaitility.kotlin.untilAsserted
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.kafka.core.KafkaAdmin
import payout.application.port.TransactionRunner
import payout.support.IntegrationTest
import payout.support.RecordingOutboxPublisher
import payout.support.TestData
import java.time.Clock
import java.time.Duration
import java.util.UUID
import java.util.concurrent.CountDownLatch
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit

/**
 * Several relays (plus the scheduled one in the context) drain the same PostgreSQL outbox at once.
 * `FOR UPDATE SKIP LOCKED` must hand each row to exactly one of them.
 */
@IntegrationTest
class OutboxRelayConcurrencyIntegrationTest @Autowired constructor(
    private val outbox: OutboxJpaRepository,
    private val recorder: RecordingOutboxPublisher,
    private val kafkaAdmin: KafkaAdmin,
    private val tx: TransactionRunner,
    private val clock: Clock,
) {
    @Test
    fun `relays draining the same outbox concurrently publish every row exactly once`() {
        kafkaAdmin.createOrModifyTopics(NewTopic(TEST_TOPIC, PARTITIONS, REPLICATION))
        val rows = List(ROWS) { OutboxMessageEntity(UUID.randomUUID(), TEST_TOPIC, "key-$it", "{}", TestData.now()) }
        tx.inTransaction { outbox.saveAll(rows) }
        val relays = List(RELAYS) { OutboxRelay(outbox, recorder, tx, clock, OutboxProperties(batchSize = BATCH)) }

        runConcurrently(relays.map { relay -> { relay.drain() } })

        await atMost AWAIT untilAsserted {
            outbox.findAllById(rows.map { it.id }).count { it.publishedAt != null } shouldBe ROWS
        }
        rows.map { recorder.sendCount(it.id) }.toSet() shouldBe setOf(1)
    }

    private fun runConcurrently(tasks: List<() -> Unit>) {
        val pool = Executors.newFixedThreadPool(tasks.size)
        val startGate = CountDownLatch(1)
        try {
            val futures = tasks.map { task -> pool.submit { startGate.await(); task() } }
            startGate.countDown()
            futures.forEach { it.get(TIMEOUT_SECONDS, TimeUnit.SECONDS) }
        } finally {
            pool.shutdownNow()
        }
    }

    private companion object {
        const val TEST_TOPIC = "outbox-relay-test"
        const val PARTITIONS = 3
        const val REPLICATION: Short = 1
        const val ROWS = 300
        const val RELAYS = 4
        const val BATCH = 20
        const val TIMEOUT_SECONDS = 60L
        val AWAIT: Duration = Duration.ofSeconds(30)
    }
}
