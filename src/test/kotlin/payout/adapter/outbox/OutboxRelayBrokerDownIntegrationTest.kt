package payout.adapter.outbox

import io.kotest.matchers.longs.shouldBeLessThan
import io.kotest.matchers.shouldBe
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import org.apache.kafka.clients.producer.ProducerConfig
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.autoconfigure.kafka.KafkaProperties
import org.springframework.kafka.core.DefaultKafkaProducerFactory
import org.springframework.kafka.core.KafkaTemplate
import payout.application.fakes.FakeTransactionRunner
import payout.support.IntegrationTest
import payout.support.TestData
import java.time.Clock
import java.time.Duration
import java.util.UUID

/**
 * A real Kafka producer, configured exactly like the application's, pointed at a port where no
 * broker listens. Proves the relay gives up within its deadline instead of holding the outbox row
 * locks for the producer's default 60 s metadata wait.
 */
@IntegrationTest
class OutboxRelayBrokerDownIntegrationTest @Autowired constructor(
    private val kafkaProperties: KafkaProperties,
    private val clock: Clock,
) {
    @Test
    fun `an unreachable broker releases the batch within the deadline and marks nothing`() {
        val producerConfig = kafkaProperties.buildProducerProperties() + (ProducerConfig.BOOTSTRAP_SERVERS_CONFIG to UNREACHABLE_BROKER)
        val maxBlock = Duration.ofMillis(producerConfig.getValue(ProducerConfig.MAX_BLOCK_MS_CONFIG).toString().toLong())
        val producerFactory = DefaultKafkaProducerFactory<String, String>(producerConfig)
        val outbox = mockk<OutboxJpaRepository>(relaxed = true)
        every { outbox.lockUnpublished(BATCH) } returns List(BATCH) {
            OutboxMessageEntity(UUID.randomUUID(), "payout.requested", "key", "{}", TestData.now())
        }
        val relay = OutboxRelay(
            outbox,
            KafkaOutboxPublisher(KafkaTemplate(producerFactory)),
            FakeTransactionRunner(),
            clock,
            OutboxProperties(batchSize = BATCH, sendTimeout = DEADLINE),
        )

        val started = System.nanoTime()
        val published = try {
            relay.relayBatch()
        } finally {
            producerFactory.destroy()
        }
        val elapsed = Duration.ofNanos(System.nanoTime() - started)

        published shouldBe 0
        verify(exactly = 0) { outbox.markPublished(any(), any()) }
        elapsed.toMillis() shouldBeLessThan DEADLINE.plus(maxBlock).plus(SLACK).toMillis()
    }

    private companion object {
        const val UNREACHABLE_BROKER = "localhost:1"
        const val BATCH = 10
        val DEADLINE: Duration = Duration.ofSeconds(1)
        val SLACK: Duration = Duration.ofSeconds(1)
    }
}
