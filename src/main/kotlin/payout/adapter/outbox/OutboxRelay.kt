package payout.adapter.outbox

import org.apache.kafka.clients.producer.ProducerRecord
import org.slf4j.LoggerFactory
import org.springframework.boot.context.properties.ConfigurationProperties
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.scheduling.annotation.Scheduled
import org.springframework.stereotype.Component
import payout.adapter.kafka.MessageHeaders
import payout.application.port.TransactionRunner
import java.time.Clock
import java.time.Duration
import java.util.concurrent.CompletableFuture
import java.util.concurrent.ExecutionException
import java.util.concurrent.TimeUnit
import java.util.concurrent.TimeoutException

@ConfigurationProperties("payout.outbox")
data class OutboxProperties(
    val batchSize: Int = 100,
    val pollInterval: Duration = Duration.ofMillis(500),
    /**
     * Deadline for the whole batch, not per record. Keep the producer's `max.block.ms` and
     * `delivery.timeout.ms` below it, or a single send can outlast it.
     */
    val sendTimeout: Duration = Duration.ofSeconds(5),
) {
    init {
        require(batchSize > 0) { "payout.outbox.batch-size must be positive" }
    }
}

fun interface OutboxPublisher {
    fun publish(message: OutboxMessageEntity): CompletableFuture<*>
}

@Component
class KafkaOutboxPublisher(private val kafka: KafkaTemplate<String, String>) : OutboxPublisher {
    override fun publish(message: OutboxMessageEntity): CompletableFuture<*> {
        val record = ProducerRecord(message.topic, message.messageKey, message.payload)
        record.headers().add(MessageHeaders.MESSAGE_ID, message.id.toString().toByteArray(Charsets.UTF_8))
        return kafka.send(record)
    }
}

/**
 * Moves committed outbox rows to Kafka. At-least-once: a row is marked published only after the
 * broker acked it, inside the transaction that locked it. A row whose send fails or is still pending
 * at the batch deadline stays unpublished for the next tick; if the commit fails after a successful
 * send the row goes out again, and consumers dedupe on the `message-id` header (the row id).
 */
@Component
class OutboxRelay(
    private val outbox: OutboxJpaRepository,
    private val publisher: OutboxPublisher,
    private val tx: TransactionRunner,
    private val clock: Clock,
    private val properties: OutboxProperties,
) {
    private val log = LoggerFactory.getLogger(javaClass)

    @Scheduled(fixedDelayString = "\${payout.outbox.poll-interval:500ms}")
    fun drain() {
        try {
            do {
                val published = relayBatch()
            } while (published == properties.batchSize)
        } catch (e: Exception) {
            log.warn("Outbox relay failed; unpublished rows stay in place and are retried on the next tick", e)
        }
    }

    /**
     * Returns how many rows were published. One deadline covers starting the sends and waiting for
     * acks, so rows stay locked for at most `send-timeout` plus one blocked send (`max.block.ms`).
     */
    fun relayBatch(): Int = tx.inTransaction {
        val deadline = System.nanoTime() + properties.sendTimeout.toNanos()
        val batch = outbox.lockUnpublished(properties.batchSize)
        val sends = startSends(batch, deadline)
        awaitAcks(sends.values, deadline)
        val (acked, notAcked) = sends.entries.partition { (_, send) -> send.isDone && !send.isCompletedExceptionally }
        notAcked.forEach { (row, send) ->
            val cause = if (send.isCompletedExceptionally) send.exceptionNow().toString() else "no ack before deadline"
            log.warn("Outbox row {} for {} not published ({}); retrying next tick", row.id, row.topic, cause)
        }
        if (sends.size < batch.size) log.warn("Outbox deadline reached; {} rows not sent this tick", batch.size - sends.size)
        if (acked.isNotEmpty()) outbox.markPublished(acked.map { it.key.id }, clock.instant())
        acked.size
    }

    /** KafkaTemplate.send blocks while broker metadata is missing, so the deadline is checked before each send. */
    private fun startSends(batch: List<OutboxMessageEntity>, deadline: Long): Map<OutboxMessageEntity, CompletableFuture<*>> =
        batch.asSequence()
            .takeWhile { System.nanoTime() < deadline }
            .associateWith(::sendOrFailedFuture)

    private fun sendOrFailedFuture(row: OutboxMessageEntity): CompletableFuture<*> =
        try {
            publisher.publish(row)
        } catch (e: Exception) {
            CompletableFuture.failedFuture<Unit>(e)
        }

    private fun awaitAcks(sends: Collection<CompletableFuture<*>>, deadline: Long) {
        val remaining = deadline - System.nanoTime()
        if (sends.isEmpty() || remaining <= 0) return
        try {
            CompletableFuture.allOf(*sends.toTypedArray()).get(remaining, TimeUnit.NANOSECONDS)
        } catch (e: ExecutionException) {
            // At least one send failed. Each send is inspected and logged by the caller.
        } catch (e: TimeoutException) {
            // Deadline reached. Pending sends count as not acked and are logged by the caller.
        } catch (e: InterruptedException) {
            // Shutting down: publish what was acked so far and let the caller see the interrupt.
            Thread.currentThread().interrupt()
        }
    }
}
