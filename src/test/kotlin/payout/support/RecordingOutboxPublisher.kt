package payout.support

import payout.adapter.outbox.KafkaOutboxPublisher
import payout.adapter.outbox.OutboxMessageEntity
import payout.adapter.outbox.OutboxPublisher
import java.util.UUID
import java.util.concurrent.CompletableFuture
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger

/**
 * Wraps the real Kafka publisher and counts sends per outbox row, for every relay in the context
 * (the scheduled one included), so a test can prove no row went out twice.
 */
class RecordingOutboxPublisher(private val delegate: KafkaOutboxPublisher) : OutboxPublisher {
    private val sends = ConcurrentHashMap<UUID, AtomicInteger>()

    override fun publish(message: OutboxMessageEntity): CompletableFuture<*> {
        sends.computeIfAbsent(message.id) { AtomicInteger() }.incrementAndGet()
        return delegate.publish(message)
    }

    fun sendCount(rowId: UUID): Int = sends[rowId]?.get() ?: 0
}
