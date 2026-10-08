package payout.adapter.outbox

import com.fasterxml.jackson.databind.ObjectMapper
import jakarta.persistence.Entity
import jakarta.persistence.EntityManager
import jakarta.persistence.Id
import jakarta.persistence.Table
import org.springframework.data.jpa.repository.JpaRepository
import org.springframework.data.jpa.repository.Modifying
import org.springframework.data.jpa.repository.Query
import org.springframework.stereotype.Component
import payout.adapter.kafka.PayoutRequestedMessage
import payout.adapter.kafka.Topics
import payout.application.port.PayoutEventOutbox
import payout.domain.Payout
import java.time.Instant
import java.util.UUID

@Entity
@Table(name = "outbox_message")
class OutboxMessageEntity(
    /** Doubles as the Kafka `message-id` header, so a re-published row is recognisably the same message. */
    @Id
    val id: UUID,
    val topic: String,
    val messageKey: String,
    val payload: String,
    val createdAt: Instant,
    var publishedAt: Instant? = null,
)

interface OutboxJpaRepository : JpaRepository<OutboxMessageEntity, UUID> {
    /** SKIP LOCKED lets several relay instances drain the table without publishing the same row twice. */
    @Query(
        value = """
            select * from outbox_message
            where published_at is null
            order by created_at, id
            limit :limit
            for update skip locked
        """,
        nativeQuery = true,
    )
    fun lockUnpublished(limit: Int): List<OutboxMessageEntity>

    @Modifying
    @Query("update OutboxMessageEntity o set o.publishedAt = :at where o.id in :ids")
    fun markPublished(ids: Collection<UUID>, at: Instant): Int

    fun findByMessageKey(messageKey: String): List<OutboxMessageEntity>
}

@Component
class JpaPayoutEventOutbox(
    private val entityManager: EntityManager,
    private val objectMapper: ObjectMapper,
) : PayoutEventOutbox {

    override fun payoutRequested(payout: Payout) {
        val message = PayoutRequestedMessage(
            payoutId = payout.id.value,
            amountMinor = payout.money.amountMinor,
            currency = payout.money.currency.currencyCode,
        )
        // persist, not save(): the id is assigned up front, and merge would cost an extra select.
        entityManager.persist(
            OutboxMessageEntity(
                id = UUID.randomUUID(),
                topic = Topics.PAYOUT_REQUESTED,
                messageKey = payout.id.toString(),
                payload = objectMapper.writeValueAsString(message),
                createdAt = payout.createdAt,
            ),
        )
    }
}
