package payout.adapter.persistence

import jakarta.persistence.Embeddable
import jakarta.persistence.EmbeddedId
import jakarta.persistence.Entity
import jakarta.persistence.Table
import org.springframework.data.jpa.repository.JpaRepository
import org.springframework.data.jpa.repository.Modifying
import org.springframework.data.jpa.repository.Query
import org.springframework.stereotype.Component
import payout.application.port.ProcessedMessageRepository
import java.io.Serializable
import java.time.Instant

@Embeddable
data class ProcessedMessageKey(
    val consumer: String,
    val messageId: String,
) : Serializable

@Entity
@Table(name = "processed_message")
class ProcessedMessageEntity(
    @EmbeddedId
    val key: ProcessedMessageKey,
    val processedAt: Instant,
)

interface ProcessedMessageJpaRepository : JpaRepository<ProcessedMessageEntity, ProcessedMessageKey> {
    /**
     * A concurrent insert of the same key blocks on the primary key until the other transaction
     * finishes, then inserts nothing. The return value says which side won.
     */
    @Modifying
    @Query(
        value = """
            insert into processed_message (consumer, message_id, processed_at)
            values (:consumer, :messageId, :processedAt)
            on conflict do nothing
        """,
        nativeQuery = true,
    )
    fun insertIfAbsent(consumer: String, messageId: String, processedAt: Instant): Int
}

@Component
class JpaProcessedMessageRepository(private val jpa: ProcessedMessageJpaRepository) : ProcessedMessageRepository {

    override fun exists(consumer: String, messageId: String): Boolean =
        jpa.existsById(ProcessedMessageKey(consumer, messageId))

    override fun markProcessed(consumer: String, messageId: String, at: Instant): Boolean =
        jpa.insertIfAbsent(consumer, messageId, at) == 1
}
