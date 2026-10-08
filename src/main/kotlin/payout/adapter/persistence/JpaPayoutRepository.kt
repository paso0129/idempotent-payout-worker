package payout.adapter.persistence

import org.hibernate.exception.ConstraintViolationException
import org.springframework.dao.DataIntegrityViolationException
import org.springframework.stereotype.Component
import payout.application.DuplicateIdempotencyKeyException
import payout.application.port.PayoutRepository
import payout.domain.IdempotencyKey
import payout.domain.Payout
import payout.domain.PayoutId
import java.time.Instant

@Component
class JpaPayoutRepository(private val jpa: PayoutJpaRepository) : PayoutRepository {

    override fun findById(id: PayoutId): Payout? = jpa.findById(id.value).orElse(null)?.toDomain()

    override fun findByIdForUpdate(id: PayoutId): Payout? = jpa.findByIdForUpdate(id.value)?.toDomain()

    override fun findByIdempotencyKey(key: IdempotencyKey): Payout? = jpa.findByIdempotencyKey(key.value)?.toDomain()

    override fun insert(payout: Payout) {
        try {
            jpa.saveAndFlush(PayoutEntity.from(payout))
        } catch (e: DataIntegrityViolationException) {
            if (isIdempotencyKeyViolation(e)) throw DuplicateIdempotencyKeyException(payout.idempotencyKey)
            throw e
        }
    }

    override fun update(payout: Payout) {
        val entity = jpa.findById(payout.id.value).orElseThrow { IllegalStateException("payout ${payout.id} does not exist") }
        entity.copyStateFrom(payout)
        jpa.flush()
    }

    override fun findCreatedBetween(from: Instant, until: Instant): List<Payout> =
        jpa.findByCreatedAtGreaterThanEqualAndCreatedAtLessThanOrderByCreatedAt(from, until).map { it.toDomain() }

    private fun isIdempotencyKeyViolation(e: DataIntegrityViolationException): Boolean =
        (e.cause as? ConstraintViolationException)?.constraintName.equals(IDEMPOTENCY_KEY_CONSTRAINT, ignoreCase = true)

    private companion object {
        const val IDEMPOTENCY_KEY_CONSTRAINT = "uk_payout_idempotency_key"
    }
}
