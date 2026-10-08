package payout.adapter.persistence

import jakarta.persistence.Entity
import jakarta.persistence.EnumType
import jakarta.persistence.Enumerated
import jakarta.persistence.Id
import jakarta.persistence.LockModeType
import jakarta.persistence.Table
import jakarta.persistence.Version
import org.springframework.data.jpa.repository.JpaRepository
import org.springframework.data.jpa.repository.Lock
import org.springframework.data.jpa.repository.Query
import payout.domain.BeneficiaryAccount
import payout.domain.IdempotencyKey
import payout.domain.Money
import payout.domain.Payout
import payout.domain.PayoutId
import payout.domain.PayoutStatus
import java.time.Instant
import java.util.UUID

/** JPA row for [Payout]. Mutable only because JPA needs it; the domain object stays immutable. */
@Entity
@Table(name = "payout")
class PayoutEntity(
    @Id
    val id: UUID,
    val idempotencyKey: String,
    val requestFingerprint: String,
    val bankCode: String,
    val accountNumber: String,
    val accountHolder: String,
    val amountMinor: Long,
    val currency: String,
    @Enumerated(EnumType.STRING)
    var status: PayoutStatus,
    var bankReference: String?,
    var failureReason: String?,
    val createdAt: Instant,
    var updatedAt: Instant,
) {
    /** Optimistic lock as a safety net; the worker already serialises on the row lock. */
    @Version
    var version: Long? = null

    fun toDomain(): Payout = Payout(
        id = PayoutId(id),
        idempotencyKey = IdempotencyKey(idempotencyKey),
        requestFingerprint = requestFingerprint,
        beneficiary = BeneficiaryAccount(bankCode, accountNumber, accountHolder),
        money = Money.of(amountMinor, currency),
        status = status,
        bankReference = bankReference,
        failureReason = failureReason,
        createdAt = createdAt,
        updatedAt = updatedAt,
    )

    /** Copies the fields a transition may change. Identity and amount never change after insert. */
    fun copyStateFrom(payout: Payout) {
        check(payout.id.value == id) { "entity $id cannot take the state of payout ${payout.id}" }
        status = payout.status
        bankReference = payout.bankReference
        failureReason = payout.failureReason
        updatedAt = payout.updatedAt
    }

    companion object {
        fun from(payout: Payout): PayoutEntity = PayoutEntity(
            id = payout.id.value,
            idempotencyKey = payout.idempotencyKey.value,
            requestFingerprint = payout.requestFingerprint,
            bankCode = payout.beneficiary.bankCode,
            accountNumber = payout.beneficiary.accountNumber,
            accountHolder = payout.beneficiary.holderName,
            amountMinor = payout.money.amountMinor,
            currency = payout.money.currency.currencyCode,
            status = payout.status,
            bankReference = payout.bankReference,
            failureReason = payout.failureReason,
            createdAt = payout.createdAt,
            updatedAt = payout.updatedAt,
        )
    }
}

interface PayoutJpaRepository : JpaRepository<PayoutEntity, UUID> {
    @Lock(LockModeType.PESSIMISTIC_WRITE)
    @Query("select p from PayoutEntity p where p.id = :id")
    fun findByIdForUpdate(id: UUID): PayoutEntity?

    fun findByIdempotencyKey(idempotencyKey: String): PayoutEntity?

    fun findByCreatedAtGreaterThanEqualAndCreatedAtLessThanOrderByCreatedAt(from: Instant, until: Instant): List<PayoutEntity>
}
