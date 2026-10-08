package payout.application

import payout.domain.IdempotencyKey
import payout.domain.PayoutId
import payout.domain.PayoutStatus

class PayoutNotFoundException(val payoutId: PayoutId) : RuntimeException("payout $payoutId not found")

/** Same Idempotency-Key, different request body. */
class IdempotencyKeyConflictException(val key: IdempotencyKey) :
    RuntimeException("Idempotency-Key $key was already used with a different request body")

/** Raised by [payout.application.port.PayoutRepository.insert] when the unique key constraint fires. */
class DuplicateIdempotencyKeyException(val key: IdempotencyKey) :
    RuntimeException("Idempotency-Key $key already exists")

/** A bank result arrived before our own SENT transition committed; retrying later resolves it. */
class PayoutNotSentYetException(val payoutId: PayoutId, val status: PayoutStatus) :
    RuntimeException("payout $payoutId is $status; result can only be applied once it is SENT")
