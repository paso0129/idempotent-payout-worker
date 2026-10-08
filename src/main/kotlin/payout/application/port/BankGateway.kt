package payout.application.port

import payout.domain.BeneficiaryAccount
import payout.domain.Money
import payout.domain.reconciliation.BankStatementLine
import java.time.Instant

interface BankGateway {
    /**
     * Must be idempotent on [TransferRequest.idempotencyKey]: a repeated key returns the original
     * outcome instead of moving money a second time. Must also bound the call with a client timeout
     * (raising [BankTimeoutException]): the worker holds the payout row lock while it waits.
     *
     * @throws TransientBankException the bank refused to process now and did nothing; safe to retry.
     * @throws BankTimeoutException the outcome is unknown; retry with the same key to learn it.
     */
    fun transfer(request: TransferRequest): TransferOutcome
}

data class TransferRequest(
    val idempotencyKey: String,
    val beneficiary: BeneficiaryAccount,
    val money: Money,
)

sealed interface TransferOutcome {
    data class Accepted(val bankReference: String) : TransferOutcome

    /** A final answer from the bank (closed account, invalid beneficiary, ...). Never retried. */
    data class Rejected(val reasonCode: String) : TransferOutcome
}

/** Retryable bank failures. Permanent refusals are a [TransferOutcome.Rejected], not an exception. */
sealed class RetryableBankException(message: String) : RuntimeException(message)

class TransientBankException(message: String) : RetryableBankException(message)

class BankTimeoutException(message: String) : RetryableBankException(message)

/** Where the bank's list of executed transfers comes from (statement file, inquiry API, ...). */
interface BankStatementSource {
    fun statement(from: Instant, until: Instant): List<BankStatementLine>
}
