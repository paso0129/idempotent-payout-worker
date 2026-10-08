package payout.adapter.bank

import payout.application.port.BankGateway
import payout.application.port.BankStatementSource
import payout.application.port.BankTimeoutException
import payout.application.port.TransferOutcome
import payout.application.port.TransferRequest
import payout.application.port.TransientBankException
import payout.domain.reconciliation.BankStatementLine
import java.time.Clock
import java.time.Instant
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.atomic.AtomicLong

enum class FakeBankBehavior {
    SUCCESS,
    REJECT,

    /** Bank says "not now" and did nothing. */
    TRANSIENT_ERROR,

    /** No response; the bank never saw the request. */
    TIMEOUT_BEFORE_EXECUTION,

    /** No response; the bank did move the money. The case that needs the idempotency key. */
    TIMEOUT_AFTER_EXECUTION,
}

data class ExecutedTransfer(
    val idempotencyKey: String,
    val bankReference: String,
    val request: TransferRequest,
    val executedAt: Instant,
) {
    fun toStatementLine() = BankStatementLine(bankReference, idempotencyKey, request.money, executedAt)
}

/**
 * In-memory bank that honours idempotency keys the way the worker expects a real one to: the first
 * request for a key decides the outcome, later requests with that key get the same answer back.
 * Behaviour per key can be scripted to simulate errors; unscripted calls succeed.
 */
class FakeBankGateway(private val clock: Clock) : BankGateway, BankStatementSource {
    private val ledger = ConcurrentHashMap<String, LedgerEntry>()
    private val keyScripts = ConcurrentHashMap<String, ConcurrentLinkedQueue<FakeBankBehavior>>()
    private val accountScripts = ConcurrentHashMap<String, ConcurrentLinkedQueue<FakeBankBehavior>>()
    private val received = CopyOnWriteArrayList<TransferRequest>()
    private val injectedLines = CopyOnWriteArrayList<BankStatementLine>()
    private val referenceSequence = AtomicLong()

    /**
     * The scripted behaviour is consulted first: a timeout or "not now" can hit any request on the
     * way, including a retry of a key the bank already executed. Otherwise a known key is replayed.
     */
    override fun transfer(request: TransferRequest): TransferOutcome {
        received.add(request)
        return when (nextBehavior(request)) {
            FakeBankBehavior.SUCCESS -> record(request) { accept() }
            FakeBankBehavior.REJECT -> record(request) { TransferOutcome.Rejected(REJECT_REASON) }
            FakeBankBehavior.TRANSIENT_ERROR -> throw TransientBankException("bank temporarily unavailable")
            FakeBankBehavior.TIMEOUT_BEFORE_EXECUTION -> throw BankTimeoutException("no response from bank")
            FakeBankBehavior.TIMEOUT_AFTER_EXECUTION -> {
                record(request) { accept() }
                throw BankTimeoutException("bank executed the transfer but the response was lost")
            }
        }
    }

    fun script(idempotencyKey: String, vararg behaviors: FakeBankBehavior) {
        keyScripts.computeIfAbsent(idempotencyKey) { ConcurrentLinkedQueue() }.addAll(behaviors)
    }

    /** For end-to-end tests, where the payout id (our idempotency key) is generated after the script is set. */
    fun scriptForAccount(accountNumber: String, vararg behaviors: FakeBankBehavior) {
        accountScripts.computeIfAbsent(accountNumber) { ConcurrentLinkedQueue() }.addAll(behaviors)
    }

    fun callCount(idempotencyKey: String): Int = received.count { it.idempotencyKey == idempotencyKey }

    fun receivedRequests(): List<TransferRequest> = received.toList()

    fun executedTransfers(): List<ExecutedTransfer> = ledger.values.mapNotNull { it.executed }

    /** Executed transfers as the bank's statement shows them, plus any injected anomalies. */
    override fun statement(from: Instant, until: Instant): List<BankStatementLine> =
        (executedTransfers().map { it.toStatementLine() } + injectedLines)
            .filter { !it.executedAt.isBefore(from) && it.executedAt.isBefore(until) }

    /** A line that did not come through [transfer]: manual transfers, bank-side duplicates, wrong amounts. */
    fun injectStatementLine(line: BankStatementLine) {
        injectedLines.add(line)
    }

    private fun nextBehavior(request: TransferRequest): FakeBankBehavior =
        keyScripts[request.idempotencyKey]?.poll()
            ?: accountScripts[request.beneficiary.accountNumber]?.poll()
            ?: FakeBankBehavior.SUCCESS

    private fun accept(): TransferOutcome.Accepted =
        TransferOutcome.Accepted("FAKE-%010d".format(referenceSequence.incrementAndGet()))

    /**
     * computeIfAbsent makes "check key, then execute" atomic, like a unique index on the bank side.
     * A key seen before gets its original outcome back and `decide` is not called.
     */
    private fun record(request: TransferRequest, decide: () -> TransferOutcome): TransferOutcome {
        val entry = ledger.computeIfAbsent(request.idempotencyKey) {
            val outcome = decide()
            val executed = (outcome as? TransferOutcome.Accepted)?.let {
                ExecutedTransfer(request.idempotencyKey, it.bankReference, request, clock.instant())
            }
            LedgerEntry(request, outcome, executed)
        }
        return replay(entry, request)
    }

    private fun replay(entry: LedgerEntry, request: TransferRequest): TransferOutcome =
        if (entry.request.money == request.money && entry.request.beneficiary == request.beneficiary) {
            entry.outcome
        } else {
            TransferOutcome.Rejected(KEY_REUSED_REASON)
        }

    private data class LedgerEntry(
        val request: TransferRequest,
        val outcome: TransferOutcome,
        val executed: ExecutedTransfer?,
    )

    companion object {
        const val REJECT_REASON = "BENEFICIARY_ACCOUNT_INVALID"
        const val KEY_REUSED_REASON = "IDEMPOTENCY_KEY_REUSED"
    }
}
