package payout.application

import io.kotest.assertions.throwables.shouldThrow
import io.kotest.matchers.collections.shouldBeEmpty
import io.kotest.matchers.collections.shouldHaveSize
import io.kotest.matchers.shouldBe
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import payout.adapter.bank.ExecutedTransfer
import payout.adapter.bank.FakeBankGateway
import payout.application.port.BankGateway
import payout.application.port.PayoutRepository
import payout.application.port.ProcessedMessageRepository
import payout.application.port.TransactionRunner
import payout.application.port.TransferOutcome
import payout.application.port.TransferRequest
import payout.domain.Payout
import payout.domain.PayoutStatus
import payout.support.IntegrationTest
import payout.support.TestData
import java.time.Clock
import java.util.UUID
import java.util.concurrent.Callable
import java.util.concurrent.CountDownLatch
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit

/**
 * The worker against real PostgreSQL row locks and unique constraints, called directly from many
 * threads so the race is not hidden behind Kafka partition ordering.
 */
@IntegrationTest
class ProcessPayoutConcurrencyIntegrationTest @Autowired constructor(
    private val payouts: PayoutRepository,
    private val processed: ProcessedMessageRepository,
    private val bank: FakeBankGateway,
    private val tx: TransactionRunner,
    private val clock: Clock,
) {
    private fun givenRequestedPayout(): Payout = TestData.requestedPayout().also { tx.inTransaction { payouts.insert(it) } }

    private fun worker(gateway: BankGateway) = ProcessPayoutService(payouts, processed, gateway, tx, clock)

    private fun transfersTo(payout: Payout): List<ExecutedTransfer> =
        bank.executedTransfers().filter { it.request.beneficiary.accountNumber == payout.beneficiary.accountNumber }

    @Test
    fun `the same message processed by many threads at once calls the bank once`() {
        val payout = givenRequestedPayout()
        val worker = worker(SlowBank(bank))

        val results = runConcurrently(THREADS) { worker.process("msg-${payout.id}", payout.id) }

        results.mapNotNull { it.exceptionOrNull() }.shouldBeEmpty()
        val outcomes = results.map { it.getOrThrow() }
        outcomes.filterIsInstance<ProcessPayoutOutcome.Completed>() shouldHaveSize 1
        outcomes.filter { it == ProcessPayoutOutcome.Duplicate } shouldHaveSize THREADS - 1
        bank.callCount(payout.id.toString()) shouldBe 1
        transfersTo(payout) shouldHaveSize 1
        payouts.findById(payout.id)?.status shouldBe PayoutStatus.SENT
    }

    @Test
    fun `different messages for the same payout racing each other still call the bank once`() {
        val payout = givenRequestedPayout()
        val worker = worker(SlowBank(bank))

        val results = runConcurrently(THREADS) { worker.process(UUID.randomUUID().toString(), payout.id) }

        results.mapNotNull { it.exceptionOrNull() }.shouldBeEmpty()
        val outcomes = results.map { it.getOrThrow() }
        outcomes.filterIsInstance<ProcessPayoutOutcome.Completed>() shouldHaveSize 1
        outcomes.filter { it == ProcessPayoutOutcome.NothingToDo(PayoutStatus.SENT) } shouldHaveSize THREADS - 1
        bank.callCount(payout.id.toString()) shouldBe 1
        payouts.findById(payout.id)?.status shouldBe PayoutStatus.SENT
    }

    @Test
    fun `a crash after the bank moved the money but before commit does not pay twice on redelivery`() {
        val payout = givenRequestedPayout()
        val worker = worker(CrashOnceAfterBankCall(bank))

        shouldThrow<SimulatedCrash> { worker.process("msg-${payout.id}", payout.id) }

        // The bank has the transfer, our database does not know: in-doubt, still SENDING.
        transfersTo(payout) shouldHaveSize 1
        payouts.findById(payout.id)?.status shouldBe PayoutStatus.SENDING
        processed.exists(ProcessPayoutService.CONSUMER, "msg-${payout.id}") shouldBe false

        worker.process("msg-${payout.id}", payout.id)

        val sent = checkNotNull(payouts.findById(payout.id))
        sent.status shouldBe PayoutStatus.SENT
        bank.callCount(payout.id.toString()) shouldBe 2
        transfersTo(payout) shouldHaveSize 1
        sent.bankReference shouldBe transfersTo(payout).single().bankReference
    }

    private fun <T> runConcurrently(threads: Int, task: () -> T): List<Result<T>> {
        val pool = Executors.newFixedThreadPool(threads)
        val startGate = CountDownLatch(1)
        try {
            val futures = (1..threads).map {
                pool.submit(Callable { startGate.await(); runCatching(task) })
            }
            startGate.countDown()
            return futures.map { it.get(TIMEOUT_SECONDS, TimeUnit.SECONDS) }
        } finally {
            pool.shutdownNow()
        }
    }

    private class SimulatedCrash : RuntimeException("process died before the database commit")

    /** Widens the race window: every competitor reaches the row lock while the first call is in flight. */
    private class SlowBank(private val delegate: BankGateway) : BankGateway {
        override fun transfer(request: TransferRequest): TransferOutcome {
            Thread.sleep(BANK_LATENCY_MILLIS)
            return delegate.transfer(request)
        }
    }

    private class CrashOnceAfterBankCall(private val delegate: BankGateway) : BankGateway {
        @Volatile
        private var crashed = false

        override fun transfer(request: TransferRequest): TransferOutcome {
            val outcome = delegate.transfer(request)
            if (!crashed) {
                crashed = true
                throw SimulatedCrash()
            }
            return outcome
        }
    }

    private companion object {
        const val THREADS = 8
        const val BANK_LATENCY_MILLIS = 300L
        const val TIMEOUT_SECONDS = 30L
    }
}
