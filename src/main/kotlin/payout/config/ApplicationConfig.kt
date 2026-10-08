package payout.config

import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.scheduling.annotation.EnableScheduling
import payout.application.ApplyPayoutResultService
import payout.application.GiveUpPayoutService
import payout.application.PayoutQueryService
import payout.application.ProcessPayoutService
import payout.application.ReconciliationService
import payout.application.RequestPayoutService
import payout.application.port.BankGateway
import payout.application.port.BankStatementSource
import payout.application.port.PayoutEventOutbox
import payout.application.port.PayoutRepository
import payout.application.port.ProcessedMessageRepository
import payout.application.port.TransactionRunner
import java.time.Clock
import java.time.Duration

/** The application layer is plain Kotlin; this is the only place it meets Spring. */
@Configuration
@EnableScheduling
class ApplicationConfig {

    /**
     * Ticks in microseconds, PostgreSQL's timestamptz precision. Otherwise the first response would
     * carry nanoseconds and an idempotent replay (read back from the database) would not.
     */
    @Bean
    fun clock(): Clock = Clock.tick(Clock.systemUTC(), Duration.ofNanos(NANOS_PER_MICRO))

    @Bean
    fun requestPayoutService(
        payouts: PayoutRepository,
        outbox: PayoutEventOutbox,
        tx: TransactionRunner,
        clock: Clock,
    ) = RequestPayoutService(payouts, outbox, tx, clock)

    @Bean
    fun processPayoutService(
        payouts: PayoutRepository,
        processedMessages: ProcessedMessageRepository,
        bank: BankGateway,
        tx: TransactionRunner,
        clock: Clock,
    ) = ProcessPayoutService(payouts, processedMessages, bank, tx, clock)

    @Bean
    fun applyPayoutResultService(
        payouts: PayoutRepository,
        processedMessages: ProcessedMessageRepository,
        tx: TransactionRunner,
        clock: Clock,
    ) = ApplyPayoutResultService(payouts, processedMessages, tx, clock)

    @Bean
    fun payoutQueryService(payouts: PayoutRepository) = PayoutQueryService(payouts)

    @Bean
    fun giveUpPayoutService(payouts: PayoutRepository, tx: TransactionRunner, clock: Clock) =
        GiveUpPayoutService(payouts, tx, clock)

    @Bean
    fun reconciliationService(
        payouts: PayoutRepository,
        bankStatement: BankStatementSource,
        properties: ReconciliationProperties,
    ) = ReconciliationService(payouts, bankStatement, properties.zone)

    private companion object {
        const val NANOS_PER_MICRO = 1_000L
    }
}
