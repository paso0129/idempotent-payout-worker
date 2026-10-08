package payout.config

import org.slf4j.LoggerFactory
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import payout.adapter.bank.FakeBankGateway
import java.time.Clock

/**
 * Demo and tests only. The fake keeps its ledger in memory: no money moves, and its idempotency
 * memory is lost on restart. A real deployment sets `payout.fake-bank.enabled=false` and provides
 * its own [payout.application.port.BankGateway] and [payout.application.port.BankStatementSource].
 */
@Configuration
@ConditionalOnProperty("payout.fake-bank.enabled", havingValue = "true")
class FakeBankConfig {
    private val log = LoggerFactory.getLogger(javaClass)

    @Bean
    fun fakeBankGateway(clock: Clock): FakeBankGateway {
        log.warn("FakeBankGateway is active: transfers are simulated in memory. Demo and tests only.")
        return FakeBankGateway(clock)
    }
}
