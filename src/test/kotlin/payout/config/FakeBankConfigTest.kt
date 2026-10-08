package payout.config

import io.kotest.matchers.shouldBe
import org.junit.jupiter.api.Test
import org.springframework.boot.test.context.runner.ApplicationContextRunner
import payout.adapter.bank.FakeBankGateway
import java.time.Clock

class FakeBankConfigTest {
    private val runner = ApplicationContextRunner()
        .withUserConfiguration(FakeBankConfig::class.java)
        .withBean(Clock::class.java, { Clock.systemUTC() })

    @Test
    fun `the fake bank exists only when explicitly enabled`() {
        runner.withPropertyValues("payout.fake-bank.enabled=true").run { context ->
            context.getBeansOfType(FakeBankGateway::class.java).size shouldBe 1
        }
        runner.run { context ->
            context.getBeansOfType(FakeBankGateway::class.java).size shouldBe 0
        }
        runner.withPropertyValues("payout.fake-bank.enabled=false").run { context ->
            context.getBeansOfType(FakeBankGateway::class.java).size shouldBe 0
        }
    }
}
