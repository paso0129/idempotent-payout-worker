package payout.adapter.persistence

import org.springframework.stereotype.Component
import org.springframework.transaction.PlatformTransactionManager
import org.springframework.transaction.support.TransactionTemplate
import payout.application.port.TransactionRunner

/** Explicit transaction boundaries for the use cases, without putting Spring annotations on them. */
@Component
class SpringTransactionRunner(transactionManager: PlatformTransactionManager) : TransactionRunner {
    private val template = TransactionTemplate(transactionManager)

    @Suppress("UNCHECKED_CAST")
    override fun <T> inTransaction(block: () -> T): T = template.execute { block() } as T
}
