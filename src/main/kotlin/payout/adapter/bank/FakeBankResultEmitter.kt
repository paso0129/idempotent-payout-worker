package payout.adapter.bank

import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.kafka.clients.producer.ProducerRecord
import org.slf4j.LoggerFactory
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.scheduling.annotation.Scheduled
import org.springframework.stereotype.Component
import payout.adapter.kafka.MessageHeaders
import payout.adapter.kafka.PayoutResultMessage
import payout.adapter.kafka.Topics
import payout.application.BankResultOutcome
import java.util.UUID
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.TimeUnit

/**
 * Local demo only: plays the bank's asynchronous settlement notice by publishing a SETTLED result
 * to `payout.result` for every transfer the fake bank executed. The message id is derived from the
 * bank reference, so a re-emitted notice is deduplicated by the consumer like a real one would be.
 */
@Component
@ConditionalOnProperty("payout.fake-bank.emit-results", havingValue = "true")
class FakeBankResultEmitter(
    private val bank: FakeBankGateway,
    private val kafka: KafkaTemplate<String, String>,
    private val objectMapper: ObjectMapper,
) {
    private val log = LoggerFactory.getLogger(javaClass)
    private val emitted: MutableSet<String> = ConcurrentHashMap.newKeySet()

    @Scheduled(fixedDelayString = "\${payout.fake-bank.emit-interval:1s}")
    fun emitSettlements() {
        bank.executedTransfers()
            .filterNot { it.bankReference in emitted }
            .forEach(::emit)
    }

    private fun emit(transfer: ExecutedTransfer) {
        val message = PayoutResultMessage(
            payoutId = UUID.fromString(transfer.idempotencyKey),
            outcome = BankResultOutcome.SETTLED.name,
            bankReference = transfer.bankReference,
            reasonCode = null,
        )
        val record = ProducerRecord(Topics.PAYOUT_RESULT, transfer.idempotencyKey, objectMapper.writeValueAsString(message))
        record.headers().add(MessageHeaders.MESSAGE_ID, "settlement-${transfer.bankReference}".toByteArray(Charsets.UTF_8))
        try {
            kafka.send(record).get(SEND_TIMEOUT_SECONDS, TimeUnit.SECONDS)
            emitted += transfer.bankReference
        } catch (e: InterruptedException) {
            Thread.currentThread().interrupt()
            log.warn("Interrupted while emitting settlement for {}; retrying next tick", transfer.bankReference)
        } catch (e: Exception) {
            log.warn("Could not emit settlement for {}; retrying next tick", transfer.bankReference, e)
        }
    }

    private companion object {
        const val SEND_TIMEOUT_SECONDS = 5L
    }
}
