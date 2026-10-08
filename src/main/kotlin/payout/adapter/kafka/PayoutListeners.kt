package payout.adapter.kafka

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.slf4j.LoggerFactory
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.stereotype.Component
import payout.application.ApplyPayoutResultService
import payout.application.BankResultOutcome
import payout.application.ProcessPayoutService
import payout.application.PayoutResult
import payout.domain.PayoutId

/**
 * `payout.requested` consumer. Exceptions are classified by [KafkaConfig]: retryable ones hop to
 * the retry topics (same record, same `message-id`), the rest go straight to the DLT.
 */
@Component
class PayoutRequestedListener(
    private val service: ProcessPayoutService,
    private val reader: KafkaMessageReader,
) {
    private val log = LoggerFactory.getLogger(javaClass)

    @KafkaListener(id = ProcessPayoutService.CONSUMER, topics = [Topics.PAYOUT_REQUESTED])
    fun onMessage(record: ConsumerRecord<String, String>) {
        val messageId = reader.messageId(record)
        val message = reader.payload(record, PayoutRequestedMessage::class.java)
        val outcome = service.process(messageId, PayoutId(message.payoutId))
        log.info("payout.requested message={} payout={} topic={} -> {}", messageId, message.payoutId, record.topic(), outcome)
    }
}

@Component
class PayoutResultListener(
    private val service: ApplyPayoutResultService,
    private val reader: KafkaMessageReader,
) {
    private val log = LoggerFactory.getLogger(javaClass)

    @KafkaListener(id = ApplyPayoutResultService.CONSUMER, topics = [Topics.PAYOUT_RESULT])
    fun onMessage(record: ConsumerRecord<String, String>) {
        val messageId = reader.messageId(record)
        val message = reader.payload(record, PayoutResultMessage::class.java)
        val outcome = BankResultOutcome.entries.firstOrNull { it.name == message.outcome }
            ?: throw InvalidMessageException("unknown result outcome '${message.outcome}' in message $messageId")
        val result = PayoutResult(PayoutId(message.payoutId), outcome, message.bankReference, message.reasonCode)
        val applied = service.apply(messageId, result)
        log.info("payout.result message={} payout={} -> {}", messageId, message.payoutId, applied)
    }
}
