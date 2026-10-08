package payout.adapter.kafka

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.slf4j.LoggerFactory
import org.springframework.kafka.support.KafkaHeaders
import org.springframework.stereotype.Component
import payout.application.GiveUpPayoutService
import payout.application.GiveUpReason
import payout.application.port.BankTimeoutException
import payout.application.port.TransientBankException
import payout.domain.PayoutId

/** Names the last error for the reason code. The payout's state alone decides FAILED or UNKNOWN. */
internal fun giveUpReasonFor(exceptionClassName: String?): GiveUpReason = when (exceptionClassName) {
    TransientBankException::class.java.name -> GiveUpReason.RETRIES_EXHAUSTED_BANK_UNAVAILABLE
    BankTimeoutException::class.java.name -> GiveUpReason.BANK_OUTCOME_UNKNOWN
    InvalidMessageException::class.java.name -> GiveUpReason.INVALID_MESSAGE
    else -> GiveUpReason.PROCESSING_FAILED
}

private fun ConsumerRecord<*, *>.coordinates(): String = "${topic()}-${partition()}@${offset()}"

// The listener's own exception arrives wrapped in ListenerExecutionFailedException; the cause names it.
private fun KafkaMessageReader.deadLetterCause(record: ConsumerRecord<String, String>): String? =
    header(record, KafkaHeaders.EXCEPTION_CAUSE_FQCN) ?: header(record, KafkaHeaders.EXCEPTION_FQCN)

/** Full context for the log line: original topic, exception class and message. */
private fun KafkaMessageReader.deadLetterDetail(record: ConsumerRecord<String, String>): String {
    val exception = deadLetterCause(record)?.substringAfterLast('.') ?: "unknown"
    val message = header(record, KafkaHeaders.EXCEPTION_MESSAGE).orEmpty()
    return "from ${header(record, KafkaHeaders.ORIGINAL_TOPIC) ?: record.topic()}: $exception: $message"
}

/**
 * End of the line for `payout.requested`. The payout stops looking in flight: FAILED if it never
 * reached the bank, UNKNOWN otherwise. The stored reason is a code only; exception details stay in
 * this log.
 */
@Component(PayoutRequestedDltHandler.BEAN_NAME)
class PayoutRequestedDltHandler(
    private val giveUp: GiveUpPayoutService,
    private val reader: KafkaMessageReader,
) {
    private val log = LoggerFactory.getLogger(javaClass)

    fun handle(record: ConsumerRecord<String, String>) {
        val reason = giveUpReasonFor(reader.deadLetterCause(record))
        val detail = reader.deadLetterDetail(record)
        try {
            val message = reader.payload(record, PayoutRequestedMessage::class.java)
            val payout = giveUp.giveUp(PayoutId(message.payoutId), reason)
            log.error("DLT payout={} status={} reason={} detail={}", payout.id, payout.status, reason, detail)
        } catch (e: Exception) {
            if (KafkaConfig.NON_RETRYABLE.none { it.isInstance(e) }) {
                // e.g. database down: rethrow, and the DLT strategy puts the record back on the DLT.
                log.error("DLT record {} could not be applied yet ({}); redelivering", record.coordinates(), detail, e)
                throw e
            }
            // Retrying cannot help (unparseable, unknown payout): stop here instead of looping forever.
            log.error("DLT record {} can never be applied ({}); needs manual handling", record.coordinates(), detail, e)
        }
    }

    companion object {
        const val BEAN_NAME = "payoutRequestedDltHandler"
        const val METHOD_NAME = "handle"
    }
}

/**
 * End of the line for `payout.result`. Nothing is changed automatically: the payout keeps its state
 * (often SENT, waiting for a settlement that was given up on), so alert loudly for a manual re-apply.
 */
@Component(PayoutResultDltHandler.BEAN_NAME)
class PayoutResultDltHandler(private val reader: KafkaMessageReader) {
    private val log = LoggerFactory.getLogger(javaClass)

    fun handle(record: ConsumerRecord<String, String>) {
        log.error("DLT bank result {} key={} was not applied: {}", record.coordinates(), record.key(), reader.deadLetterDetail(record))
    }

    companion object {
        const val BEAN_NAME = "payoutResultDltHandler"
        const val METHOD_NAME = "handle"
    }
}
