package payout.adapter.kafka

import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import io.kotest.assertions.throwables.shouldThrow
import io.kotest.matchers.shouldBe
import io.mockk.every
import io.mockk.mockk
import io.mockk.slot
import io.mockk.verify
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.junit.jupiter.api.Test
import org.springframework.dao.DataAccessResourceFailureException
import org.springframework.kafka.support.KafkaHeaders
import payout.application.GiveUpPayoutService
import payout.application.GiveUpReason
import payout.application.PayoutNotFoundException
import payout.application.port.BankTimeoutException
import payout.application.port.TransientBankException
import payout.domain.PayoutFixtures
import payout.domain.PayoutId

class DeadLetterHandlersTest {
    private val objectMapper = jacksonObjectMapper()
    private val reader = KafkaMessageReader(objectMapper)
    private val giveUp = mockk<GiveUpPayoutService>()
    private val handler = PayoutRequestedDltHandler(giveUp, reader)

    private fun deadLetter(
        payoutId: PayoutId,
        payload: String? = null,
        cause: String? = TransientBankException::class.java.name,
    ): ConsumerRecord<String, String> {
        val json = payload ?: objectMapper.writeValueAsString(PayoutRequestedMessage(payoutId.value, 150_000, "KRW"))
        return ConsumerRecord(Topics.PAYOUT_REQUESTED + "-dlt", 0, 0L, payoutId.toString(), json).apply {
            cause?.let { headers().add(KafkaHeaders.EXCEPTION_CAUSE_FQCN, it.toByteArray()) }
            headers().add(KafkaHeaders.EXCEPTION_MESSAGE, "bank temporarily unavailable".toByteArray())
            headers().add(KafkaHeaders.ORIGINAL_TOPIC, Topics.PAYOUT_REQUESTED.toByteArray())
        }
    }

    @Test
    fun `a payout whose last attempt timed out is handed over as outcome unknown`() {
        val payout = PayoutFixtures.requestedPayout()
        every { giveUp.giveUp(payout.id, any()) } returns payout

        handler.handle(deadLetter(payout.id, cause = BankTimeoutException::class.java.name))

        verify { giveUp.giveUp(payout.id, GiveUpReason.BANK_OUTCOME_UNKNOWN) }
    }

    @Test
    fun `the last error decides the reason handed to the use case`() {
        reasonFor(TransientBankException::class.java.name) shouldBe GiveUpReason.RETRIES_EXHAUSTED_BANK_UNAVAILABLE
        reasonFor(BankTimeoutException::class.java.name) shouldBe GiveUpReason.BANK_OUTCOME_UNKNOWN
        reasonFor(InvalidMessageException::class.java.name) shouldBe GiveUpReason.INVALID_MESSAGE
        reasonFor("java.lang.IllegalStateException") shouldBe GiveUpReason.PROCESSING_FAILED
        reasonFor(null) shouldBe GiveUpReason.PROCESSING_FAILED
    }

    @Test
    fun `a transient database failure is rethrown so the dead letter is not lost`() {
        val payout = PayoutFixtures.requestedPayout()
        every { giveUp.giveUp(payout.id, any()) } throws DataAccessResourceFailureException("connection refused")

        shouldThrow<DataAccessResourceFailureException> { handler.handle(deadLetter(payout.id)) }
    }

    @Test
    fun `a record that can never be applied is logged and dropped`() {
        val payout = PayoutFixtures.requestedPayout()
        every { giveUp.giveUp(payout.id, any()) } throws PayoutNotFoundException(payout.id)

        handler.handle(deadLetter(payout.id))
        handler.handle(deadLetter(payout.id, payload = "{not json"))

        verify(exactly = 1) { giveUp.giveUp(any(), any()) }
    }

    @Test
    fun `result dead letters are only reported`() {
        PayoutResultDltHandler(reader).handle(deadLetter(PayoutId.random()))
    }

    private fun reasonFor(exceptionClassName: String?): GiveUpReason {
        val payout = PayoutFixtures.requestedPayout()
        val reason = slot<GiveUpReason>()
        every { giveUp.giveUp(payout.id, capture(reason)) } returns payout

        handler.handle(deadLetter(payout.id, cause = exceptionClassName))

        return reason.captured
    }
}
