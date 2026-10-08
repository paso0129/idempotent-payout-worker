package payout.adapter.bank

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.fasterxml.jackson.module.kotlin.readValue
import io.kotest.matchers.collections.shouldHaveSize
import io.kotest.matchers.shouldBe
import io.mockk.every
import io.mockk.mockk
import io.mockk.slot
import io.mockk.verify
import org.apache.kafka.clients.producer.ProducerRecord
import org.junit.jupiter.api.Test
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.kafka.support.SendResult
import payout.adapter.kafka.MessageHeaders
import payout.adapter.kafka.PayoutResultMessage
import payout.adapter.kafka.Topics
import payout.application.port.TransferRequest
import payout.domain.Money
import payout.domain.PayoutFixtures
import payout.domain.PayoutFixtures.NOW
import java.time.Clock
import java.time.ZoneOffset
import java.util.UUID
import java.util.concurrent.CompletableFuture

class FakeBankResultEmitterTest {
    private val objectMapper: ObjectMapper = jacksonObjectMapper()
    private val bank = FakeBankGateway(Clock.fixed(NOW, ZoneOffset.UTC))
    private val kafka = mockk<KafkaTemplate<String, String>>()
    private val sent = mutableListOf<ProducerRecord<String, String>>()
    private val emitter = FakeBankResultEmitter(bank, kafka, objectMapper)

    init {
        val record = slot<ProducerRecord<String, String>>()
        every { kafka.send(capture(record)) } answers {
            sent += record.captured
            CompletableFuture.completedFuture(mockk<SendResult<String, String>>())
        }
    }

    @Test
    fun `each executed transfer is settled once on payout result`() {
        val payoutId = UUID.randomUUID()
        bank.transfer(TransferRequest(payoutId.toString(), PayoutFixtures.beneficiary(), Money.of(150_000, "KRW")))

        emitter.emitSettlements()
        emitter.emitSettlements()

        sent shouldHaveSize 1
        val record = sent.single()
        record.topic() shouldBe Topics.PAYOUT_RESULT
        record.key() shouldBe payoutId.toString()
        String(record.headers().lastHeader(MessageHeaders.MESSAGE_ID).value()) shouldBe
            "settlement-${bank.executedTransfers().single().bankReference}"
        val message = objectMapper.readValue<PayoutResultMessage>(record.value())
        message.payoutId shouldBe payoutId
        message.outcome shouldBe "SETTLED"
    }

    @Test
    fun `a failed send is retried on the next tick`() {
        bank.transfer(TransferRequest(UUID.randomUUID().toString(), PayoutFixtures.beneficiary(), Money.of(1_000, "KRW")))
        every { kafka.send(any<ProducerRecord<String, String>>()) } returns
            CompletableFuture.failedFuture(IllegalStateException("broker down"))

        emitter.emitSettlements()

        verify(exactly = 1) { kafka.send(any<ProducerRecord<String, String>>()) }
        every { kafka.send(any<ProducerRecord<String, String>>()) } returns
            CompletableFuture.completedFuture(mockk<SendResult<String, String>>())
        emitter.emitSettlements()
        verify(exactly = 2) { kafka.send(any<ProducerRecord<String, String>>()) }
    }
}
