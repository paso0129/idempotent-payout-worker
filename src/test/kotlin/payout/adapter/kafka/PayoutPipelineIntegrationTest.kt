package payout.adapter.kafka

import com.fasterxml.jackson.databind.ObjectMapper
import io.kotest.matchers.collections.shouldHaveSize
import io.kotest.matchers.shouldBe
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.TopicPartition
import org.awaitility.Awaitility
import org.awaitility.kotlin.atMost
import org.awaitility.kotlin.await
import org.awaitility.kotlin.untilAsserted
import org.awaitility.kotlin.untilNotNull
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.http.MediaType
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.kafka.core.ConsumerFactory
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.test.web.servlet.MockMvc
import org.springframework.test.web.servlet.post
import payout.adapter.bank.FakeBankBehavior.REJECT
import payout.adapter.bank.FakeBankBehavior.TIMEOUT_AFTER_EXECUTION
import payout.adapter.bank.FakeBankBehavior.TIMEOUT_BEFORE_EXECUTION
import payout.adapter.bank.FakeBankBehavior.TRANSIENT_ERROR
import payout.adapter.bank.FakeBankGateway
import payout.application.ApplyPayoutResultService
import payout.application.port.PayoutRepository
import payout.application.port.ProcessedMessageRepository
import payout.application.port.TransactionRunner
import payout.domain.Payout
import payout.domain.PayoutId
import payout.domain.PayoutStatus
import payout.support.IntegrationTest
import payout.support.TestData
import java.time.Duration
import java.util.UUID
import java.util.concurrent.TimeUnit

/** HTTP -> outbox -> Kafka -> worker -> fake bank -> result topic, against real brokers and PostgreSQL. */
@IntegrationTest
class PayoutPipelineIntegrationTest @Autowired constructor(
    private val mockMvc: MockMvc,
    private val objectMapper: ObjectMapper,
    private val kafka: KafkaTemplate<String, String>,
    private val payouts: PayoutRepository,
    private val processed: ProcessedMessageRepository,
    private val bank: FakeBankGateway,
    private val tx: TransactionRunner,
    private val jdbc: JdbcTemplate,
    private val consumerFactory: ConsumerFactory<String, String>,
    private val kafkaProperties: PayoutKafkaProperties,
) {
    private fun requestPayout(accountNumber: String): PayoutId {
        val response = mockMvc.post("/payouts") {
            contentType = MediaType.APPLICATION_JSON
            header("Idempotency-Key", UUID.randomUUID().toString())
            content = """{"bankCode":"BANK01","accountNumber":"$accountNumber","accountHolder":"Jane Doe","amountMinor":150000,"currency":"KRW"}"""
        }.andReturn().response
        return PayoutId.parse(objectMapper.readTree(response.contentAsString)["payoutId"].asText())
    }

    private fun awaitStatus(id: PayoutId, status: PayoutStatus): Payout =
        await atMost AWAIT untilNotNull { payouts.findById(id)?.takeIf { it.status == status } }

    private fun publish(topic: String, key: String, payload: Any, messageId: String?) {
        val record = ProducerRecord(topic, key, objectMapper.writeValueAsString(payload))
        messageId?.let { record.headers().add(MessageHeaders.MESSAGE_ID, it.toByteArray()) }
        kafka.send(record).get(SEND_TIMEOUT_SECONDS, TimeUnit.SECONDS)
    }

    private fun publishResult(payout: Payout, messageId: String, outcome: String = "SETTLED") = publish(
        Topics.PAYOUT_RESULT,
        payout.id.toString(),
        PayoutResultMessage(payout.id.value, outcome, payout.bankReference, reasonCode = null),
        messageId,
    )

    /** Reads a topic from the start with a throwaway consumer (no group offsets) until the key shows up. */
    private fun awaitRecord(topic: String, key: String) {
        consumerFactory.createConsumer("observer-${UUID.randomUUID()}", null).use { consumer ->
            val partitions = consumer.partitionsFor(topic).map { TopicPartition(topic, it.partition()) }
            consumer.assign(partitions)
            consumer.seekToBeginning(partitions)
            await atMost AWAIT untilAsserted {
                consumer.poll(POLL).any { it.key() == key } shouldBe true
            }
        }
    }

    private fun version(id: PayoutId): Long =
        checkNotNull(jdbc.queryForObject("select version from payout where id = ?", Long::class.java, id.value))

    private fun assertBankCallsStay(id: PayoutId, expected: Int) {
        Awaitility.await().during(QUIET_PERIOD).atMost(QUIET_PERIOD.plusSeconds(2)).untilAsserted {
            bank.callCount(id.toString()) shouldBe expected
        }
    }

    @Test
    fun `a payout flows from the API through Kafka to SENT and then SETTLED`() {
        val id = requestPayout(TestData.uniqueAccountNumber())

        val sent = awaitStatus(id, PayoutStatus.SENT)
        publishResult(sent, messageId = "result-${UUID.randomUUID()}")

        awaitStatus(id, PayoutStatus.SETTLED).bankReference shouldBe sent.bankReference
        bank.callCount(id.toString()) shouldBe 1
        jdbc.queryForObject(
            "select count(*) from outbox_message where message_key = ? and published_at is not null",
            Int::class.java,
            id.toString(),
        ) shouldBe 1
    }

    @Test
    fun `one message delivered many times across partitions calls the bank once`() {
        val payout = TestData.requestedPayout().also { tx.inTransaction { payouts.insert(it) } }
        val messageId = UUID.randomUUID().toString()
        val message = PayoutRequestedMessage(payout.id.value, payout.money.amountMinor, "KRW")

        // Different record keys spread the copies over partitions, i.e. over concurrent consumers.
        repeat(DUPLICATE_DELIVERIES) { copy -> publish(Topics.PAYOUT_REQUESTED, "copy-$copy", message, messageId) }

        awaitStatus(payout.id, PayoutStatus.SENT)
        assertBankCallsStay(payout.id, expected = 1)
    }

    @Test
    fun `transient bank errors are retried through the retry topics until the transfer succeeds`() {
        val account = TestData.uniqueAccountNumber()
        bank.scriptForAccount(account, TRANSIENT_ERROR, TRANSIENT_ERROR)

        val id = requestPayout(account)

        awaitStatus(id, PayoutStatus.SENT)
        bank.callCount(id.toString()) shouldBe 3
    }

    @Test
    fun `a payout sent to the bank that keeps failing ends in the DLT as UNKNOWN, never FAILED`() {
        val account = TestData.uniqueAccountNumber()
        val attempts = kafkaProperties.retry.attempts
        bank.scriptForAccount(account, *Array(attempts + 1) { TRANSIENT_ERROR })

        val id = requestPayout(account)

        val unknown = awaitStatus(id, PayoutStatus.UNKNOWN)
        unknown.failureReason shouldBe "RETRIES_EXHAUSTED_BANK_UNAVAILABLE"
        bank.callCount(id.toString()) shouldBe attempts
    }

    @Test
    fun `money moved by a timed-out first attempt survives later attempts that only see the bank down`() {
        val account = TestData.uniqueAccountNumber()
        val attempts = kafkaProperties.retry.attempts
        bank.scriptForAccount(account, TIMEOUT_AFTER_EXECUTION, *Array(attempts - 1) { TRANSIENT_ERROR })

        val id = requestPayout(account)

        // The last error says "bank unavailable", yet the first attempt moved the money: still UNKNOWN.
        awaitStatus(id, PayoutStatus.UNKNOWN).failureReason shouldBe "RETRIES_EXHAUSTED_BANK_UNAVAILABLE"
        val transfer = bank.executedTransfers().single { it.request.beneficiary.accountNumber == account }
        val notice = PayoutResultMessage(id.value, "SETTLED", transfer.bankReference, reasonCode = null)
        publish(Topics.PAYOUT_RESULT, id.toString(), notice, "settlement-${transfer.bankReference}")

        awaitStatus(id, PayoutStatus.SETTLED).bankReference shouldBe transfer.bankReference
        bank.executedTransfers().filter { it.request.beneficiary.accountNumber == account } shouldHaveSize 1
    }

    @Test
    fun `timeouts until retries run out leave the payout UNKNOWN, and the late bank result settles it`() {
        val account = TestData.uniqueAccountNumber()
        val attempts = kafkaProperties.retry.attempts
        // The first call moves the money but the answer is lost; every retry times out on the way.
        bank.scriptForAccount(account, TIMEOUT_AFTER_EXECUTION, *Array(attempts - 1) { TIMEOUT_BEFORE_EXECUTION })

        val id = requestPayout(account)

        awaitStatus(id, PayoutStatus.UNKNOWN).failureReason shouldBe "BANK_OUTCOME_UNKNOWN"
        val transfer = bank.executedTransfers().single { it.request.beneficiary.accountNumber == account }

        // The bank's own settlement notice arrives after we gave up.
        val notice = PayoutResultMessage(id.value, "SETTLED", transfer.bankReference, reasonCode = null)
        publish(Topics.PAYOUT_RESULT, id.toString(), notice, "settlement-${transfer.bankReference}")

        val settled = awaitStatus(id, PayoutStatus.SETTLED)
        settled.bankReference shouldBe transfer.bankReference
        settled.failureReason shouldBe null
        bank.callCount(id.toString()) shouldBe attempts
        bank.executedTransfers().filter { it.request.beneficiary.accountNumber == account } shouldHaveSize 1
    }

    @Test
    fun `a permanent bank rejection is not retried`() {
        val account = TestData.uniqueAccountNumber()
        bank.scriptForAccount(account, REJECT)

        val id = requestPayout(account)

        awaitStatus(id, PayoutStatus.REJECTED).failureReason shouldBe FakeBankGateway.REJECT_REASON
        assertBankCallsStay(id, expected = 1)
    }

    @Test
    fun `a timeout after the bank moved the money is retried without a second transfer`() {
        val account = TestData.uniqueAccountNumber()
        bank.scriptForAccount(account, TIMEOUT_AFTER_EXECUTION)

        val id = requestPayout(account)

        val sent = awaitStatus(id, PayoutStatus.SENT)
        bank.callCount(id.toString()) shouldBe 2
        val transfers = bank.executedTransfers().filter { it.request.beneficiary.accountNumber == account }
        transfers shouldHaveSize 1
        sent.bankReference shouldBe transfers.single().bankReference
    }

    @Test
    fun `a message without a message id is not retried and goes straight to the DLT`() {
        val payout = TestData.requestedPayout().also { tx.inTransaction { payouts.insert(it) } }
        val message = PayoutRequestedMessage(payout.id.value, payout.money.amountMinor, "KRW")

        publish(Topics.PAYOUT_REQUESTED, payout.id.toString(), message, messageId = null)

        awaitStatus(payout.id, PayoutStatus.FAILED).failureReason shouldBe "INVALID_MESSAGE"
        bank.callCount(payout.id.toString()) shouldBe 0
    }

    @Test
    fun `a bank result that overtakes our SENT commit is retried and applied once the payout is SENT`() {
        val requested = TestData.requestedPayout().also { tx.inTransaction { payouts.insert(it) } }
        val sending = requested.startSending(TestData.now())
        tx.inTransaction { payouts.findByIdForUpdate(requested.id); payouts.update(sending) }

        publishResult(sending, messageId = "result-${UUID.randomUUID()}")
        // The first attempt failed with PayoutNotSentYetException: the record now waits on the first retry topic.
        awaitRecord(topic = "${Topics.PAYOUT_RESULT}-retry-0", key = sending.id.toString())
        val requestedMessage = PayoutRequestedMessage(sending.id.value, sending.money.amountMinor, "KRW")
        publish(Topics.PAYOUT_REQUESTED, sending.id.toString(), requestedMessage, UUID.randomUUID().toString())

        awaitStatus(sending.id, PayoutStatus.SETTLED)
        bank.callCount(sending.id.toString()) shouldBe 1
    }

    @Test
    fun `the same bank result delivered twice is applied once`() {
        val id = requestPayout(TestData.uniqueAccountNumber())
        val sent = awaitStatus(id, PayoutStatus.SENT)
        val resultId = "result-${UUID.randomUUID()}"
        publishResult(sent, resultId)
        awaitStatus(id, PayoutStatus.SETTLED)
        val versionAfterSettle = version(id)

        val laterId = "result-${UUID.randomUUID()}"
        publishResult(sent, resultId)
        publishResult(sent, laterId)

        // Same partition (key = payout id), so once the later message is recorded the duplicate was consumed too.
        await atMost AWAIT untilAsserted { processed.exists(ApplyPayoutResultService.CONSUMER, laterId) shouldBe true }
        version(id) shouldBe versionAfterSettle
        payouts.findById(id)?.status shouldBe PayoutStatus.SETTLED
    }

    private companion object {
        val AWAIT: Duration = Duration.ofSeconds(30)
        val QUIET_PERIOD: Duration = Duration.ofSeconds(2)
        val POLL: Duration = Duration.ofMillis(200)
        const val SEND_TIMEOUT_SECONDS = 10L
        const val DUPLICATE_DELIVERIES = 6
    }
}
