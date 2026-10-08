package payout.adapter.web

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import io.kotest.matchers.collections.shouldHaveSize
import io.kotest.matchers.shouldBe
import io.kotest.matchers.shouldNotBe
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.http.MediaType
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.test.web.servlet.MockMvc
import org.springframework.test.web.servlet.MvcResult
import org.springframework.test.web.servlet.ResultActionsDsl
import org.springframework.test.web.servlet.get
import org.springframework.test.web.servlet.post
import payout.support.IntegrationTest
import payout.support.TestData
import java.util.UUID
import java.util.concurrent.Callable
import java.util.concurrent.CountDownLatch
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit

@IntegrationTest
class PayoutApiIntegrationTest @Autowired constructor(
    private val mockMvc: MockMvc,
    private val objectMapper: ObjectMapper,
    private val jdbc: JdbcTemplate,
) {
    private fun body(accountNumber: String, amountMinor: Long = 150_000, currency: String = "KRW") = """
        {
          "bankCode": "BANK01",
          "accountNumber": "$accountNumber",
          "accountHolder": "Jane Doe",
          "amountMinor": $amountMinor,
          "currency": "$currency"
        }
    """.trimIndent()

    private fun postPayout(key: String?, json: String): ResultActionsDsl = mockMvc.post("/payouts") {
        contentType = MediaType.APPLICATION_JSON
        content = json
        key?.let { header(IDEMPOTENCY_KEY, it) }
    }

    private fun MvcResult.json(): JsonNode = objectMapper.readTree(response.contentAsString)

    private fun countPayouts(key: String): Int =
        checkNotNull(jdbc.queryForObject("select count(*) from payout where idempotency_key = ?", Int::class.java, key))

    private fun countOutbox(payoutId: String): Int =
        checkNotNull(jdbc.queryForObject("select count(*) from outbox_message where message_key = ?", Int::class.java, payoutId))

    @Test
    fun `a new request is accepted with 201 and stored together with its outbox event`() {
        val key = UUID.randomUUID().toString()

        val result = postPayout(key, body(TestData.uniqueAccountNumber())).andExpect {
            status { isCreated() }
            header { string("Location", org.hamcrest.Matchers.startsWith("/payouts/")) }
            header { doesNotExist(REPLAYED) }
            jsonPath("$.status") { value("REQUESTED") }
            jsonPath("$.amountMinor") { value(150_000) }
            jsonPath("$.currency") { value("KRW") }
        }.andReturn()

        val payoutId = result.json()["payoutId"].asText()
        countPayouts(key) shouldBe 1
        countOutbox(payoutId) shouldBe 1
    }

    @Test
    fun `retrying with the same key and body returns the same payout and creates nothing new`() {
        val key = UUID.randomUUID().toString()
        val json = body(TestData.uniqueAccountNumber())
        val first = postPayout(key, json).andExpect { status { isCreated() } }.andReturn().json()

        val replay = postPayout(key, json).andExpect {
            status { isCreated() }
            header { string(REPLAYED, "true") }
        }.andReturn().json()

        replay["payoutId"] shouldBe first["payoutId"]
        replay["amountMinor"] shouldBe first["amountMinor"]
        replay["currency"] shouldBe first["currency"]
        replay["createdAt"] shouldBe first["createdAt"]
        countPayouts(key) shouldBe 1
        countOutbox(first["payoutId"].asText()) shouldBe 1
    }

    @Test
    fun `the same key with a different body is rejected with 409`() {
        val key = UUID.randomUUID().toString()
        val account = TestData.uniqueAccountNumber()
        postPayout(key, body(account, amountMinor = 150_000)).andExpect { status { isCreated() } }

        postPayout(key, body(account, amountMinor = 990_000)).andExpect {
            status { isConflict() }
            content { contentTypeCompatibleWith(MediaType.APPLICATION_PROBLEM_JSON) }
            jsonPath("$.title") { value("Idempotency-Key reused") }
        }

        countPayouts(key) shouldBe 1
    }

    @Test
    fun `concurrent requests with one key create exactly one payout`() {
        val key = UUID.randomUUID().toString()
        val json = body(TestData.uniqueAccountNumber())
        val pool = Executors.newFixedThreadPool(THREADS)
        val gate = CountDownLatch(1)

        val responses = try {
            (1..THREADS).map {
                pool.submit(Callable { gate.await(); postPayout(key, json).andReturn() })
            }.also { gate.countDown() }.map { it.get(TIMEOUT_SECONDS, TimeUnit.SECONDS) }
        } finally {
            pool.shutdownNow()
        }

        responses.map { it.response.status }.toSet() shouldBe setOf(CREATED)
        responses.map { it.json()["payoutId"].asText() }.toSet() shouldHaveSize 1
        countPayouts(key) shouldBe 1
    }

    @Test
    fun `a missing Idempotency-Key header is a 400`() {
        postPayout(key = null, json = body(TestData.uniqueAccountNumber())).andExpect { status { isBadRequest() } }
    }

    @Test
    fun `a malformed Idempotency-Key header is a 400`() {
        postPayout(key = "has space", json = body(TestData.uniqueAccountNumber())).andExpect { status { isBadRequest() } }
    }

    @Test
    fun `invalid amounts and currencies are rejected with 400 before anything is stored`() {
        val key = UUID.randomUUID().toString()

        postPayout(key, body(TestData.uniqueAccountNumber(), amountMinor = 0)).andExpect { status { isBadRequest() } }
        postPayout(key, body(TestData.uniqueAccountNumber(), currency = "XYZ")).andExpect { status { isBadRequest() } }
        postPayout(key, "{not json").andExpect { status { isBadRequest() } }

        countPayouts(key) shouldBe 0
    }

    @Test
    fun `a payout can be read back by id and unknown ids are 404`() {
        val created = postPayout(UUID.randomUUID().toString(), body(TestData.uniqueAccountNumber()))
            .andReturn().json()

        val fetched = mockMvc.get("/payouts/${created["payoutId"].asText()}").andExpect {
            status { isOk() }
        }.andReturn().json()

        fetched["payoutId"] shouldBe created["payoutId"]
        fetched["accountNumber"] shouldNotBe null
        fetched["accountNumber"].asText().startsWith("*") shouldBe true
        mockMvc.get("/payouts/${UUID.randomUUID()}").andExpect { status { isNotFound() } }
        mockMvc.get("/payouts/not-a-uuid").andExpect { status { isBadRequest() } }
    }

    private companion object {
        const val IDEMPOTENCY_KEY = "Idempotency-Key"
        const val REPLAYED = "Idempotent-Replayed"
        const val THREADS = 8
        const val CREATED = 201
        const val TIMEOUT_SECONDS = 30L
    }
}
