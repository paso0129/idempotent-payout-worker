package payout.adapter.kafka

import org.apache.kafka.clients.admin.NewTopic
import org.springframework.boot.context.properties.ConfigurationProperties
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.kafka.annotation.EnableKafkaRetryTopic
import org.springframework.kafka.config.TopicBuilder
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.kafka.retrytopic.DltStrategy
import org.springframework.kafka.retrytopic.RetryTopicConfiguration
import org.springframework.kafka.retrytopic.RetryTopicConfigurationBuilder
import payout.application.PayoutNotFoundException
import payout.domain.IllegalStatusTransitionException
import java.time.Duration

@ConfigurationProperties("payout.kafka")
data class PayoutKafkaProperties(
    val partitions: Int = 3,
    val replicationFactor: Short = 1,
    /** `payout.requested`: bank errors clear up in seconds or not at all. */
    val retry: Retry = Retry(),
    /**
     * `payout.result`: a result can overtake our own SENT commit, and after a worker crash that
     * commit waits for a rebalance and a redelivery. Defaults wait about 8 minutes in total.
     */
    val resultRetry: Retry = Retry(
        attempts = 6,
        initialDelay = Duration.ofSeconds(5),
        multiplier = 3.0,
        maxDelay = Duration.ofMinutes(5),
    ),
    /** Retry and DLT topics are mostly idle; one consumer thread each keeps the DB pool small. */
    val retryConcurrency: Int = 1,
) {
    /** Non-blocking retries: each delay is its own retry topic, then the DLT. */
    data class Retry(
        /** Total deliveries including the first one. */
        val attempts: Int = 4,
        val initialDelay: Duration = Duration.ofSeconds(1),
        val multiplier: Double = 2.0,
        val maxDelay: Duration = Duration.ofSeconds(30),
    ) {
        init {
            require(attempts >= 1) { "payout.kafka.retry.attempts must be at least 1" }
        }
    }
}

@Configuration
@EnableKafkaRetryTopic
class KafkaConfig(private val properties: PayoutKafkaProperties) {

    @Bean
    fun payoutRequestedTopic(): NewTopic = topic(Topics.PAYOUT_REQUESTED)

    @Bean
    fun payoutResultTopic(): NewTopic = topic(Topics.PAYOUT_RESULT)

    @Bean
    fun payoutRequestedRetry(template: KafkaTemplate<String, String>): RetryTopicConfiguration =
        retryTopics(properties.retry)
            .includeTopic(Topics.PAYOUT_REQUESTED)
            .dltHandlerMethod(PayoutRequestedDltHandler.BEAN_NAME, PayoutRequestedDltHandler.METHOD_NAME)
            .create(template)

    @Bean
    fun payoutResultRetry(template: KafkaTemplate<String, String>): RetryTopicConfiguration =
        retryTopics(properties.resultRetry)
            .includeTopic(Topics.PAYOUT_RESULT)
            .dltHandlerMethod(PayoutResultDltHandler.BEAN_NAME, PayoutResultDltHandler.METHOD_NAME)
            .create(template)

    private fun topic(name: String): NewTopic =
        TopicBuilder.name(name).partitions(properties.partitions).replicas(properties.replicationFactor.toInt()).build()

    private fun retryTopics(retry: PayoutKafkaProperties.Retry): RetryTopicConfigurationBuilder =
        RetryTopicConfigurationBuilder.newInstance()
            .maxAttempts(retry.attempts)
            .exponentialBackoff(retry.initialDelay.toMillis(), retry.multiplier, retry.maxDelay.toMillis())
            .notRetryOn(NON_RETRYABLE)
            .traversingCauses()
            .suffixTopicsWithIndexValues()
            .concurrency(properties.retryConcurrency)
            // A DLT handler that throws (database down) gets the record back instead of losing it.
            .dltProcessingFailureStrategy(DltStrategy.ALWAYS_RETRY_ON_ERROR)
            .autoCreateTopicsWith(properties.partitions, properties.replicationFactor)

    companion object {
        /** Retrying cannot change the answer: these go straight to the DLT. */
        val NON_RETRYABLE: List<Class<out Throwable>> = listOf(
            InvalidMessageException::class.java,
            PayoutNotFoundException::class.java,
            IllegalStatusTransitionException::class.java,
        )
    }
}
