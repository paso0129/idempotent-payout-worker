package payout.support

import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.boot.test.context.TestConfiguration
import org.springframework.boot.testcontainers.service.connection.ServiceConnection
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Import
import org.springframework.context.annotation.Primary
import org.springframework.test.context.ActiveProfiles
import org.testcontainers.containers.PostgreSQLContainer
import org.testcontainers.kafka.KafkaContainer
import org.testcontainers.utility.DockerImageName
import payout.adapter.outbox.KafkaOutboxPublisher

/**
 * Real PostgreSQL and Kafka. Every integration test shares one Spring context (and so one pair of
 * containers): tests isolate themselves with fresh ids instead of cleaning tables.
 */
@TestConfiguration(proxyBeanMethods = false)
class TestcontainersConfig {
    @Bean
    @ServiceConnection
    fun postgres(): PostgreSQLContainer<*> = PostgreSQLContainer(DockerImageName.parse(POSTGRES_IMAGE))

    @Bean
    @ServiceConnection
    fun kafka(): KafkaContainer = KafkaContainer(DockerImageName.parse(KAFKA_IMAGE))

    @Bean
    @Primary
    fun recordingOutboxPublisher(delegate: KafkaOutboxPublisher) = RecordingOutboxPublisher(delegate)

    private companion object {
        const val POSTGRES_IMAGE = "postgres:16-alpine"
        const val KAFKA_IMAGE = "apache/kafka:3.9.1"
    }
}

@Target(AnnotationTarget.CLASS)
@Retention(AnnotationRetention.RUNTIME)
@SpringBootTest
@AutoConfigureMockMvc
@Import(TestcontainersConfig::class)
@ActiveProfiles("test")
annotation class IntegrationTest
