package payout.adapter.kafka

import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.springframework.stereotype.Component

@Component
class KafkaMessageReader(private val objectMapper: ObjectMapper) {

    fun messageId(record: ConsumerRecord<String, String>): String =
        header(record, MessageHeaders.MESSAGE_ID)?.takeIf { it.isNotBlank() }
            ?: throw InvalidMessageException("record ${record.topic()}-${record.partition()}@${record.offset()} has no ${MessageHeaders.MESSAGE_ID} header")

    fun <T> payload(record: ConsumerRecord<String, String>, type: Class<T>): T =
        try {
            objectMapper.readValue(record.value(), type)
        } catch (e: Exception) {
            throw InvalidMessageException("record ${record.topic()}-${record.partition()}@${record.offset()} is not a valid ${type.simpleName}", e)
        }

    fun header(record: ConsumerRecord<String, String>, name: String): String? =
        record.headers().lastHeader(name)?.value()?.toString(Charsets.UTF_8)
}
