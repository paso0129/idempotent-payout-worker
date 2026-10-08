package payout.config

import org.springframework.boot.context.properties.ConfigurationProperties
import java.time.ZoneId

@ConfigurationProperties("payout.reconciliation")
data class ReconciliationProperties(
    /** Business day boundaries are local midnight in this zone. */
    val zone: ZoneId = ZoneId.of("Asia/Seoul"),
)
