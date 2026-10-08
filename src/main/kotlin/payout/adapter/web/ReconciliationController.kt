package payout.adapter.web

import org.springframework.format.annotation.DateTimeFormat
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.RequestParam
import org.springframework.web.bind.annotation.RestController
import payout.application.ReconciliationService
import payout.config.ReconciliationProperties
import payout.domain.reconciliation.Discrepancy
import java.time.LocalDate

data class ReconciliationResponse(
    val date: LocalDate,
    val zone: String,
    val matched: Int,
    val discrepancies: List<Discrepancy>,
)

@RestController
class ReconciliationController(
    private val reconciliation: ReconciliationService,
    private val properties: ReconciliationProperties,
) {
    @GetMapping("/reconciliation")
    fun report(@RequestParam @DateTimeFormat(iso = DateTimeFormat.ISO.DATE) date: LocalDate): ReconciliationResponse {
        val report = reconciliation.reconcile(date)
        return ReconciliationResponse(date, properties.zone.id, report.matched, report.discrepancies)
    }
}
