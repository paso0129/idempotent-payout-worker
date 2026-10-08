package payout.adapter.web

import jakarta.validation.Valid
import org.springframework.http.HttpStatus
import org.springframework.http.ProblemDetail
import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.ExceptionHandler
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.PathVariable
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RequestBody
import org.springframework.web.bind.annotation.RequestHeader
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RestController
import org.springframework.web.bind.annotation.RestControllerAdvice
import payout.application.IdempotencyKeyConflictException
import payout.application.PayoutNotFoundException
import payout.application.PayoutQueryService
import payout.application.RequestPayoutResult
import payout.application.RequestPayoutService
import payout.domain.PayoutId
import java.net.URI
import java.util.UUID

@RestController
@RequestMapping("/payouts")
class PayoutController(
    private val requestPayout: RequestPayoutService,
    private val payoutQuery: PayoutQueryService,
) {
    /**
     * A retry with the same key gets the same payout back with the same status code, flagged by
     * `Idempotent-Replayed: true`. The body shows the payout's current state.
     */
    @PostMapping
    fun request(
        @RequestHeader(IDEMPOTENCY_KEY) idempotencyKey: String,
        @Valid @RequestBody body: PayoutRequestBody,
    ): ResponseEntity<PayoutResponse> {
        val result = requestPayout.request(body.toCommand(idempotencyKey))
        val response = ResponseEntity.created(URI.create("/payouts/${result.payout.id}"))
        if (result is RequestPayoutResult.Replayed) response.header(REPLAYED, "true")
        return response.body(PayoutResponse.from(result.payout))
    }

    @GetMapping("/{payoutId}")
    fun get(@PathVariable payoutId: UUID): PayoutResponse = PayoutResponse.from(payoutQuery.get(PayoutId(payoutId)))

    companion object {
        const val IDEMPOTENCY_KEY = "Idempotency-Key"
        const val REPLAYED = "Idempotent-Replayed"
    }
}

/** RFC 9457 problem details. Framework errors (validation, missing header) use Spring's defaults. */
@RestControllerAdvice
class ApiExceptionHandler {

    @ExceptionHandler(IdempotencyKeyConflictException::class)
    fun conflict(e: IdempotencyKeyConflictException): ProblemDetail =
        problem(HttpStatus.CONFLICT, "Idempotency-Key reused", e.message)

    @ExceptionHandler(InvalidPayoutRequestException::class)
    fun invalid(e: InvalidPayoutRequestException): ProblemDetail =
        problem(HttpStatus.BAD_REQUEST, "Invalid payout request", e.message)

    @ExceptionHandler(PayoutNotFoundException::class)
    fun notFound(e: PayoutNotFoundException): ProblemDetail =
        problem(HttpStatus.NOT_FOUND, "Payout not found", e.message)

    private fun problem(status: HttpStatus, title: String, detail: String?): ProblemDetail =
        ProblemDetail.forStatusAndDetail(status, detail.orEmpty()).also { it.title = title }
}
