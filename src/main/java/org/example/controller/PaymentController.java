package org.example.controller;

import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.example.dto.CreatePaymentRequest;
import org.example.dto.PaymentResponse;
import org.example.filter.IdempotencyFilter;
import org.example.service.PaymentOrchestratorService;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * PaymentController — REST API layer for the Payment Orchestration System.
 *
 * <h2>Endpoints</h2>
 * <ul>
 *   <li>{@code POST /v1/payments}   — Create a new payment (idempotent)</li>
 *   <li>{@code GET  /v1/payments/{id}} — Fetch current payment status</li>
 * </ul>
 *
 * <h2>Layer Responsibilities</h2>
 * <p>The controller is intentionally thin:
 * <ul>
 *   <li>Extract HTTP-level concerns (headers, path variables, status codes)</li>
 *   <li>Validate the request body via {@code @Valid} (delegates to Bean Validation)</li>
 *   <li>Delegate ALL business logic to {@link PaymentOrchestratorService}</li>
 *   <li>Map results to appropriate HTTP responses</li>
 * </ul>
 * <p>No business logic, no DB queries, and no Redis/Kafka calls belong here.
 *
 * <h2>Idempotency Header</h2>
 * <p>The {@code Idempotency-Key} header is required for POST and is validated in two places:
 * <ol>
 *   <li>{@code IdempotencyFilter} — rejects missing/blank keys with 400 before the
 *       request reaches this controller</li>
 *   <li>{@code @RequestHeader(required=true)} here — provides compile-time documentation
 *       and an additional 400 if the filter is somehow bypassed</li>
 * </ol>
 */
@Slf4j
@RestController
@RequestMapping("/v1/payments")
@RequiredArgsConstructor
public class PaymentController {

    private final PaymentOrchestratorService paymentOrchestratorService;

    /**
     * POST /v1/payments — Create a new payment.
     *
     * <h3>Request</h3>
     * <pre>
     * POST /v1/payments
     * Headers:
     *   Content-Type: application/json
     *   Idempotency-Key: &lt;unique-uuid-or-string&gt;
     * Body:
     * {
     *   "amount": 150.00,
     *   "currency": "USD",
     *   "paymentMethod": "CARD"
     * }
     * </pre>
     *
     * <h3>Response Scenarios</h3>
     * <ul>
     *   <li>{@code 201 Created}    — Payment initiated; processing started synchronously</li>
     *   <li>{@code 200 OK}         — Idempotent replay of a previously completed payment (from filter)</li>
     *   <li>{@code 400 Bad Request} — Missing Idempotency-Key header or invalid request body</li>
     *   <li>{@code 409 Conflict}   — Idempotency key currently in-flight (concurrent request)</li>
     * </ul>
     *
     * <p>Note: A {@code 201} response may contain a payment in {@code PROCESSING} status
     * if the synchronous provider call failed and the payment was pushed to Kafka for retry.
     * Clients should poll {@code GET /v1/payments/{id}} for the final status.
     *
     * @param idempotencyKey the client-supplied idempotency key from the request header
     * @param request        the validated payment creation request body
     * @return HTTP 201 with the created payment details
     */
    @PostMapping
    public ResponseEntity<PaymentResponse> createPayment(
            @RequestHeader(IdempotencyFilter.IDEMPOTENCY_KEY_HEADER) String idempotencyKey,
            @Valid @RequestBody CreatePaymentRequest request
    ) {
        log.info("POST /v1/payments — Idempotency-Key: [{}], method: {}, amount: {} {}",
                idempotencyKey, request.paymentMethod(), request.amount(), request.currency());

        PaymentResponse response = paymentOrchestratorService.createPayment(request, idempotencyKey);

        log.info("POST /v1/payments — Payment [{}] created with status [{}]",
                response.id(), response.status());

        // Return 201 Created with the payment details in the response body.
        return ResponseEntity.status(HttpStatus.CREATED).body(response);
    }

    /**
     * GET /v1/payments/{id} — Fetch the current real-time status of a payment.
     *
     * <h3>Request</h3>
     * <pre>
     * GET /v1/payments/550e8400-e29b-41d4-a716-446655440000
     * </pre>
     *
     * <h3>Response Scenarios</h3>
     * <ul>
     *   <li>{@code 200 OK}    — Payment found; current status returned</li>
     *   <li>{@code 404 Not Found} — No payment with the given ID exists</li>
     * </ul>
     *
     * <p>This endpoint always reads directly from MySQL (not the Redis cache) to ensure
     * callers get the most up-to-date status, especially during the async Kafka retry phase.
     *
     * @param id the UUID of the payment to fetch
     * @return HTTP 200 with the current payment state
     */
    @GetMapping("/{id}")
    public ResponseEntity<PaymentResponse> getPayment(@PathVariable String id) {
        log.debug("GET /v1/payments/{}", id);

        PaymentResponse response = paymentOrchestratorService.getPayment(id);

        log.debug("GET /v1/payments/{} — status: {}", id, response.status());

        return ResponseEntity.ok(response);
    }
}

