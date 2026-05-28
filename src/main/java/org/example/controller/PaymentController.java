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

// REST API layer for the Payment Orchestration System.
// POST /v1/payments  — create a new payment (idempotent)
// GET  /v1/payments/{id} — fetch current payment status
@Slf4j
@RestController
@RequestMapping("/v1/payments")
@RequiredArgsConstructor
public class PaymentController {

    private final PaymentOrchestratorService paymentOrchestratorService;

    // POST /v1/payments — creates a new payment.
    // Returns 201 with status SUCCESS or PROCESSING (if provider failed and Kafka retry was triggered).
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

        return ResponseEntity.status(HttpStatus.CREATED).body(response);
    }

    // GET /v1/payments/{id} ��� fetches the current real-time status from MySQL (not Redis cache).
    @GetMapping("/{id}")
    public ResponseEntity<PaymentResponse> getPayment(@PathVariable String id) {
        log.debug("GET /v1/payments/{}", id);

        PaymentResponse response = paymentOrchestratorService.getPayment(id);

        log.debug("GET /v1/payments/{} — status: {}", id, response.status());

        return ResponseEntity.ok(response);
    }
}
