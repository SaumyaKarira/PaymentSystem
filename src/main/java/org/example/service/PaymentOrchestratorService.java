package org.example.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.example.dto.CreatePaymentRequest;
import org.example.dto.PaymentEvent;
import org.example.dto.PaymentResponse;
import org.example.entity.Payment;
import org.example.entity.PaymentStatus;
import org.example.exception.PaymentNotFoundException;
import org.example.exception.ProviderException;
import org.example.provider.PaymentProviderConnector;
import org.example.repository.PaymentRepository;
import org.example.routing.RoutingEngine;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.UUID;

// Orchestrates the full payment lifecycle: idempotency check → DB persist → provider call → Kafka publish.
// @Transactional annotations are placed on PaymentRepository methods (not here) to avoid the
// Spring proxy self-invocation problem where internal this.method() calls bypass the proxy.
@Slf4j
@Service
@RequiredArgsConstructor
public class PaymentOrchestratorService {

    private final PaymentRepository paymentRepository;
    private final IdempotencyService idempotencyService;
    private final RoutingEngine routingEngine;
    private final KafkaTemplate<String, PaymentEvent> kafkaTemplate;

    @Value("${payment.kafka.main-topic:payment-main-topic}")
    private String mainTopic;

    // Not @Transactional — spans Redis, provider HTTP call, and Kafka publish.
    // Each DB write is handled by repository-level @Transactional instead.
    public PaymentResponse createPayment(CreatePaymentRequest request, String idempotencyKey) {

        // ── STEP 1: ACQUIRE REDIS IN-FLIGHT LOCK ─────────────────────────────
        // Redis SET NX (atomic set-if-not-exists). Only one thread can hold this lock
        // for a given idempotency key. The filter already checked, but this is the
        // definitive guard against any concurrent requests that slipped through.
        boolean lockAcquired = idempotencyService.acquireInFlightLock(idempotencyKey);
        if (!lockAcquired) {
            log.warn("Idempotency lock contention for key [{}]", idempotencyKey);
            return idempotencyService.getCachedResponse(idempotencyKey)
                    .orElseThrow(() -> new org.example.exception.IdempotencyConflictException(idempotencyKey));
        }

        try {
            // ── STEP 2: PERSIST PAYMENT IN INITIATED STATE ───────────────────
            // paymentRepository.save() uses Spring Data's built-in save which is
            // @Transactional by default in SimpleJpaRepository — so this INSERT
            // is committed as soon as save() returns. No proxy-bypass issue here
            // because we are calling the repository bean (a Spring proxy), not this.
            //
            // NOTE: We assign the result of save() to a NEW final variable `savedPayment`
            // rather than reassigning the builder result. This is required because lambdas
            // (used in orElseThrow below) can only capture variables that are effectively
            // final — a variable reassigned after declaration is NOT effectively final.
            Payment builtPayment = Payment.builder()
                    .id(UUID.randomUUID().toString())
                    .idempotencyKey(idempotencyKey)
                    .amount(request.amount())
                    .currency(request.currency())
                    .paymentMethod(request.paymentMethod())
                    .status(PaymentStatus.INITIATED)
                    .retryCount(0)
                    .build();
            final Payment payment = paymentRepository.save(builtPayment);
            log.info("Payment [{}] saved in INITIATED state", payment.getId());

            // ── STEP 3: SELECT PROVIDER VIA ROUTING ENGINE ───────────────────
            PaymentProviderConnector connector = routingEngine.route(request.paymentMethod());
            log.info("Payment [{}] routed to provider [{}]", payment.getId(), connector.getProviderId());

            try {
                // ── STEP 4a: CALL PROVIDER (simulated — 20% failure rate) ────
                String providerRefId = connector.processPayment(
                        payment.getId(),
                        payment.getAmount(),
                        payment.getCurrency()
                );

                // ── STEP 4b: PROVIDER SUCCEEDED → UPDATE TO SUCCESS ──────────
                // This calls paymentRepository.updateOnSuccess() which carries
                // @Transactional + @Modifying(clearAutomatically=true).
                // The UPDATE is executed and committed immediately.
                // version=0 because payment was just inserted — no other thread
                // could have modified it yet between INSERT and here.
                int updated = paymentRepository.updateOnSuccess(
                        payment.getId(),
                        PaymentStatus.SUCCESS,
                        connector.getProviderId(),
                        providerRefId,
                        payment.getRetryCount(),
                        payment.getVersion()   // 0L from fresh INSERT
                );
                if (updated == 0) {
                    // Extremely rare: another thread (Kafka consumer) updated this
                    // payment concurrently between our INSERT and this UPDATE.
                    // The other update wins — log and continue.
                    log.warn("Optimistic lock conflict on SUCCESS update for payment [{}]", payment.getId());
                } else {
                    log.info("Payment [{}] → SUCCESS via provider [{}], ref: {}",
                            payment.getId(), connector.getProviderId(), providerRefId);
                }

                // Reload the latest entity from DB.
                // clearAutomatically=true on updateOnSuccess ensured the EntityManager
                // cache was flushed, so findById now returns the updated row.
                Payment successPayment = paymentRepository.findById(payment.getId())
                        .orElseThrow(() -> new PaymentNotFoundException(payment.getId()));

                PaymentResponse response = PaymentResponse.from(successPayment);

                // Cache in Redis so idempotent replays skip all DB/provider calls
                idempotencyService.storeCompletedResponse(idempotencyKey, response);
                return response;

            } catch (ProviderException providerEx) {
                // ── STEP 4c: PROVIDER FAILED → TRANSITION TO PROCESSING ──────
                // The synchronous provider call failed (504/500). We do NOT mark
                // as FAILED here — the Kafka retry pipeline will attempt recovery.
                log.warn("Payment [{}] provider [{}] failed: {}. Moving to Kafka retry.",
                        payment.getId(), providerEx.getProviderName(), providerEx.getMessage());

                // UPDATE status → PROCESSING.
                // @Transactional + @Modifying on the repository method ensures this
                // commits even though we are in a non-transactional service method.
                int updated = paymentRepository.updateStatusWithVersionCheck(
                        payment.getId(),
                        PaymentStatus.PROCESSING,
                        payment.getRetryCount(),   // still 0 — first attempt
                        payment.getVersion()        // 0L from fresh INSERT
                );
                if (updated == 0) {
                    log.warn("Version conflict transitioning payment [{}] to PROCESSING", payment.getId());
                }

                // Publish to Kafka. The payment ID is the message key — this guarantees
                // all retry events for this payment go to the same partition, preserving
                // the processing order for this specific payment.
                PaymentEvent event = new PaymentEvent(
                        payment.getId(),
                        payment.getAmount(),
                        payment.getCurrency(),
                        payment.getPaymentMethod(),
                        payment.getRetryCount()
                );
                kafkaTemplate.send(mainTopic, payment.getId(), event);
                log.info("Payment [{}] published to Kafka [{}] for non-blocking retry",
                        payment.getId(), mainTopic);

                // Reload fresh entity after the PROCESSING update
                Payment processingPayment = paymentRepository.findById(payment.getId())
                        .orElseThrow(() -> new PaymentNotFoundException(payment.getId()));

                PaymentResponse processingResponse = PaymentResponse.from(processingPayment);

                // Cache the PROCESSING response so idempotent replays return tracking info
                idempotencyService.storeCompletedResponse(idempotencyKey, processingResponse);

                // Return immediately — client thread is released; Kafka handles retry async
                return processingResponse;
            }

        } catch (Exception unexpectedException) {
            // Release the Redis idempotency key on any unhandled error so the client
            // can safely retry once the underlying issue is resolved.
            log.error("Unexpected error creating payment for key [{}]: {}",
                    idempotencyKey, unexpectedException.getMessage(), unexpectedException);
            idempotencyService.releaseKey(idempotencyKey);
            throw unexpectedException;
        }
    }

    // Fetches live payment state from MySQL — readOnly=true skips dirty checking for a slight perf gain.
    @Transactional(readOnly = true)
    public PaymentResponse getPayment(String paymentId) {
        Payment payment = paymentRepository.findById(paymentId)
                .orElseThrow(() -> new PaymentNotFoundException(paymentId));
        return PaymentResponse.from(payment);
    }
}

