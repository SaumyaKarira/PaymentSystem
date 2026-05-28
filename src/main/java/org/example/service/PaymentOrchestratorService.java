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

/**
 * PaymentOrchestratorService — core orchestration for the payment lifecycle.
 *
 * <h2>The @Transactional Proxy Problem — Why Internal Calls Don't Work</h2>
 * <p>Spring's {@code @Transactional} works via a JDK/CGLIB proxy. When you call a
 * {@code @Transactional} method from OUTSIDE the bean (through the proxy), Spring
 * intercepts the call and wraps it in a transaction. But when a method inside the
 * same class calls another method in the same class using {@code this.method()},
 * it bypasses the proxy entirely — the transaction annotation is silently ignored.
 *
 * <p>The previous implementation had {@code protected @Transactional} methods like
 * {@code persistNewPayment()}, {@code updatePaymentOnSuccess()}, etc., which were
 * all called via {@code this.} — meaning their {@code @Transactional} annotations
 * NEVER fired. When the JPQL {@code @Modifying} queries then ran with no active
 * transaction, JPA threw:
 * <pre>
 *   TransactionRequiredException: Executing an update/delete query
 * </pre>
 *
 * <h2>The Fix Applied Here</h2>
 * <p>The fix is two-part:
 * <ol>
 *   <li><strong>Repository level</strong>: {@code @Transactional} is added directly
 *       on the {@code @Modifying} methods in {@code PaymentRepository}. This is the
 *       correct place — it guarantees a transaction at the repository boundary
 *       regardless of what the caller does, since the repository IS a Spring-proxied
 *       interface bean and its methods are always called through the proxy.</li>
 *   <li><strong>Service level</strong>: The internal helper methods that were
 *       {@code protected @Transactional} are inlined directly into the public method
 *       flow, removing the dead same-class transaction annotations. The service
 *       methods that ARE public and called from outside (like {@code getPayment()})
 *       retain their {@code @Transactional} annotation correctly.</li>
 * </ol>
 *
 * <h2>Payment State Transition Flow</h2>
 *
 * <h3>Happy Path (80% — synchronous SUCCESS)</h3>
 * <pre>
 * 1. Redis SET NX "idempotency:K" IN_FLIGHT  → lock acquired
 * 2. INSERT payments (status=INITIATED, version=0)  → committed immediately
 * 3. ProviderConnector.processPayment()  → returns providerRefId
 * 4. paymentRepository.updateOnSuccess()  — @Transactional on the repository method
 *    → UPDATE payments SET status=SUCCESS, version=1 WHERE id=? AND version=0
 * 5. paymentRepository.findById()  → returns fresh entity (cache cleared by clearAutomatically=true)
 * 6. Redis SET "idempotency:K" {PaymentResponse}  → cached for 24h
 * 7. Return 201 {status: SUCCESS}
 * </pre>
 *
 * <h3>Failure Path (20% — async Kafka retry → SUCCESS or FAILED)</h3>
 * <pre>
 * 1. Redis SET NX → lock acquired
 * 2. INSERT payments (status=INITIATED, version=0) → committed
 * 3. ProviderConnector.processPayment() → THROWS ProviderException (504/500)
 * 4. paymentRepository.updateStatusWithVersionCheck(PROCESSING, retryCount=0, version=0)
 *    → UPDATE payments SET status=PROCESSING, version=1 WHERE id=? AND version=0
 *    → @Transactional on repository method ensures this commits
 * 5. kafkaTemplate.send("payment-main-topic", paymentId, event)
 *    → message published; client thread released immediately (non-blocking)
 * 6. Redis SET "idempotency:K" {status: PROCESSING} → cached
 * 7. Return 201 {status: PROCESSING}
 *
 * --- Async Kafka path (separate thread) ---
 * 8. PaymentRetryConsumer.processPayment() attempt #1
 *    → routes to primary provider (CARD → ProviderA, UPI → ProviderB)
 *    → if fails: updateStatusWithVersionCheck(PROCESSING, retryCount=1, version=1)
 *    → throws ProviderException → @RetryableTopic forwards to retry-0 (after 2s)
 * 9. attempt #2, #3, #4 → same primary provider
 *    → if final attempt fails → DLT handler fires
 *    → updateStatusWithVersionCheck(FAILED, retryCount=4, version=4) → status=FAILED
 *    → if any attempt succeeds: updateOnSuccess(SUCCESS, ...) → status=SUCCESS in DB
 * </pre>
 */
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

    /**
     * Creates a new payment and orchestrates the full synchronous processing flow.
     *
     * <p><strong>Transaction design:</strong> This method is NOT annotated with
     * {@code @Transactional} because it spans multiple non-DB operations (Redis lock,
     * external provider HTTP call, Kafka publish). Wrapping all of this in a single DB
     * transaction would hold a connection open during the provider call — wasteful and
     * risky. Instead, each DB operation delegates to the repository which carries its
     * own {@code @Transactional} annotation, ensuring each write is committed
     * independently and promptly.
     */
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
                        payment.getRetryCount(),
                        payment.getVersion()
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

    /**
     * Fetches a payment by UUID from MySQL — always reads live data, never Redis.
     *
     * <p>{@code @Transactional(readOnly = true)} tells the JPA provider to use a
     * read-only transaction — no dirty checking, no flush, which gives a small
     * performance benefit for SELECT-only operations.
     */
    @Transactional(readOnly = true)
    public PaymentResponse getPayment(String paymentId) {
        Payment payment = paymentRepository.findById(paymentId)
                .orElseThrow(() -> new PaymentNotFoundException(paymentId));
        return PaymentResponse.from(payment);
    }
}

