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
import org.springframework.dao.OptimisticLockingFailureException;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.UUID;

/**
 * PaymentOrchestratorService — the heart of the payment system.
 *
 * <p>This service coordinates the full payment lifecycle for synchronous API requests:
 * <ol>
 *   <li>Idempotency lock acquisition in Redis</li>
 *   <li>Payment entity creation and persistence in MySQL</li>
 *   <li>Synchronous provider call via the routing engine</li>
 *   <li>Status update (SUCCESS or PROCESSING) with optimistic lock protection</li>
 *   <li>Kafka publishing on provider failure for async retry</li>
 *   <li>Redis response caching on success</li>
 * </ol>
 *
 * <h2>Concurrency Safety</h2>
 * <p>Two concurrency scenarios are protected:
 * <ul>
 *   <li><strong>Duplicate HTTP requests</strong>: The {@code IdempotencyFilter} + Redis
 *       {@code SET NX} lock in {@code IdempotencyService.acquireInFlightLock()} ensures
 *       only one request proceeds for a given idempotency key at a time.</li>
 *   <li><strong>HTTP request vs. Kafka consumer race</strong>: The {@code @Version} field
 *       on the {@link Payment} entity means that if both a REST thread and a Kafka thread
 *       attempt to update the same payment row, the second one will see
 *       {@code ObjectOptimisticLockingFailureException} (Hibernate detects that the
 *       version in the DB no longer matches the version read by the losing thread).</li>
 * </ul>
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class PaymentOrchestratorService {

    private final PaymentRepository paymentRepository;
    private final IdempotencyService idempotencyService;
    private final RoutingEngine routingEngine;
    private final KafkaTemplate<String, PaymentEvent> kafkaTemplate;

    /**
     * Injected from {@code application.yml: payment.kafka.main-topic}.
     * Defaults to "payment-main-topic" if the property is absent.
     */
    @Value("${payment.kafka.main-topic:payment-main-topic}")
    private String mainTopic;

    /**
     * Creates a new payment, attempts synchronous processing, and handles failure gracefully.
     *
     * <p>This method is intentionally NOT annotated with {@code @Transactional} at the
     * method level because it spans multiple non-transactional operations (Redis lock,
     * DB save, external HTTP call, Kafka publish).  Individual DB operations use
     * inner {@code @Transactional} methods to demarcate their own transaction boundaries.
     *
     * @param request         the validated payment creation request DTO
     * @param idempotencyKey  the value from the {@code Idempotency-Key} header
     * @return a {@link PaymentResponse} representing the newly created payment
     */
    public PaymentResponse createPayment(CreatePaymentRequest request, String idempotencyKey) {

        // ── STEP 1: ACQUIRE REDIS IN-FLIGHT LOCK ─────────────────────────────
        // Try to atomically set the Redis key to IN_FLIGHT.
        // acquireInFlightLock() uses SET NX (set if not exists) — only one thread wins.
        // The filter already checked for in-flight and cached states, but between the
        // filter check and here, a concurrent request could have slipped through.
        // This lock is the definitive guard against concurrent duplicate processing.
        boolean lockAcquired = idempotencyService.acquireInFlightLock(idempotencyKey);
        if (!lockAcquired) {
            // Another request beat us to the lock. This is extremely rare in practice
            // (the filter handles the common case), but must be handled correctly.
            log.warn("Idempotency lock contention for key [{}] — concurrent request won the lock",
                    idempotencyKey);
            // Check if a completed response exists (the other request may have finished)
            return idempotencyService.getCachedResponse(idempotencyKey)
                    .orElseThrow(() -> new org.example.exception.IdempotencyConflictException(idempotencyKey));
        }

        try {
            // ── STEP 2: PERSIST INITIAL PAYMENT RECORD ───────────────────────
            // Create and save the payment in INITIATED state.
            // This is done in its own @Transactional method to ensure the record
            // is committed to MySQL before we attempt the provider call.
            Payment payment = persistNewPayment(request, idempotencyKey);
            log.info("Payment [{}] created in INITIATED state for idempotency key [{}]",
                    payment.getId(), idempotencyKey);

            // ── STEP 3: ROUTE & ATTEMPT SYNCHRONOUS PROVIDER CALL ────────────
            // Select the primary provider based on payment method.
            PaymentProviderConnector connector = routingEngine.route(request.paymentMethod());
            log.info("Payment [{}] routed to provider [{}]", payment.getId(), connector.getProviderId());

            try {
                // Call the (simulated) external provider — may throw ProviderException ~20%
                String providerRefId = connector.processPayment(
                        payment.getId(),
                        payment.getAmount(),
                        payment.getCurrency()
                );

                // ── STEP 4a: SUCCESS PATH ─────────────────────────────────────
                // Update the payment status to SUCCESS with provider details.
                // Uses version-guarded update to prevent race conditions.
                updatePaymentOnSuccess(payment, connector.getProviderId(), providerRefId);
                log.info("Payment [{}] succeeded via provider [{}]. Ref: {}",
                        payment.getId(), connector.getProviderId(), providerRefId);

                // Reload the payment to get the latest state (post-update timestamps, etc.)
                Payment updatedPayment = paymentRepository.findById(payment.getId())
                        .orElseThrow(() -> new PaymentNotFoundException(payment.getId()));

                PaymentResponse successResponse = PaymentResponse.from(updatedPayment);

                // Cache the completed response in Redis — future duplicate requests will
                // receive this directly from the filter without hitting MySQL.
                idempotencyService.storeCompletedResponse(idempotencyKey, successResponse);
                return successResponse;

            } catch (ProviderException providerEx) {
                // ── STEP 4b: FAILURE PATH — TRANSITION TO KAFKA RETRY ────────
                // The primary provider call failed (simulated 5xx/timeout).
                // Transition the payment to PROCESSING status and push to Kafka.
                log.warn("Payment [{}] primary provider [{}] failed: {}. Pushing to retry pipeline.",
                        payment.getId(), providerEx.getProviderName(), providerEx.getMessage());

                updatePaymentToProcessing(payment, connector.getProviderId());

                // Publish the PaymentEvent to Kafka for non-blocking retry.
                // We use the payment ID as the Kafka message key to ensure ordering:
                // all retry events for the same payment go to the same partition.
                PaymentEvent event = new PaymentEvent(
                        payment.getId(),
                        payment.getAmount(),
                        payment.getCurrency(),
                        payment.getPaymentMethod(),
                        payment.getRetryCount(),
                        payment.getVersion()
                );
                kafkaTemplate.send(mainTopic, payment.getId(), event);
                log.info("Payment [{}] published to Kafka topic [{}] for retry", payment.getId(), mainTopic);

                // Reload and return the current state (PROCESSING) to the caller.
                // The caller can poll GET /v1/payments/{id} to check the outcome.
                Payment processingPayment = paymentRepository.findById(payment.getId())
                        .orElseThrow(() -> new PaymentNotFoundException(payment.getId()));
                PaymentResponse processingResponse = PaymentResponse.from(processingPayment);

                // Store the current (PROCESSING) response in Redis.
                // This ensures idempotent re-calls while retry is in flight will see
                // the current status rather than re-entering the creation flow.
                idempotencyService.storeCompletedResponse(idempotencyKey, processingResponse);
                return processingResponse;
            }

        } catch (Exception unexpectedException) {
            // ── UNEXPECTED ERROR CLEANUP ──────────────────────────────────────
            // For any unhandled exception (e.g., DB connectivity issues), release the
            // Redis idempotency key so the client can retry after the error is resolved.
            // Without this, the key would remain as IN_FLIGHT for 24 hours.
            log.error("Unexpected error during payment creation for idempotency key [{}]: {}",
                    idempotencyKey, unexpectedException.getMessage(), unexpectedException);
            idempotencyService.releaseKey(idempotencyKey);
            throw unexpectedException;
        }
    }

    /**
     * Fetches a payment by its UUID from MySQL.
     *
     * <p>This is a read-only operation — no locks, no Redis involvement.
     * Spring Data JPA's {@code findById} issues a simple {@code SELECT} query.
     *
     * @param paymentId the UUID of the payment to fetch
     * @return the payment response DTO
     * @throws PaymentNotFoundException if no payment with the given ID exists
     */
    @Transactional(readOnly = true)
    public PaymentResponse getPayment(String paymentId) {
        Payment payment = paymentRepository.findById(paymentId)
                .orElseThrow(() -> new PaymentNotFoundException(paymentId));
        return PaymentResponse.from(payment);
    }

    /**
     * Persists a new {@link Payment} entity in INITIATED state.
     *
     * <p>{@code @Transactional} ensures this entire DB write is committed atomically.
     * If the {@code save()} fails (e.g., duplicate idempotency key due to a DB race),
     * the transaction rolls back automatically.
     *
     * @param request        the payment request
     * @param idempotencyKey the idempotency key
     * @return the saved (and now DB-persisted) Payment entity
     */
    @Transactional
    protected Payment persistNewPayment(CreatePaymentRequest request, String idempotencyKey) {
        Payment payment = Payment.builder()
                .id(UUID.randomUUID().toString())
                .idempotencyKey(idempotencyKey)
                .amount(request.amount())
                .currency(request.currency())
                .paymentMethod(request.paymentMethod())
                .status(PaymentStatus.INITIATED)
                .retryCount(0)
                .build();
        return paymentRepository.save(payment);
    }

    /**
     * Updates a payment to SUCCESS state with provider details.
     *
     * <h2>Optimistic Locking Comment</h2>
     * <p>We use the custom {@code updateOnSuccess} JPQL query which includes
     * {@code WHERE version = :version} in its WHERE clause.  This is equivalent to
     * what Hibernate does with {@code @Version} on a standard {@code save()}, but it
     * avoids a SELECT before the UPDATE (saves one DB round-trip).
     *
     * <p>If the update returns 0 rows (version mismatch), it means the Kafka consumer
     * concurrently updated this payment first.  In this extremely rare scenario, we log
     * a warning and do not throw — the Kafka consumer's update takes precedence.
     *
     * @param payment             the payment entity (with current version)
     * @param providerId          the provider that succeeded
     * @param providerReferenceId the provider's transaction ID
     */
    @Transactional
    protected void updatePaymentOnSuccess(Payment payment, String providerId, String providerReferenceId) {
        try {
            int rowsUpdated = paymentRepository.updateOnSuccess(
                    payment.getId(),
                    PaymentStatus.SUCCESS,
                    providerId,
                    providerReferenceId,
                    payment.getRetryCount(),
                    payment.getVersion()
            );
            if (rowsUpdated == 0) {
                // Version mismatch: another thread (Kafka consumer) already updated this record.
                // This is an expected race condition under concurrent processing.
                // The other thread's update is presumed correct — log and continue.
                log.warn("Optimistic lock conflict during SUCCESS update for payment [{}]. "
                        + "Another thread may have already updated the status.", payment.getId());
            }
        } catch (OptimisticLockingFailureException e) {
            // Hibernate-level optimistic lock exception (from save() path, not our custom query).
            // Same handling: log and continue. The other transaction's state is authoritative.
            log.warn("ObjectOptimisticLockingFailureException for payment [{}]: {}",
                    payment.getId(), e.getMessage());
        }
    }

    /**
     * Transitions a payment from INITIATED to PROCESSING state.
     *
     * <p>Called after the primary provider call fails.  The {@code WHERE version = :version}
     * guard ensures we don't accidentally overwrite a concurrent SUCCESS update from another
     * thread that might have retried immediately.
     *
     * @param payment     the payment entity (with current version)
     * @param providerId  the provider that failed (recorded for observability)
     */
    @Transactional
    protected void updatePaymentToProcessing(Payment payment, String providerId) {
        // Update provider_id so we know which connector was tried on the sync path
        payment.setProviderId(providerId);
        try {
            int rowsUpdated = paymentRepository.updateStatusWithVersionCheck(
                    payment.getId(),
                    PaymentStatus.PROCESSING,
                    payment.getRetryCount(),
                    payment.getVersion()
            );
            if (rowsUpdated == 0) {
                log.warn("Version conflict transitioning payment [{}] to PROCESSING. "
                        + "Row may have been updated concurrently.", payment.getId());
            }
        } catch (OptimisticLockingFailureException e) {
            log.warn("ObjectOptimisticLockingFailureException transitioning payment [{}] to PROCESSING: {}",
                    payment.getId(), e.getMessage());
        }
    }
}

