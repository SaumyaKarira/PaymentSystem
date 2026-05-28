package org.example.kafka;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.example.dto.PaymentEvent;
import org.example.entity.Payment;
import org.example.entity.PaymentStatus;
import org.example.exception.ProviderException;
import org.example.provider.PaymentProviderConnector;
import org.example.repository.PaymentRepository;
import org.example.routing.RoutingEngine;
import org.springframework.dao.OptimisticLockingFailureException;
import org.springframework.kafka.annotation.DltHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.RetryableTopic;
import org.springframework.kafka.retrytopic.TopicSuffixingStrategy;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.retry.annotation.Backoff;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.nio.charset.StandardCharsets;

/**
 * PaymentRetryConsumer — Kafka consumer responsible for non-blocking retry processing
 * of failed payments via Spring Kafka's {@code @RetryableTopic} infrastructure.
 *
 * <h2>How @RetryableTopic Works (Non-Blocking Retry)</h2>
 * <p>When {@code processPayment()} throws an exception, Spring Kafka intercepts it and:
 * <ol>
 *   <li>Publishes the message to the next retry topic with a {@code kafka_backoff_timestamp}
 *       header set to now + backoff delay.</li>
 *   <li>Acknowledges the offset on the current topic immediately.</li>
 *   <li>The retry consumer pauses its partition until the backoff timestamp is reached,
 *       then resumes — no Thread.sleep, no blocked consumer threads.</li>
 *   <li>After all attempts are exhausted the message is forwarded to the DLT.</li>
 * </ol>
 *
 * <h2>Retry Topic Layout (attempts=4)</h2>
 * <pre>
 *   payment-main-topic               ← attempt 1  (deliveryAttempt header = 1)
 *   payment-main-topic-retry-0       ← attempt 2  (deliveryAttempt header = 2, after 2s)
 *   payment-main-topic-retry-1       ← attempt 3  (deliveryAttempt header = 3, after 4s)
 *   payment-main-topic-retry-2       ← attempt 4  (deliveryAttempt header = 4, after 8s)
 *   payment-main-topic-dlt           ← all attempts exhausted
 * </pre>
 *
 * <p>All retry attempts use the same primary provider determined by the payment method:
 * CARD → ProviderA, UPI → ProviderB. There is no fallback/failover to an alternate provider.
 *
 * <h2>retryCount Tracking — The Bug That Was Fixed</h2>
 * <p>Previously {@code retryCount} was calculated as {@code payment.getRetryCount() + 1},
 * reading the value from the database. This was unreliable because:
 * <ul>
 *   <li>The previous attempt's DB update may have returned {@code rowsUpdated=0} due to
 *       an optimistic lock conflict, leaving the DB value stale (still 0).</li>
 *   <li>The DB value represents "what was persisted last time", not "how many Kafka
 *       deliveries have actually occurred".</li>
 * </ul>
 *
 * <h2>The Fix: Use KafkaHeaders.DELIVERY_ATTEMPT as the authoritative counter</h2>
 * <p>Spring Kafka's {@code @RetryableTopic} infrastructure injects a
 * {@code kafka_deliveryAttempt} header into every message. This is a monotonically
 * increasing integer starting at 1 (first delivery = 1, first retry = 2, etc.) and
 * is maintained by Spring Kafka's internal state — it is ALWAYS accurate regardless
 * of what happened to the DB in previous attempts.
 *
 * <p>The mapping from header to DB column:
 * <pre>
 *   deliveryAttempt = 1  → first delivery (original topic) → fails → retryCount stored = 1
 *   deliveryAttempt = 2  → first retry   (retry-0)         → fails → retryCount stored = 2
 *   deliveryAttempt = 3  → second retry  (retry-1)         → fails → retryCount stored = 3
 *   deliveryAttempt = 4  → third retry   (retry-2)         → fails → DLT fires
 *   DLT handler                                                     → retryCount stored = totalAttempts (4)
 * </pre>
 *
 * <p>On SUCCESS, retryCount = deliveryAttempt (the attempt that finally succeeded).
 * On FAILURE of each attempt, retryCount = deliveryAttempt (attempts completed so far).
 * On DLT, retryCount = totalConfiguredAttempts (all attempts were exhausted).
 *
 * <h2>Concurrency Safety</h2>
 * <p>All DB updates use version-guarded JPQL ({@code WHERE version = :version}).
 * If {@code rowsUpdated == 0}, another thread already changed this row (optimistic lock
 * conflict). We log the conflict and do NOT re-throw — the concurrent update wins and
 * the Kafka offset is committed normally.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class PaymentRetryConsumer {

    private final PaymentRepository paymentRepository;
    private final RoutingEngine routingEngine;

    /**
     * Total number of Kafka delivery attempts configured via {@code @RetryableTopic(attempts="4")}.
     * Used by the DLT handler to record the final retry count when all attempts are exhausted.
     *
     * <p>Keep this in sync with the {@code attempts} value in {@code @RetryableTopic} below.
     * If you change {@code attempts="4"} to a different value, update this constant too.
     */
    private static final int TOTAL_CONFIGURED_ATTEMPTS = 4;

    /**
     * Primary Kafka listener with non-blocking retry configuration.
     *
     * <p>Annotation breakdown:
     * <ul>
     *   <li>{@code attempts=4}: 1 initial attempt + 3 retry attempts.</li>
     *   <li>{@code backoff}: Exponential backoff — 2s → 4s → 8s between attempts.</li>
     *   <li>{@code autoCreateTopics=true}: Spring Kafka auto-creates retry and DLT topics.</li>
     *   <li>{@code topicSuffixingStrategy=SUFFIX_WITH_INDEX_VALUE}: topics named
     *       {@code payment-main-topic-retry-0}, {@code ...-retry-1}, {@code ...-retry-2}.</li>
     * </ul>
     */
    @RetryableTopic(
            attempts = "4",
            backoff = @Backoff(delay = 2000, multiplier = 2.0),
            autoCreateTopics = "true",
            topicSuffixingStrategy = TopicSuffixingStrategy.SUFFIX_WITH_INDEX_VALUE,
            kafkaTemplate = "kafkaTemplate"
    )
    @KafkaListener(
            topics = "${payment.kafka.main-topic:payment-main-topic}",
            groupId = "payment-retry-group",
            containerFactory = "kafkaListenerContainerFactory"
    )
    @Transactional
    public void processPayment(
            ConsumerRecord<String, PaymentEvent> record,

            // ── KAFKA HEADER: RECEIVED_TOPIC ─────────────────────────────────────
            // The topic this specific delivery came from. Used for logging only.
            // Value: "payment-main-topic", "payment-main-topic-retry-0", etc.
            @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,

            // ── KAFKA HEADER: DELIVERY_ATTEMPT ────────────────────────────────────
            // Spring Kafka's @RetryableTopic infrastructure injects this header
            // automatically. It is a 1-based monotonically increasing integer:
            //   - First delivery on payment-main-topic          → deliveryAttempt = 1
            //   - First retry   on payment-main-topic-retry-0   → deliveryAttempt = 2
            //   - Second retry  on payment-main-topic-retry-1   → deliveryAttempt = 3
            //   - Third retry   on payment-main-topic-retry-2   → deliveryAttempt = 4
            //
            // required=false: the header is absent on the very first delivery on the
            // main topic; in that case we default to 1.
            @Header(name = KafkaHeaders.DELIVERY_ATTEMPT, required = false) Integer deliveryAttempt
    ) {
        PaymentEvent event = record.value();

        // ── DELIVERY_ATTEMPT → retryCount MAPPING ────────────────────────────────
        //
        // deliveryAttempt is 1-based. We use it directly as retryCount in MySQL:
        //   attempt 1 → retryCount = 1  (1 Kafka delivery has occurred)
        //   attempt 2 → retryCount = 2  (2 Kafka deliveries have occurred)
        //   attempt 3 → retryCount = 3
        //   attempt 4 → retryCount = 4
        //
        // This is the AUTHORITATIVE counter. It does NOT depend on what the DB holds —
        // it is maintained by Spring Kafka's internal retry state machine and is always
        // accurate even if previous DB updates failed due to optimistic lock conflicts.
        //
        // NOTE: we do NOT use payment.getRetryCount() + 1 here. That DB value could be
        // stale (stuck at 0 or any previous value) if a prior attempt's version-guarded
        // UPDATE returned rowsUpdated=0. Using deliveryAttempt eliminates that staleness.
        final int deliveryAttemptValue = (deliveryAttempt != null) ? deliveryAttempt : 1;

        log.info("Kafka consumer: processing payment [{}] from topic [{}], deliveryAttempt={}",
                event.paymentId(), topic, deliveryAttemptValue);

        // ── FETCH CURRENT PAYMENT STATE FROM MYSQL ──────────────��────────────────
        // Always re-fetch from DB to get the latest version number for the optimistic
        // lock guard. We cannot use the version embedded in the PaymentEvent because
        // the entity may have been updated by a prior retry attempt since the event
        // was first published.
        Payment payment = paymentRepository.findById(event.paymentId()).orElse(null);
        if (payment == null) {
            // Payment deleted between event publication and this delivery — skip cleanly.
            // Do NOT throw: throwing would trigger another retry cycle for a non-existent payment.
            log.error("Payment [{}] not found in DB during retry processing (attempt={}). "
                    + "Skipping — payment may have been deleted.", event.paymentId(), deliveryAttemptValue);
            return;
        }

        // ── SKIP IF ALREADY IN A TERMINAL STATE ──────────────────────────────────
        // Handles the race condition where a concurrent HTTP request or an earlier Kafka
        // retry already resolved the payment to SUCCESS or FAILED between the time this
        // message was published and now. No work needed — commit the offset and move on.
        if (payment.getStatus() == PaymentStatus.SUCCESS
                || payment.getStatus() == PaymentStatus.FAILED) {
            log.info("Payment [{}] already in terminal state [{}] at deliveryAttempt={}. Skipping.",
                    event.paymentId(), payment.getStatus(), deliveryAttemptValue);
            return;
        }

        // ── PROVIDER ROUTING ──────────────────────────────────────────────────────
        // Always route to the primary provider for the given payment method.
        //   CARD → ProviderA
        //   UPI  → ProviderB
        PaymentProviderConnector connector = routingEngine.route(event.paymentMethod());

        log.info("Payment [{}] deliveryAttempt={}: routing to provider [{}]",
                event.paymentId(), deliveryAttemptValue, connector.getProviderId());

        // ── ATTEMPT PROVIDER CALL ─────────────────────────────────────────────────
        try {
            String providerRefId = connector.processPayment(
                    payment.getId(),
                    payment.getAmount(),
                    payment.getCurrency()
            );

            // ── SUCCESS PATH ──────────────────────────────────────────────────────
            // retryCount = deliveryAttemptValue: records that this delivery attempt was
            // the one that finally succeeded. This is the authoritative Kafka-based count.
            //
            // Version-guarded UPDATE (WHERE version = :version):
            //   - If rowsUpdated=1: success, DB is now consistent.
            //   - If rowsUpdated=0: optimistic lock conflict — another thread updated
            //     this row concurrently. The other update wins; we log and move on.
            //     The Kafka offset is still committed (no re-throw).
            int rowsUpdated = paymentRepository.updateOnSuccess(
                    payment.getId(),
                    PaymentStatus.SUCCESS,
                    connector.getProviderId(),
                    providerRefId,
                    deliveryAttemptValue,   // ← AUTHORITATIVE: Kafka delivery count, not DB value
                    payment.getVersion()    // ← optimistic lock guard from freshly fetched entity
            );

            if (rowsUpdated == 0) {
                log.warn("Optimistic lock conflict on SUCCESS update for payment [{}] "
                        + "(deliveryAttempt={}, version={}). Concurrent update detected; "
                        + "skipping — the concurrent update takes precedence.",
                        payment.getId(), deliveryAttemptValue, payment.getVersion());
            } else {
                log.info("Payment [{}] → SUCCESS on deliveryAttempt={} via provider [{}]. "
                        + "retryCount={} persisted to DB. Ref: {}",
                        payment.getId(), deliveryAttemptValue, connector.getProviderId(),
                        deliveryAttemptValue, providerRefId);
            }

            // Returning normally commits the Kafka offset — @RetryableTopic does NOT retry.

        } catch (ProviderException providerEx) {
            // ── FAILURE PATH ──────────────────────────────────────────────────────
            // Provider call failed. We persist the current attempt count to the DB
            // for observability (so operators can see progress in MySQL), then re-throw
            // to let @RetryableTopic forward the message to the next retry topic.
            //
            // retryCount = deliveryAttemptValue:
            //   Records "this many Kafka deliveries have been attempted so far".
            //   Using deliveryAttemptValue is safe even if the previous DB update
            //   was lost (rowsUpdated=0), because this value comes from Kafka headers,
            //   not from the potentially-stale DB column.
            log.warn("Payment [{}] FAILED on deliveryAttempt={} via provider [{}]: {}. "
                    + "Persisting retryCount={} to DB before forwarding to next retry topic.",
                    payment.getId(), deliveryAttemptValue, providerEx.getProviderName(),
                    providerEx.getMessage(), deliveryAttemptValue);

            // Version-guarded update: persist attempt count, keep status=PROCESSING.
            // If rowsUpdated=0 (version conflict), the count update is lost for this
            // attempt — tolerable because deliveryAttemptValue on the NEXT delivery will
            // still be correct (Kafka maintains this independently of DB state).
            try {
                int rowsUpdated = paymentRepository.updateStatusWithVersionCheck(
                        payment.getId(),
                        PaymentStatus.PROCESSING,
                        deliveryAttemptValue,   // ← AUTHORITATIVE: Kafka delivery count
                        payment.getVersion()    // ← optimistic lock guard
                );
                if (rowsUpdated == 0) {
                    log.warn("Optimistic lock conflict persisting retryCount={} for payment [{}] "
                            + "(version={}). DB retryCount may lag by one attempt — "
                            + "next delivery will self-correct via deliveryAttempt header.",
                            deliveryAttemptValue, payment.getId(), payment.getVersion());
                } else {
                    log.debug("Payment [{}] retryCount={} persisted to DB after failed attempt.",
                            payment.getId(), deliveryAttemptValue);
                }
            } catch (OptimisticLockingFailureException lockEx) {
                // Hibernate-level optimistic lock collision (thrown before UPDATE executes).
                // Same semantics as rowsUpdated=0 — tolerable, log and continue.
                log.warn("OptimisticLockingFailureException persisting retryCount for payment [{}]: {}",
                        payment.getId(), lockEx.getMessage());
            }

            // Re-throw to signal @RetryableTopic: this delivery failed, forward to retry-N+1.
            // If this was the last attempt (deliveryAttempt=4), @RetryableTopic forwards to DLT.
            throw providerEx;
        }
    }

    /**
     * Dead Letter Topic (DLT) handler — invoked when ALL retry attempts are exhausted.
     *
     * <p>Spring Kafka calls this method after the message has been forwarded to
     * {@code payment-main-topic-dlt}, meaning all {@code TOTAL_CONFIGURED_ATTEMPTS}
     * Kafka deliveries have failed.
     *
     * <h2>retryCount in the DLT handler</h2>
     * <p>{@code deliveryAttempt} from the Kafka header is the authoritative final count.
     * We fall back to {@code TOTAL_CONFIGURED_ATTEMPTS} when the header is absent (e.g.,
     * in direct unit-test invocations where Spring header resolution is bypassed).
     * This ensures the DB always reflects the true exhausted attempt count regardless of
     * whether any intermediate {@code processPayment()} DB updates were lost to version
     * conflicts.
     *
     * <h2>Exception Header</h2>
     * <p>Spring Kafka attaches a {@code kafka_dlt-exception-stacktrace} header to every
     * DLT message. We extract and log it here for post-mortem debugging.
     *
     * <h2>Unit-test invocation</h2>
     * <p>Tests that call this method directly on a manually-constructed instance should
     * pass {@code null} for both {@code exceptionHeader} and {@code deliveryAttempt}.
     * The {@code null} delivery attempt safely falls back to {@code TOTAL_CONFIGURED_ATTEMPTS}.
     *
     * @param record              the Kafka record that landed on the DLT
     * @param exceptionHeader     the exception stacktrace bytes (may be null)
     * @param deliveryAttempt     the final delivery attempt count from the Kafka header (may be null)
     */
    @DltHandler
    @Transactional
    public void handleDlt(
            ConsumerRecord<String, PaymentEvent> record,
            @Header(name = "kafka_dlt-exception-stacktrace", required = false) byte[] exceptionHeader,
            @Header(name = KafkaHeaders.DELIVERY_ATTEMPT, required = false) Integer deliveryAttempt
    ) {
        PaymentEvent event = record.value();

        // ── DELIVERY_ATTEMPT → finalRetryCount MAPPING ───────────────────────────
        //
        // Use the Kafka header value if present; fall back to TOTAL_CONFIGURED_ATTEMPTS.
        // This guarantees that retryCount in the DB always equals the actual number of
        // Kafka deliveries attempted, even if some intermediate DB updates were lost.
        final int finalRetryCount = (deliveryAttempt != null)
                ? deliveryAttempt
                : TOTAL_CONFIGURED_ATTEMPTS;

        // Extract the final exception stack trace for post-mortem observability.
        String stackTrace = (exceptionHeader != null)
                ? new String(exceptionHeader, StandardCharsets.UTF_8)
                : "No stack trace header available";

        log.error("DLT HANDLER: Payment [{}] has exhausted all {} Kafka delivery attempts. "
                + "Marking as FAILED with finalRetryCount={}.\nFinal exception:\n{}",
                event.paymentId(), finalRetryCount, finalRetryCount, stackTrace);

        // ── FETCH CURRENT PAYMENT STATE ────────────────────────────────────────��──
        Payment payment = paymentRepository.findById(event.paymentId()).orElse(null);
        if (payment == null) {
            log.error("DLT HANDLER: Payment [{}] not found in DB. Cannot mark as FAILED.",
                    event.paymentId());
            return;
        }

        // ── SKIP IF ALREADY IN TERMINAL STATE ────────────────────────────────────
        // Guard against the race where a concurrent synchronous SUCCESS resolved the
        // payment between the last Kafka retry and this DLT delivery.
        if (payment.getStatus() == PaymentStatus.SUCCESS
                || payment.getStatus() == PaymentStatus.FAILED) {
            log.warn("DLT HANDLER: Payment [{}] already in terminal state [{}]. "
                    + "Skipping FAILED update (concurrent update took precedence).",
                    event.paymentId(), payment.getStatus());
            return;
        }

        // ── MARK AS FAILED ────────────────────────────────────────────────────────
        // finalRetryCount = authoritative Kafka-based attempt count (not DB-sourced).
        // Version-guarded UPDATE ensures we never overwrite a concurrent SUCCESS.
        //   rowsUpdated=1 → DB now reflects FAILED with correct retryCount.
        //   rowsUpdated=0 → another thread updated the row; their update takes precedence.
        try {
            int rowsUpdated = paymentRepository.updateStatusWithVersionCheck(
                    payment.getId(),
                    PaymentStatus.FAILED,
                    finalRetryCount,        // ← AUTHORITATIVE: Kafka delivery count
                    payment.getVersion()    // ← optimistic lock guard from fresh fetch
            );

            if (rowsUpdated > 0) {
                log.error("DLT HANDLER: Payment [{}] → FAILED. retryCount={} persisted to DB. "
                        + "All {} Kafka delivery attempts were exhausted.",
                        event.paymentId(), finalRetryCount, finalRetryCount);
            } else {
                log.warn("DLT HANDLER: Optimistic lock conflict marking payment [{}] as FAILED "
                        + "(version={}). Row was concurrently updated — skipping.",
                        event.paymentId(), payment.getVersion());
            }
        } catch (OptimisticLockingFailureException e) {
            // Hibernate-level optimistic lock collision in the DLT handler.
            // Do NOT re-throw — re-throwing would cause Spring Kafka to re-deliver
            // the DLT message, triggering an infinite DLT retry loop.
            log.error("DLT HANDLER: OptimisticLockingFailureException marking payment [{}] as FAILED: {}",
                    event.paymentId(), e.getMessage());
        }
    }
}

