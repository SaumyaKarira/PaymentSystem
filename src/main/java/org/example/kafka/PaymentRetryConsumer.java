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
 * PaymentRetryConsumer — the Kafka consumer responsible for non-blocking retry processing
 * of failed payments.
 *
 * <h2>How @RetryableTopic Works (Spring Kafka Non-Blocking Retry)</h2>
 * <p>When {@code processPayment()} throws an exception, Spring Kafka's
 * {@code RetryTopicConfigurationSupport} intercepts the exception and:
 * <ol>
 *   <li>Publishes the original message to the next retry topic
 *       ({@code payment-main-topic-retry-0}, {@code ...-retry-1}, etc.)
 *       with a {@code kafka_backoff_timestamp} header set to now + backoff delay.</li>
 *   <li>Acknowledges the offset on the current topic immediately (the message is not
 *       "re-queued" on the same topic; it moves to the next retry topic).</li>
 *   <li>The retry topic consumer pauses the relevant partition until the backoff
 *       timestamp is reached, then resumes processing — this is what makes it
 *       "non-blocking" (no Thread.sleep, no blocked consumer threads).</li>
 *   <li>After all retry attempts are exhausted, the message is forwarded to the
 *       Dead Letter Topic (DLT): {@code payment-main-topic-dlt}.</li>
 * </ol>
 *
 * <h2>Generated Retry Topic Names</h2>
 * <p>With {@code attempts=4} (1 original + 3 retries) and the default suffix strategy:
 * <pre>
 *   payment-main-topic               ← original topic (attempt 1)
 *   payment-main-topic-retry-0       ← retry attempt 2 (after 2s)
 *   payment-main-topic-retry-1       ← retry attempt 3 (after 4s)
 *   payment-main-topic-retry-2       ← retry attempt 4 (after 8s)
 *   payment-main-topic-dlt           ← dead letter topic
 * </pre>
 *
 * <h2>Failover Strategy</h2>
 * <p>On the first retry attempt, {@code retryCount >= 1}, we switch to the alternate
 * provider (failover=true in RoutingEngine).  This means:
 * <ul>
 *   <li>Attempt 1 (original):   CARD → Provider A</li>
 *   <li>Attempt 2 (retry-0):    CARD → Provider B (failover)</li>
 *   <li>Attempt 3 (retry-1):    CARD → Provider B (failover, still)</li>
 *   <li>Attempt 4 (retry-2):    CARD → Provider B (failover, still)</li>
 *   <li>DLT:                    All attempts exhausted → mark FAILED</li>
 * </ul>
 *
 * <h2>Concurrency Safety with Optimistic Locking</h2>
 * <p>Between retry attempts, a human operator or another service might manually update
 * the payment status (e.g., to SUCCESS via a back-office tool).  The version-guarded
 * DB update ({@code WHERE version = :version}) ensures the Kafka consumer never
 * overwrites a more recent state.  If a version conflict is detected, the consumer
 * logs the conflict and does NOT re-throw — the offset is committed and the message
 * is considered "handled" (the concurrent update wins).
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class PaymentRetryConsumer {

    private final PaymentRepository paymentRepository;
    private final RoutingEngine routingEngine;

    /**
     * Primary Kafka listener with non-blocking retry configuration.
     *
     * <p>Annotation breakdown:
     * <ul>
     *   <li>{@code attempts=4}: 1 initial attempt + 3 retry attempts.  Spring Kafka
     *       generates 3 retry topics ({@code retry-0}, {@code retry-1}, {@code retry-2}).</li>
     *   <li>{@code backoff}: Exponential backoff starting at 2000ms with multiplier 2.0:
     *       2s → 4s → 8s between attempts.</li>
     *   <li>{@code autoCreateTopics=true}: Spring Kafka auto-creates the retry and DLT topics
     *       on the local Kafka broker if they don't exist.  For production, create topics
     *       explicitly with the correct replication factor.</li>
     *   <li>{@code topicSuffixingStrategy=SUFFIX_WITH_INDEX_VALUE}: topics are named
     *       {@code payment-main-topic-retry-0}, {@code ...-retry-1}, {@code ...-retry-2}.</li>
     * </ul>
     *
     * <p>{@code @KafkaListener}: Subscribes this method to {@code payment-main-topic}.
     * The {@code containerFactory} bean is the one we configured in {@link org.example.config.KafkaConsumerConfig}.
     */
    @RetryableTopic(
            // Total attempts including the original = 4 (so 3 retries after the first failure)
            attempts = "4",
            backoff = @Backoff(
                    // Initial delay before the first retry: 2 seconds
                    delay = 2000,
                    // Multiplier: each subsequent retry doubles the wait time.
                    // retry-0 waits 2s, retry-1 waits 4s, retry-2 waits 8s
                    multiplier = 2.0
            ),
            // Auto-create retry and DLT topics on the local Kafka broker.
            // Topics created: payment-main-topic-retry-0, -retry-1, -retry-2, -dlt
            autoCreateTopics = "true",
            // Use the numeric index in the topic suffix (retry-0, retry-1, retry-2).
            topicSuffixingStrategy = TopicSuffixingStrategy.SUFFIX_WITH_INDEX_VALUE,
            // Use the containerFactory bean from KafkaConsumerConfig
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
            @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
            @Header(name = KafkaHeaders.DELIVERY_ATTEMPT, required = false) Integer deliveryAttempt
    ) {
        PaymentEvent event = record.value();
        int attempt = deliveryAttempt != null ? deliveryAttempt : 1;

        log.info("Kafka retry consumer: processing payment [{}] from topic [{}], attempt #{}",
                event.paymentId(), topic, attempt);

        // ── FETCH CURRENT PAYMENT STATE FROM MYSQL ────────────────────────────
        // Re-fetch the payment from DB to get the latest version number.
        // We cannot rely on the version embedded in the PaymentEvent because the
        // payment may have been updated between when the event was published and now.
        Payment payment = paymentRepository.findById(event.paymentId()).orElse(null);
        if (payment == null) {
            // Payment not found — this should not happen in normal operation.
            // Could occur if a payment was deleted (e.g., admin cleanup).
            // Log and do NOT throw — throwing would cause another retry cycle.
            log.error("Payment [{}] not found in DB during retry processing. Skipping.", event.paymentId());
            return;
        }

        // ── SKIP IF ALREADY COMPLETED ─────────────────────────────────────────
        // If the payment is already SUCCESS or FAILED, skip processing.
        // This handles the case where a concurrent HTTP request or earlier retry
        // already resolved the payment — no need to do duplicate work.
        if (payment.getStatus() == org.example.entity.PaymentStatus.SUCCESS
                || payment.getStatus() == org.example.entity.PaymentStatus.FAILED) {
            log.info("Payment [{}] already in terminal state [{}]. Skipping retry.",
                    event.paymentId(), payment.getStatus());
            return;
        }

        // ── FAILOVER LOGIC ────────────────────────────────────────────────────
        // On the first retry (attempt >= 2) and beyond, we switch to the alternate
        // provider.  The retryCount embedded in the event indicates how many times
        // the original orchestrator service attempted this payment (should be 0 on
        // first entry to Kafka).  We use the Kafka delivery attempt header for routing.
        //
        // failover=true → RoutingEngine selects the ALTERNATE provider:
        //   CARD: Provider A (primary) → Provider B (failover)
        //   UPI:  Provider B (primary) → Provider A (failover)
        boolean useFailover = attempt > 1;
        PaymentProviderConnector connector = routingEngine.route(event.paymentMethod(), useFailover);

        log.info("Payment [{}] retry attempt #{}: routing to provider [{}] (failover={})",
                event.paymentId(), attempt, connector.getProviderId(), useFailover);

        // Increment the retry count on the in-memory entity (will be persisted on success)
        int newRetryCount = payment.getRetryCount() + 1;

        // ── ATTEMPT PROVIDER CALL ─────────────────────────────────────────────
        try {
            String providerRefId = connector.processPayment(
                    payment.getId(),
                    payment.getAmount(),
                    payment.getCurrency()
            );

            // ── SUCCESS: UPDATE DB TO SUCCESS STATE ───────────────────────────
            // Use version-guarded update to prevent overwriting concurrent modifications.
            int rowsUpdated = paymentRepository.updateOnSuccess(
                    payment.getId(),
                    PaymentStatus.SUCCESS,
                    connector.getProviderId(),
                    providerRefId,
                    newRetryCount,
                    payment.getVersion()
            );

            if (rowsUpdated == 0) {
                // Optimistic lock conflict: another thread updated this row between our
                // SELECT (findById above) and this UPDATE.
                // This is safe to ignore — the other update is presumed correct.
                log.warn("Optimistic lock conflict during retry SUCCESS update for payment [{}]. "
                        + "Concurrent update detected; skipping this update.", payment.getId());
            } else {
                log.info("Payment [{}] succeeded on retry attempt #{} via provider [{}]. Ref: {}",
                        payment.getId(), attempt, connector.getProviderId(), providerRefId);
            }

            // Do NOT throw — returning normally commits the Kafka offset.

        } catch (ProviderException providerEx) {
            // ── PROVIDER FAILED: LET @RetryableTopic HANDLE RE-ROUTING ────────
            // By re-throwing the exception, we signal to Spring Kafka's retry
            // infrastructure that this message should be forwarded to the next
            // retry topic (or DLT if all attempts are exhausted).
            //
            // Spring Kafka will:
            //   1. NOT commit the offset on the current retry topic
            //   2. Publish the message to payment-main-topic-retry-N+1
            //   3. Set the kafka_backoff_timestamp header
            //
            // We update the retry count in DB here so the DB reflects the current attempt,
            // even though the payment remains in PROCESSING state.
            log.warn("Payment [{}] failed on retry attempt #{} via provider [{}]: {}",
                    payment.getId(), attempt, providerEx.getProviderName(), providerEx.getMessage());

            // Version-guarded update of retry count (payment remains in PROCESSING)
            try {
                int rowsUpdated = paymentRepository.updateStatusWithVersionCheck(
                        payment.getId(),
                        PaymentStatus.PROCESSING,
                        newRetryCount,
                        payment.getVersion()
                );
                if (rowsUpdated == 0) {
                    log.warn("Version conflict updating retry count for payment [{}]. Ignoring.", payment.getId());
                }
            } catch (OptimisticLockingFailureException lockEx) {
                // If the version-guarded update itself fails, log and continue.
                // The retry count discrepancy is tolerable — DLT will still fire correctly.
                log.warn("OptimisticLockingFailureException updating retry count for payment [{}]: {}",
                        payment.getId(), lockEx.getMessage());
            }

            // Re-throw to trigger @RetryableTopic forwarding to the next retry topic.
            // This is the critical path: throwing tells Spring Kafka the message was NOT
            // successfully processed and must be retried.
            throw providerEx;
        }
    }

    /**
     * Dead Letter Topic (DLT) handler — called when ALL retry attempts are exhausted.
     *
     * <p>This method is invoked by Spring Kafka when a message has been forwarded to
     * {@code payment-main-topic-dlt} after failing all retry attempts.
     *
     * <h2>Responsibilities</h2>
     * <ol>
     *   <li>Mark the payment as FAILED in MySQL</li>
     *   <li>Log the final exception stack trace embedded in the Kafka headers</li>
     *   <li>Optionally trigger alerting or incident creation (not implemented here)</li>
     * </ol>
     *
     * <h2>Exception Header</h2>
     * <p>Spring Kafka adds a {@code kafka_dlt-exception-stacktrace} header to DLT messages
     * containing the full stack trace of the last exception.  We extract and log this
     * header to aid post-mortem debugging without needing to reproduce the error.
     *
     * @param record            the Kafka record that landed on the DLT
     * @param exceptionHeader   the exception stack trace from the last failed attempt
     */
    @DltHandler
    @Transactional
    public void handleDlt(
            ConsumerRecord<String, PaymentEvent> record,
            @Header(name = "kafka_dlt-exception-stacktrace", required = false) byte[] exceptionHeader
    ) {
        PaymentEvent event = record.value();

        // Extract the final exception stack trace from the Kafka DLT header for logging.
        // This header is set automatically by Spring Kafka's retry infrastructure.
        String stackTrace = exceptionHeader != null
                ? new String(exceptionHeader, StandardCharsets.UTF_8)
                : "No stack trace header available";

        log.error("DLT HANDLER: Payment [{}] has exhausted all retry attempts. "
                        + "Marking as FAILED.\nFinal exception stack trace:\n{}",
                event.paymentId(), stackTrace);

        // Fetch the current state of the payment from MySQL
        Payment payment = paymentRepository.findById(event.paymentId()).orElse(null);
        if (payment == null) {
            log.error("DLT HANDLER: Payment [{}] not found in DB. Cannot mark as FAILED.", event.paymentId());
            return;
        }

        // Skip if already in a terminal state (e.g., SUCCESS from a concurrent thread)
        if (payment.getStatus() == PaymentStatus.SUCCESS || payment.getStatus() == PaymentStatus.FAILED) {
            log.warn("DLT HANDLER: Payment [{}] already in terminal state [{}]. Skipping FAILED update.",
                    event.paymentId(), payment.getStatus());
            return;
        }

        // ── MARK AS FAILED ────────────────────────────────────────────────────
        // Use a version-guarded update to prevent overwriting any concurrent SUCCESS.
        // If rowsUpdated == 0, another thread already advanced the state — no action needed.
        try {
            int rowsUpdated = paymentRepository.updateStatusWithVersionCheck(
                    payment.getId(),
                    PaymentStatus.FAILED,
                    payment.getRetryCount(),
                    payment.getVersion()
            );

            if (rowsUpdated > 0) {
                log.error("DLT HANDLER: Payment [{}] successfully marked as FAILED after {} retry attempts.",
                        event.paymentId(), payment.getRetryCount());
            } else {
                // rowsUpdated == 0 → version mismatch.
                // A concurrent thread (possibly the sync API) updated the row.
                // The concurrent update takes precedence — do not overwrite.
                log.warn("DLT HANDLER: Version conflict marking payment [{}] as FAILED. "
                        + "Row may have been concurrently updated to a different state.", event.paymentId());
            }
        } catch (OptimisticLockingFailureException e) {
            // Hibernate-level optimistic lock collision in the DLT handler.
            // Same semantics as above — log and do not re-throw (we don't want DLT re-processing).
            log.error("DLT HANDLER: OptimisticLockingFailureException marking payment [{}] as FAILED: {}",
                    event.paymentId(), e.getMessage());
        }
    }
}

