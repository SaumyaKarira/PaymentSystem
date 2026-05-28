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

// Kafka consumer for non-blocking retry processing of failed payments via @RetryableTopic.
// Retry topic layout (attempts=4): main-topic → retry-0 (2s) → retry-1 (4s) → retry-2 (8s) → dlt
// Uses Kafka's deliveryAttempt header as the authoritative retry counter (not the DB column).
@Slf4j
@Component
@RequiredArgsConstructor
public class PaymentRetryConsumer {

    private final PaymentRepository paymentRepository;
    private final RoutingEngine routingEngine;

    // Must stay in sync with the attempts value in @RetryableTopic below
    private static final int TOTAL_CONFIGURED_ATTEMPTS = 4;

    // @RetryableTopic: 1 initial + 3 retries, exponential backoff 2s → 4s → 8s
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
            @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
            // deliveryAttempt: 1-based counter injected by @RetryableTopic (absent on first delivery → default 1)
            @Header(name = KafkaHeaders.DELIVERY_ATTEMPT, required = false) Integer deliveryAttempt
    ) {
        PaymentEvent event = record.value();

        // Use deliveryAttempt as the authoritative retry count — always accurate even if prior DB updates failed
        final int deliveryAttemptValue = (deliveryAttempt != null) ? deliveryAttempt : 1;

        log.info("Kafka consumer: processing payment [{}] from topic [{}], deliveryAttempt={}",
                event.paymentId(), topic, deliveryAttemptValue);

        // Always re-fetch from DB to get the latest version number for the optimistic lock
        Payment payment = paymentRepository.findById(event.paymentId()).orElse(null);
        if (payment == null) {
            // Payment deleted between publish and delivery — skip to avoid retrying a non-existent record
            log.error("Payment [{}] not found in DB during retry processing (attempt={}). "
                    + "Skipping — payment may have been deleted.", event.paymentId(), deliveryAttemptValue);
            return;
        }

        // Skip if a concurrent update already resolved the payment to a terminal state
        if (payment.getStatus() == PaymentStatus.SUCCESS
                || payment.getStatus() == PaymentStatus.FAILED) {
            log.info("Payment [{}] already in terminal state [{}] at deliveryAttempt={}. Skipping.",
                    event.paymentId(), payment.getStatus(), deliveryAttemptValue);
            return;
        }

        // Route to primary provider: CARD → ProviderA, UPI → ProviderB
        PaymentProviderConnector connector = routingEngine.route(event.paymentMethod());

        log.info("Payment [{}] deliveryAttempt={}: routing to provider [{}]",
                event.paymentId(), deliveryAttemptValue, connector.getProviderId());

        try {
            String providerRefId = connector.processPayment(
                    payment.getId(),
                    payment.getAmount(),
                    payment.getCurrency()
            );

            // Success — version-guarded UPDATE to SUCCESS
            int rowsUpdated = paymentRepository.updateOnSuccess(
                    payment.getId(),
                    PaymentStatus.SUCCESS,
                    connector.getProviderId(),
                    providerRefId,
                    deliveryAttemptValue,   // authoritative Kafka delivery count
                    payment.getVersion()
            );

            if (rowsUpdated == 0) {
                log.warn("Optimistic lock conflict on SUCCESS update for payment [{}] "
                        + "(deliveryAttempt={}, version={}). Concurrent update detected; skipping.",
                        payment.getId(), deliveryAttemptValue, payment.getVersion());
            } else {
                log.info("Payment [{}] → SUCCESS on deliveryAttempt={} via provider [{}]. Ref: {}",
                        payment.getId(), deliveryAttemptValue, connector.getProviderId(), providerRefId);
            }

        } catch (ProviderException providerEx) {
            // Provider failed — persist attempt count for observability, then re-throw so @RetryableTopic
            // forwards the message to the next retry topic (or DLT after the last attempt)
            log.warn("Payment [{}] FAILED on deliveryAttempt={} via provider [{}]: {}. "
                    + "Persisting retryCount={} before forwarding to next retry topic.",
                    payment.getId(), deliveryAttemptValue, providerEx.getProviderName(),
                    providerEx.getMessage(), deliveryAttemptValue);

            try {
                int rowsUpdated = paymentRepository.updateStatusWithVersionCheck(
                        payment.getId(),
                        PaymentStatus.PROCESSING,
                        deliveryAttemptValue,   // authoritative Kafka delivery count
                        payment.getVersion()
                );
                if (rowsUpdated == 0) {
                    log.warn("Optimistic lock conflict persisting retryCount={} for payment [{}] "
                            + "(version={}). Next delivery will self-correct via deliveryAttempt header.",
                            deliveryAttemptValue, payment.getId(), payment.getVersion());
                } else {
                    log.debug("Payment [{}] retryCount={} persisted to DB after failed attempt.",
                            payment.getId(), deliveryAttemptValue);
                }
            } catch (OptimisticLockingFailureException lockEx) {
                // Hibernate-level version conflict — tolerable, log and continue
                log.warn("OptimisticLockingFailureException persisting retryCount for payment [{}]: {}",
                        payment.getId(), lockEx.getMessage());
            }

            throw providerEx;
        }
    }

    // DLT handler — invoked after all retry attempts are exhausted.
    // Marks the payment as FAILED with the final attempt count.
    @DltHandler
    @Transactional
    public void handleDlt(
            ConsumerRecord<String, PaymentEvent> record,
            @Header(name = "kafka_dlt-exception-stacktrace", required = false) byte[] exceptionHeader,
            @Header(name = KafkaHeaders.DELIVERY_ATTEMPT, required = false) Integer deliveryAttempt
    ) {
        PaymentEvent event = record.value();

        // Use header value if present; fall back to configured total to ensure DB always reflects true count
        final int finalRetryCount = (deliveryAttempt != null)
                ? deliveryAttempt
                : TOTAL_CONFIGURED_ATTEMPTS;

        String stackTrace = (exceptionHeader != null)
                ? new String(exceptionHeader, StandardCharsets.UTF_8)
                : "No stack trace header available";

        log.error("DLT HANDLER: Payment [{}] exhausted all {} delivery attempts. "
                + "Marking as FAILED with finalRetryCount={}.\nFinal exception:\n{}",
                event.paymentId(), finalRetryCount, finalRetryCount, stackTrace);

        Payment payment = paymentRepository.findById(event.paymentId()).orElse(null);
        if (payment == null) {
            log.error("DLT HANDLER: Payment [{}] not found in DB. Cannot mark as FAILED.", event.paymentId());
            return;
        }

        // Skip if a concurrent update already resolved the payment (e.g., a late synchronous SUCCESS)
        if (payment.getStatus() == PaymentStatus.SUCCESS
                || payment.getStatus() == PaymentStatus.FAILED) {
            log.warn("DLT HANDLER: Payment [{}] already in terminal state [{}]. Skipping FAILED update.",
                    event.paymentId(), payment.getStatus());
            return;
        }

        try {
            int rowsUpdated = paymentRepository.updateStatusWithVersionCheck(
                    payment.getId(),
                    PaymentStatus.FAILED,
                    finalRetryCount,
                    payment.getVersion()
            );

            if (rowsUpdated > 0) {
                log.error("DLT HANDLER: Payment [{}] → FAILED. retryCount={} persisted to DB.",
                        event.paymentId(), finalRetryCount);
            } else {
                log.warn("DLT HANDLER: Optimistic lock conflict marking payment [{}] as FAILED (version={}). "
                        + "Row was concurrently updated — skipping.", event.paymentId(), payment.getVersion());
            }
        } catch (OptimisticLockingFailureException e) {
            // Do NOT re-throw — re-throwing would trigger an infinite DLT retry loop
            log.error("DLT HANDLER: OptimisticLockingFailureException marking payment [{}] as FAILED: {}",
                    event.paymentId(), e.getMessage());
        }
    }
}
