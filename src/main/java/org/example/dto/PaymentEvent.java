package org.example.dto;

import org.example.entity.PaymentMethod;

import java.math.BigDecimal;

/**
 * PaymentEvent — the Kafka message payload published to {@code payment-main-topic}.
 *
 * <p>This record is serialized to JSON by the Kafka producer (using Spring Kafka's
 * {@code JsonSerializer}) and deserialized back by the {@code PaymentRetryConsumer}
 * (using {@code JsonDeserializer}).
 *
 * <p>Design decision: we publish a lightweight event containing only the fields needed
 * for retry processing, rather than the full {@link org.example.entity.Payment} entity.
 * This decouples the Kafka message contract from the JPA entity schema.
 *
 * <p>Note: The optimistic lock {@code version} is intentionally NOT included here.
 * The consumer always re-fetches the payment entity from the database to obtain the
 * latest version number before any update. Using a stale version from the event
 * would cause unnecessary optimistic lock conflicts when a prior retry already
 * incremented the version.
 *
 * @param paymentId     the UUID of the payment to retry
 * @param amount        the original payment amount (for logging and audit)
 * @param currency      the original currency
 * @param paymentMethod CARD or UPI — used by the routing engine to select the provider
 * @param retryCount    the retry attempt count at the time of publishing (starts at 0)
 */
public record PaymentEvent(
        String paymentId,
        BigDecimal amount,
        String currency,
        PaymentMethod paymentMethod,
        int retryCount
) {}

