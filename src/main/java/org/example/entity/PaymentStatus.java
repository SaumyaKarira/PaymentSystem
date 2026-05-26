package org.example.entity;

/**
 * PaymentStatus — lifecycle state machine for a payment record.
 *
 * <p>Valid state transitions:
 * <pre>
 *   INITIATED ──(sync provider call succeeds)──────────► SUCCESS
 *   INITIATED ──(sync provider call fails)─────────────► PROCESSING
 *   PROCESSING ─(Kafka retry succeeds)──────────────────► SUCCESS
 *   PROCESSING ─(all Kafka retries exhausted → DLT)─────► FAILED
 * </pre>
 *
 * <p>The {@code version} field on {@link Payment} combined with Hibernate's
 * {@code @Version} ensures that only one thread/consumer can advance the state at a time.
 * Any concurrent update collides on the version number and throws
 * {@code ObjectOptimisticLockingFailureException}.
 */
public enum PaymentStatus {
    /**
     * The payment record has just been persisted to MySQL.
     * The synchronous provider call is about to be attempted.
     */
    INITIATED,

    /**
     * The synchronous provider call failed (5xx / timeout).
     * The payment payload has been pushed to {@code payment-main-topic}.
     * The Kafka retry consumer is responsible for driving it to SUCCESS or FAILED.
     */
    PROCESSING,

    /**
     * A provider returned a successful response.
     * The {@code providerReferenceId} field contains the provider's transaction ID.
     */
    SUCCESS,

    /**
     * All retry attempts (including Kafka non-blocking retries) were exhausted.
     * The DLT handler set this status. No further automated recovery will occur.
     */
    FAILED
}

