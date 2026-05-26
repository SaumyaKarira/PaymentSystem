package org.example.entity;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import jakarta.persistence.Version;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;
import org.hibernate.annotations.CreationTimestamp;
import org.hibernate.annotations.UpdateTimestamp;

import java.math.BigDecimal;
import java.time.LocalDateTime;

/**
 * Payment — the central JPA entity representing a single payment record.
 *
 * <h2>Optimistic Locking with {@code @Version}</h2>
 * <p>The {@code version} field is managed entirely by Hibernate. On every UPDATE,
 * Hibernate generates SQL of the form:
 * <pre>
 *   UPDATE payments
 *   SET status = ?, retry_count = ?, version = version + 1, ...
 *   WHERE id = ? AND version = &lt;expected-version&gt;
 * </pre>
 * <p>If two concurrent threads (e.g., a REST request and a Kafka consumer) both read the
 * same entity at version N and both attempt to update it:
 * <ol>
 *   <li>Thread A commits first → version becomes N+1</li>
 *   <li>Thread B tries to commit with WHERE version = N → 0 rows affected</li>
 *   <li>Hibernate detects 0 rows affected and throws
 *       {@code ObjectOptimisticLockingFailureException}</li>
 * </ol>
 * <p>The {@code PaymentOrchestratorService} and {@code PaymentRetryConsumer} both catch
 * this exception and handle it safely (log + skip / retry the DB operation).
 *
 * <h2>Why CHAR(36) for UUID?</h2>
 * <p>We store UUID as a {@code String} and map it to {@code CHAR(36)} in MySQL.  This
 * is readable in local development and avoids byte-order confusion.  The trade-off is
 * slightly more storage and slower index scans vs {@code BINARY(16)}, which is acceptable
 * for a local development system.
 */
@Entity
@Table(name = "payments")
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
@ToString
public class Payment {

    /**
     * Primary key — a pre-generated UUID passed in from the service layer.
     * Using {@code String} avoids requiring {@code @GeneratedValue} strategy changes.
     */
    @Id
    @Column(name = "id", length = 36, nullable = false, updatable = false)
    private String id;

    /**
     * Client-supplied idempotency key from the {@code Idempotency-Key} HTTP header.
     * The unique index {@code uq_idempotency_key} on the DB column is the hard safety net;
     * Redis is the soft (fast-path) guard.
     */
    @Column(name = "idempotency_key", length = 255, nullable = false, unique = true, updatable = false)
    private String idempotencyKey;

    /**
     * Monetary amount.  {@code DECIMAL(19,4)} in MySQL maps to {@code BigDecimal} in Java.
     * Never use {@code double} or {@code float} for money — IEEE 754 rounding causes errors.
     */
    @Column(name = "amount", precision = 19, scale = 4, nullable = false)
    private BigDecimal amount;

    /**
     * ISO 4217 currency code, e.g., "USD", "INR", "EUR".
     */
    @Column(name = "currency", length = 10, nullable = false)
    private String currency;

    /**
     * The payment instrument.  Stored as an EnumType.STRING so that
     * adding new enum values in future doesn't corrupt existing ordinal data.
     */
    @Enumerated(EnumType.STRING)
    @Column(name = "payment_method", nullable = false)
    private PaymentMethod paymentMethod;

    /**
     * Current lifecycle status.  Updated by the orchestrator service and Kafka consumer.
     * Protected against concurrent updates via the {@code version} optimistic lock.
     */
    @Enumerated(EnumType.STRING)
    @Column(name = "status", nullable = false)
    @Builder.Default
    private PaymentStatus status = PaymentStatus.INITIATED;

    /**
     * Identifies the provider connector that last processed (or is processing) this payment.
     * Nullable until routing has taken place.
     */
    @Column(name = "provider_id", length = 100)
    private String providerId;

    /**
     * Transaction reference ID returned by the external payment provider.
     * Used for reconciliation and customer support lookups.
     * Nullable until a provider successfully processes the payment.
     */
    @Column(name = "provider_reference_id", length = 255)
    private String providerReferenceId;

    /**
     * Number of times the Kafka retry consumer has attempted to process this payment.
     * Starts at 0. The DLT is triggered when retryCount reaches the max configured in
     * {@code @RetryableTopic(attempts = 3)}.
     */
    @Column(name = "retry_count", nullable = false)
    @Builder.Default
    private Integer retryCount = 0;

    /**
     * Optimistic locking version counter.  Managed exclusively by Hibernate.
     * <strong>Never set this field manually</strong> — doing so will break the locking
     * semantics and may cause silent data corruption.
     *
     * <p>When a concurrent modification is detected, Hibernate throws
     * {@code ObjectOptimisticLockingFailureException} (a subclass of
     * {@code OptimisticLockingFailureException}) which is handled in
     * {@code PaymentOrchestratorService} and the Kafka consumer.
     */
    @Version
    @Column(name = "version", nullable = false)
    private Long version;

    /**
     * Record creation timestamp — set once at INSERT time by Hibernate's
     * {@code @CreationTimestamp}. Never modified thereafter (immutable audit field).
     */
    @CreationTimestamp
    @Column(name = "created_at", nullable = false, updatable = false)
    private LocalDateTime createdAt;

    /**
     * Last modification timestamp — automatically refreshed by Hibernate on every UPDATE
     * via {@code @UpdateTimestamp}.
     */
    @UpdateTimestamp
    @Column(name = "updated_at", nullable = false)
    private LocalDateTime updatedAt;
}

