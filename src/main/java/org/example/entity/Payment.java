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

// JPA entity representing a single payment record.
// Optimistic locking via @Version prevents concurrent update conflicts.
@Entity
@Table(name = "payments")
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
@ToString
public class Payment {

    // Pre-generated UUID from the service layer
    @Id
    @Column(name = "id", length = 36, nullable = false, updatable = false)
    private String id;

    // Client-supplied idempotency key; UNIQUE indexed — hard safety net against duplicate payments
    @Column(name = "idempotency_key", length = 255, nullable = false, unique = true, updatable = false)
    private String idempotencyKey;

    // Monetary amount — BigDecimal to avoid IEEE 754 rounding errors
    @Column(name = "amount", precision = 19, scale = 4, nullable = false)
    private BigDecimal amount;

    // ISO 4217 currency code (e.g., "USD", "INR")
    @Column(name = "currency", length = 10, nullable = false)
    private String currency;

    // Stored as STRING so adding new enum values won't corrupt existing ordinal data
    @Enumerated(EnumType.STRING)
    @Column(name = "payment_method", nullable = false)
    private PaymentMethod paymentMethod;

    // Current lifecycle status; protected against concurrent updates by @Version
    @Enumerated(EnumType.STRING)
    @Column(name = "status", nullable = false)
    @Builder.Default
    private PaymentStatus status = PaymentStatus.INITIATED;

    // Provider connector ID that last processed this payment; null until routing occurs
    @Column(name = "provider_id", length = 100)
    private String providerId;

    // Provider transaction reference for reconciliation; null until a provider succeeds
    @Column(name = "provider_reference_id", length = 255)
    private String providerReferenceId;

    // Number of Kafka delivery attempts; starts at 0
    @Column(name = "retry_count", nullable = false)
    @Builder.Default
    private Integer retryCount = 0;

    // Optimistic lock version — managed exclusively by Hibernate; never set manually
    @Version
    @Column(name = "version", nullable = false)
    private Long version;

    // Set once at INSERT by Hibernate; immutable audit field
    @CreationTimestamp
    @Column(name = "created_at", nullable = false, updatable = false)
    private LocalDateTime createdAt;

    // Automatically refreshed by Hibernate on every UPDATE
    @UpdateTimestamp
    @Column(name = "updated_at", nullable = false)
    private LocalDateTime updatedAt;
}
