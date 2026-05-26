package org.example.entity;

/**
 * PaymentMethod — represents the payment instrument used for a transaction.
 *
 * <p>The routing engine uses this enum to select the appropriate provider connector:
 * <ul>
 *   <li>{@link #CARD} → routed to {@code ProviderAConnector}</li>
 *   <li>{@link #UPI}  → routed to {@code ProviderBConnector}</li>
 * </ul>
 *
 * <p>Stored as a STRING in the DB (via {@code @Enumerated(EnumType.STRING)}) so that
 * adding future values doesn't break existing ordinal-based data.
 */
public enum PaymentMethod {
    /** Card-based payments: credit cards, debit cards, prepaid cards */
    CARD,
    /** Unified Payments Interface: real-time bank transfer via VPA */
    UPI
}

