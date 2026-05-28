package org.example.entity;

// Payment instrument used for a transaction.
// CARD → routed to ProviderAConnector; UPI → routed to ProviderBConnector.
// Stored as STRING in DB so adding new values won't corrupt existing ordinal data.
public enum PaymentMethod {
    CARD,  // Credit/debit/prepaid card payments
    UPI    // Unified Payments Interface — real-time bank transfer
}
