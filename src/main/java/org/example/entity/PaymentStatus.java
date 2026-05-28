package org.example.entity;

// Lifecycle state of a payment record.
// Transitions: INITIATED → SUCCESS (sync provider success)
//              INITIATED → PROCESSING (sync provider failure, pushed to Kafka)
//              PROCESSING → SUCCESS (Kafka retry succeeded)
//              PROCESSING → FAILED (all Kafka retries exhausted, DLT handler fired)
public enum PaymentStatus {
    INITIATED,   // Payment persisted; synchronous provider call is about to begin
    PROCESSING,  // Sync provider call failed; Kafka retry consumer is handling recovery
    SUCCESS,     // Provider returned success; providerReferenceId is populated
    FAILED       // All retry attempts exhausted; no further automated recovery
}
