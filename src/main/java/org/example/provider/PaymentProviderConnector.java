package org.example.provider;

import java.math.BigDecimal;

// Common abstraction for all external payment provider integrations.
// RoutingEngine and PaymentOrchestratorService depend on this interface, not on concrete implementations.
public interface PaymentProviderConnector {

    // Returns the unique provider identifier (e.g., "PROVIDER_A")
    String getProviderId();

    // Processes a payment with the external provider.
    // Returns the provider's transaction reference ID on success.
    // Throws ProviderException (~20% simulated failure rate) on 504 or 500 errors.
    String processPayment(String paymentId, BigDecimal amount, String currency);
}
