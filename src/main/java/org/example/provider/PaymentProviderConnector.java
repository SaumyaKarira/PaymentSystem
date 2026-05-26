package org.example.provider;

import java.math.BigDecimal;

/**
 * PaymentProviderConnector — the common abstraction for all external payment provider
 * integrations.
 *
 * <h2>Design Rationale</h2>
 * <p>Using an interface here (rather than an abstract class) follows the
 * Dependency Inversion Principle: the {@code RoutingEngine} and
 * {@code PaymentOrchestratorService} depend on this abstraction, not on concrete
 * provider implementations.  This makes it trivial to:
 * <ul>
 *   <li>Add new providers (implement the interface, register with the routing engine)</li>
 *   <li>Write unit tests with mock connectors</li>
 *   <li>Swap providers without touching orchestration logic</li>
 * </ul>
 *
 * <h2>Failure Simulation</h2>
 * <p>Concrete implementations intentionally simulate a ~20% failure rate to exercise
 * the Kafka retry and DLQ paths.  The simulated failures are:
 * <ul>
 *   <li>{@code 504 Gateway Timeout} — the upstream provider is slow / unresponsive</li>
 *   <li>{@code 500 Internal Server Error} — the upstream provider returned an error</li>
 * </ul>
 */
public interface PaymentProviderConnector {

    /**
     * Returns a unique identifier for this provider (e.g., "PROVIDER_A").
     * Used by the orchestrator to record which provider handled a payment.
     *
     * @return non-null provider ID string
     */
    String getProviderId();

    /**
     * Attempts to process a payment with the external provider.
     *
     * <p>On success, returns a non-null provider transaction reference ID that can be
     * stored on the payment record for reconciliation purposes.
     *
     * <p>On failure (simulated ~20% of the time), throws a
     * {@link org.example.exception.ProviderException} wrapping either a timeout or
     * a 5xx error description.
     *
     * @param paymentId  the UUID of the payment being processed
     * @param amount     the monetary amount
     * @param currency   the ISO 4217 currency code
     * @return the provider's transaction reference ID on success
     * @throws org.example.exception.ProviderException on simulated provider failure
     */
    String processPayment(String paymentId, BigDecimal amount, String currency);
}

