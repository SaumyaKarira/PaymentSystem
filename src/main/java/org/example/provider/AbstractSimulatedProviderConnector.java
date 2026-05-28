package org.example.provider;

import lombok.extern.slf4j.Slf4j;
import org.example.exception.ProviderException;

import java.math.BigDecimal;
import java.util.Random;
import java.util.UUID;

/**
 * AbstractSimulatedProviderConnector — shared simulation logic for all provider connectors.
 *
 * <h2>Purpose</h2>
 * <p>Both {@link ProviderAConnector} and {@link ProviderBConnector} simulate an ~20%
 * provider failure rate to exercise the Kafka retry and DLT paths during local development.
 * This base class centralises that simulation logic so it is defined exactly once (DRY).
 *
 * <h2>Failure Simulation</h2>
 * <ul>
 *   <li>~20% of calls throw a {@link ProviderException}.</li>
 *   <li>The failure mode is randomly chosen between 504 Gateway Timeout and
 *       500 Internal Server Error to simulate real-world transient variance.</li>
 * </ul>
 *
 * <h2>Subclass Contract</h2>
 * <p>Concrete subclasses must implement:
 * <ul>
 *   <li>{@link #getProviderId()} — returns the unique provider identifier (e.g., "PROVIDER_A")</li>
 *   <li>{@link #getTxnRefPrefix()} — returns the prefix for mock transaction references
 *       (e.g., "PROVA" → reference will look like "PROVA-ABC123DEF456")</li>
 * </ul>
 */
@Slf4j
public abstract class AbstractSimulatedProviderConnector implements PaymentProviderConnector {

    /**
     * Failure probability: 20% of calls will throw a {@link ProviderException}.
     * Adjust this constant in a subclass or here to control retry path coverage.
     */
    private static final double FAILURE_RATE = 0.20;

    /**
     * Thread-safe random instance for failure simulation.
     * {@code java.util.Random} is sufficient — no cryptographic randomness needed here.
     */
    private final Random random = new Random();

    /**
     * Returns the transaction reference prefix for this provider.
     * Used to construct a mock provider transaction ID on successful processing.
     * <p>Example: "PROVA" → produces transaction refs like "PROVA-ABC123DEF456GHIJ"
     *
     * @return non-null prefix string (e.g., "PROVA", "PROVB")
     */
    protected abstract String getTxnRefPrefix();

    /**
     * Simulates an HTTP call to the external payment provider.
     *
     * <ol>
     *   <li>Generates a random double in [0.0, 1.0)</li>
     *   <li>If the value is below {@link #FAILURE_RATE}, simulates a transient failure</li>
     *   <li>Randomly chooses between 504 (timeout) and 500 (server error) failure modes</li>
     *   <li>Otherwise returns a mock provider reference ID as if the call succeeded</li>
     * </ol>
     *
     * @param paymentId the UUID of the payment
     * @param amount    the payment amount
     * @param currency  the ISO 4217 currency code
     * @return a mock provider transaction reference on success
     * @throws ProviderException on simulated 504 or 500 failure
     */
    @Override
    public String processPayment(String paymentId, BigDecimal amount, String currency) {
        log.debug("[{}] Attempting to process payment [{}], amount={} {}",
                getProviderId(), paymentId, amount, currency);

        // ── FAILURE SIMULATION ────────────────────────────────────────────────
        double roll = random.nextDouble();
        if (roll < FAILURE_RATE) {
            boolean isTimeout = random.nextBoolean();
            if (isTimeout) {
                log.warn("[{}] Simulated 504 Gateway Timeout for payment [{}]",
                        getProviderId(), paymentId);
                throw new ProviderException(
                        getProviderId(),
                        "504 Gateway Timeout: " + getProviderId()
                                + " did not respond within the deadline for payment " + paymentId
                );
            } else {
                log.warn("[{}] Simulated 500 Internal Server Error for payment [{}]",
                        getProviderId(), paymentId);
                throw new ProviderException(
                        getProviderId(),
                        "500 Internal Server Error: " + getProviderId()
                                + " encountered an internal fault while processing payment " + paymentId
                );
            }
        }

        // ── SUCCESS PATH ──────────────────────────────────────────────────────
        // Generate a deterministic-looking mock transaction reference.
        // In a real integration, this would be the txn ID from the provider's API response.
        String providerTxnRef = getTxnRefPrefix() + "-"
                + UUID.randomUUID().toString().replace("-", "").substring(0, 16).toUpperCase();

        log.info("[{}] Successfully processed payment [{}]. Provider reference: {}",
                getProviderId(), paymentId, providerTxnRef);
        return providerTxnRef;
    }
}

