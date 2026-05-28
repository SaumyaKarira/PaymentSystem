package org.example.provider;

import lombok.extern.slf4j.Slf4j;
import org.example.exception.ProviderException;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.util.Random;
import java.util.UUID;

/**
 * ProviderAConnector — simulates integration with Payment Provider A.
 *
 * <p>This connector is selected by the {@code RoutingEngine} when the payment method
 * is {@code CARD}.
 *
 * <h2>Failure Simulation Logic</h2>
 * <p>To force the retry and DLQ infrastructure to activate, this connector intentionally
 * fails ~20% of calls.  The failure mode is randomly chosen between:
 * <ul>
 *   <li><strong>504 Gateway Timeout</strong>: The upstream provider took too long to respond.</li>
 *   <li><strong>500 Internal Server Error</strong>: The upstream provider returned a server fault.</li>
 * </ul>
 * This mirrors real-world transient failures that should be retried.
 */
@Slf4j
@Component
public class ProviderAConnector implements PaymentProviderConnector {

    private static final String PROVIDER_ID = "PROVIDER_A";

    /**
     * Failure probability: 20% of calls will throw a {@code ProviderException}.
     * Adjust this constant to control retry path coverage during local testing.
     */
    private static final double FAILURE_RATE = 0.20;

    /**
     * Thread-safe random instance for simulating provider failures.
     * Using {@code java.util.Random} is sufficient here — we do not need
     * cryptographic randomness for failure simulation.
     */
    private final Random random = new Random();

    @Override
    public String getProviderId() {
        return PROVIDER_ID;
    }

    /**
     * {@inheritDoc}
     *
     * <p>Simulates an HTTP call to Provider A's payment processing endpoint.
     * <ol>
     *   <li>Generate a random double in [0.0, 1.0)</li>
     *   <li>If the value is below {@link #FAILURE_RATE}, simulate a failure</li>
     *   <li>Randomly choose between 504 (timeout) and 500 (server error)</li>
     *   <li>Otherwise, return a mock provider reference ID as if the call succeeded</li>
     * </ol>
     *
     * @param paymentId the UUID of the payment
     * @param amount    the payment amount
     * @param currency  the payment currency
     * @return a mock provider transaction reference (e.g., "PROVA-{uuid}")
     * @throws ProviderException on simulated 504 or 500 error
     */
    @Override
    public String processPayment(String paymentId, BigDecimal amount, String currency) {
        log.debug("[{}] Attempting to process payment [{}], amount={} {}",
                PROVIDER_ID, paymentId, amount, currency);

        // ── FAILURE SIMULATION ───────────────────────────────────────────────
        // Roll a random number; if it falls within the failure window, simulate
        // one of the two common transient provider errors.
        double roll = random.nextDouble();
        if (roll < FAILURE_RATE) {
            // Randomly select between two error modes to simulate real-world variance
            boolean isTimeout = random.nextBoolean();
            if (isTimeout) {
                log.warn("[{}] Simulated 504 Gateway Timeout for payment [{}]",
                        PROVIDER_ID, paymentId);
                throw new ProviderException(
                        PROVIDER_ID,
                        "504 Gateway Timeout: Provider A did not respond within the deadline "
                                + "for payment " + paymentId
                );
            } else {
                log.warn("[{}] Simulated 500 Internal Server Error for payment [{}]",
                        PROVIDER_ID, paymentId);
                throw new ProviderException(
                        PROVIDER_ID,
                        "500 Internal Server Error: Provider A encountered an internal fault "
                                + "while processing payment " + paymentId
                );
            }
        }
        // ── SUCCESS PATH ─────────────────────────────────────────────────────
        // Generate a deterministic-looking mock transaction reference.
        // In a real integration, this would be the txn ID from the provider's API response.
        String providerTxnRef = "PROVA-" + UUID.randomUUID().toString().replace("-", "").substring(0, 16).toUpperCase();
        log.info("[{}] Successfully processed payment [{}]. Provider reference: {}",
                PROVIDER_ID, paymentId, providerTxnRef);
        return providerTxnRef;
    }
}

