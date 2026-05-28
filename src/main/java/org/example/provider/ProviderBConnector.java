package org.example.provider;

import lombok.extern.slf4j.Slf4j;
import org.example.exception.ProviderException;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.util.Random;
import java.util.UUID;

/**
 * ProviderBConnector — simulates integration with Payment Provider B.
 *
 * <p>This connector is selected by the {@code RoutingEngine} when the payment method
 * is {@code UPI}.
 *
 * <h2>Failure Simulation</h2>
 * <p>Identical ~20% failure rate to Provider A, ensuring the retry infrastructure
 * is exercised for UPI payments.
 */
@Slf4j
@Component
public class ProviderBConnector implements PaymentProviderConnector {

    private static final String PROVIDER_ID = "PROVIDER_B";

    /**
     * Failure probability: 20% of calls will throw a {@code ProviderException}.
     */
    private static final double FAILURE_RATE = 0.20;

    private final Random random = new Random();

    @Override
    public String getProviderId() {
        return PROVIDER_ID;
    }

    /**
     * {@inheritDoc}
     *
     * <p>Simulates an HTTP call to Provider B's payment API.
     * The failure simulation logic mirrors {@code ProviderAConnector} — see that
     * class for a detailed explanation of the random failure mechanism.
     *
     * @param paymentId the UUID of the payment
     * @param amount    the payment amount
     * @param currency  the payment currency
     * @return a mock provider transaction reference (e.g., "PROVB-{uuid}")
     * @throws ProviderException on simulated 504 or 500 error
     */
    @Override
    public String processPayment(String paymentId, BigDecimal amount, String currency) {
        log.debug("[{}] Attempting to process payment [{}], amount={} {}",
                PROVIDER_ID, paymentId, amount, currency);

        // ── FAILURE SIMULATION ───────────────────────────────────────────────
        double roll = random.nextDouble();
        if (roll < FAILURE_RATE) {
            boolean isTimeout = random.nextBoolean();
            if (isTimeout) {
                log.warn("[{}] Simulated 504 Gateway Timeout for payment [{}]",
                        PROVIDER_ID, paymentId);
                throw new ProviderException(
                        PROVIDER_ID,
                        "504 Gateway Timeout: Provider B did not respond within the deadline "
                                + "for payment " + paymentId
                );
            } else {
                log.warn("[{}] Simulated 500 Internal Server Error for payment [{}]",
                        PROVIDER_ID, paymentId);
                throw new ProviderException(
                        PROVIDER_ID,
                        "500 Internal Server Error: Provider B encountered an internal fault "
                                + "while processing payment " + paymentId
                );
            }
        }

        // ── SUCCESS PATH ─────────────────────────────────────────────────────
        String providerTxnRef = "PROVB-" + UUID.randomUUID().toString().replace("-", "").substring(0, 16).toUpperCase();
        log.info("[{}] Successfully processed payment [{}]. Provider reference: {}",
                PROVIDER_ID, paymentId, providerTxnRef);
        return providerTxnRef;
    }
}

