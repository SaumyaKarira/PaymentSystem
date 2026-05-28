package org.example.provider;

import lombok.extern.slf4j.Slf4j;
import org.example.exception.ProviderException;

import java.math.BigDecimal;
import java.util.Random;
import java.util.UUID;

// Base class for simulated provider connectors.
// Simulates ~20% failure rate (randomly 504 or 500) to exercise Kafka retry and DLT paths.
// Subclasses supply provider ID and transaction reference prefix.
@Slf4j
public abstract class AbstractSimulatedProviderConnector implements PaymentProviderConnector {

    // 20% of calls will throw ProviderException
    private static final double FAILURE_RATE = 0.20;

    private final Random random = new Random();

    // Returns the transaction ref prefix (e.g., "PROVA", "PROVB")
    protected abstract String getTxnRefPrefix();

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

