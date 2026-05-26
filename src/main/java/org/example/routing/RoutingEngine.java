package org.example.routing;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.example.entity.PaymentMethod;
import org.example.provider.PaymentProviderConnector;
import org.example.provider.ProviderAConnector;
import org.example.provider.ProviderBConnector;
import org.springframework.stereotype.Service;

/**
 * RoutingEngine — determines which {@link PaymentProviderConnector} should handle a payment
 * based on the payment method and an optional failover flag.
 *
 * <h2>Routing Rules</h2>
 * <ul>
 *   <li>{@code CARD} (primary)  → {@link ProviderAConnector}</li>
 *   <li>{@code UPI}  (primary)  → {@link ProviderBConnector}</li>
 *   <li>{@code CARD} (failover) → {@link ProviderBConnector}
 *       (Provider A failed during Kafka retries; switch to B)</li>
 *   <li>{@code UPI}  (failover) → {@link ProviderAConnector}
 *       (Provider B failed during Kafka retries; switch to A)</li>
 * </ul>
 *
 * <h2>Why failover is at the routing layer</h2>
 * <p>Keeping failover logic in the routing engine rather than the consumer means that
 * any future addition of a third provider only requires changes here, not in the
 * consumer or orchestrator.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class RoutingEngine {

    /**
     * Injected Spring-managed provider beans.
     * Using constructor injection (via @RequiredArgsConstructor) rather than field
     * injection makes the class easier to unit-test with mock connectors.
     */
    private final ProviderAConnector providerAConnector;
    private final ProviderBConnector providerBConnector;

    /**
     * Selects the appropriate provider connector for a given payment method.
     *
     * <p>This overload is used for the PRIMARY (synchronous) payment attempt,
     * where no failover is needed yet.
     *
     * @param paymentMethod CARD or UPI
     * @return the primary provider connector for that method
     */
    public PaymentProviderConnector route(PaymentMethod paymentMethod) {
        return route(paymentMethod, false);
    }

    /**
     * Selects the appropriate provider connector, respecting the failover flag.
     *
     * <p>The {@code failover} flag is set to {@code true} by the Kafka retry consumer
     * after the first retry attempt fails.  This causes the routing engine to switch
     * to the alternative provider for subsequent retry attempts.
     *
     * <p>Failover strategy diagram:
     * <pre>
     *   Normal:   CARD → ProviderA,  UPI → ProviderB
     *   Failover: CARD → ProviderB,  UPI → ProviderA
     * </pre>
     *
     * @param paymentMethod CARD or UPI
     * @param failover      true to route to the alternate (fallback) provider
     * @return the selected provider connector
     * @throws IllegalArgumentException if {@code paymentMethod} is not handled
     */
    public PaymentProviderConnector route(PaymentMethod paymentMethod, boolean failover) {
        PaymentProviderConnector connector = switch (paymentMethod) {
            case CARD -> failover ? providerBConnector : providerAConnector;
            case UPI  -> failover ? providerAConnector : providerBConnector;
        };
        log.debug("Routing decision: method={}, failover={} → provider={}",
                paymentMethod, failover, connector.getProviderId());
        return connector;
    }
}

