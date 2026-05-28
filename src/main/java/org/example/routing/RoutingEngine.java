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
 * based on the payment method.
 *
 * <h2>Routing Rules</h2>
 * <ul>
 *   <li>{@code CARD} → {@link ProviderAConnector}</li>
 *   <li>{@code UPI}  → {@link ProviderBConnector}</li>
 * </ul>
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
     * @param paymentMethod CARD or UPI
     * @return the primary provider connector for that method
     * @throws IllegalArgumentException if {@code paymentMethod} is not handled
     */
    public PaymentProviderConnector route(PaymentMethod paymentMethod) {
        PaymentProviderConnector connector = switch (paymentMethod) {
            case CARD -> providerAConnector;
            case UPI  -> providerBConnector;
        };
        log.debug("Routing decision: method={} → provider={}", paymentMethod, connector.getProviderId());
        return connector;
    }
}

