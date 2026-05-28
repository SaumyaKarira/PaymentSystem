package org.example.routing;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.example.entity.PaymentMethod;
import org.example.provider.PaymentProviderConnector;
import org.example.provider.ProviderAConnector;
import org.example.provider.ProviderBConnector;
import org.springframework.stereotype.Service;

// Routes payments to the appropriate provider based on payment method.
// CARD → ProviderA, UPI → ProviderB
@Slf4j
@Service
@RequiredArgsConstructor
public class RoutingEngine {

    private final ProviderAConnector providerAConnector;
    private final ProviderBConnector providerBConnector;

    // Returns the provider connector for the given payment method.
    public PaymentProviderConnector route(PaymentMethod paymentMethod) {
        PaymentProviderConnector connector = switch (paymentMethod) {
            case CARD -> providerAConnector;
            case UPI  -> providerBConnector;
        };
        log.debug("Routing decision: method={} → provider={}", paymentMethod, connector.getProviderId());
        return connector;
    }
}
