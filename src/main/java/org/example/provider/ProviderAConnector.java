package org.example.provider;

import org.springframework.stereotype.Component;

/**
 * ProviderAConnector — simulates integration with Payment Provider A.
 *
 * <p>Selected by the {@code RoutingEngine} when the payment method is {@code CARD}.
 *
 * <p>All failure simulation logic (20% failure rate, 504/500 error modes) is inherited
 * from {@link AbstractSimulatedProviderConnector}, keeping this class focused solely on
 * its provider-specific identity.
 */
@Component
public class ProviderAConnector extends AbstractSimulatedProviderConnector {

    private static final String PROVIDER_ID   = "PROVIDER_A";
    private static final String TXN_REF_PREFIX = "PROVA";

    @Override
    public String getProviderId() {
        return PROVIDER_ID;
    }

    @Override
    protected String getTxnRefPrefix() {
        return TXN_REF_PREFIX;
    }
}
