package org.example.provider;

import org.springframework.stereotype.Component;

/**
 * ProviderBConnector — simulates integration with Payment Provider B.
 *
 * <p>Selected by the {@code RoutingEngine} when the payment method is {@code UPI}.
 *
 * <p>All failure simulation logic (20% failure rate, 504/500 error modes) is inherited
 * from {@link AbstractSimulatedProviderConnector}, keeping this class focused solely on
 * its provider-specific identity.
 */
@Component
public class ProviderBConnector extends AbstractSimulatedProviderConnector {

    private static final String PROVIDER_ID    = "PROVIDER_B";
    private static final String TXN_REF_PREFIX = "PROVB";

    @Override
    public String getProviderId() {
        return PROVIDER_ID;
    }

    @Override
    protected String getTxnRefPrefix() {
        return TXN_REF_PREFIX;
    }
}
