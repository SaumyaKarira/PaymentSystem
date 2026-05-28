package org.example.provider;

import org.springframework.stereotype.Component;
// Handles UPI payments via Provider B. Failure simulation inherited from base class.
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
