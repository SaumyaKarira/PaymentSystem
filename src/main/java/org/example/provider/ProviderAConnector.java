package org.example.provider;

import org.springframework.stereotype.Component;

// Handles CARD payments via Provider A. Failure simulation inherited from base class.
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
