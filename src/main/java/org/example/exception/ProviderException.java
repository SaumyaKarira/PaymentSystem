package org.example.exception;

// Thrown by a PaymentProviderConnector when the simulated provider call fails (5xx or timeout).
// providerName identifies which provider failed so callers can log it without parsing the message.
public class ProviderException extends RuntimeException {

    private final String providerName;

    public ProviderException(String providerName, String message) {
        super(message);
        this.providerName = providerName;
    }

    public ProviderException(String providerName, String message, Throwable cause) {
        super(message, cause);
        this.providerName = providerName;
    }

    public String getProviderName() {
        return providerName;
    }
}
