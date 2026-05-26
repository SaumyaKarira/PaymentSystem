package org.example.exception;

/**
 * ProviderException — thrown by a {@code PaymentProviderConnector} implementation
 * when the simulated external provider call fails (5xx or timeout).
 *
 * <p>The {@code providerName} field is included so that the orchestrator can log
 * exactly which provider failed without needing to inspect the message string.
 */
public class ProviderException extends RuntimeException {

    /** The logical name of the provider that failed (e.g., "PROVIDER_A"). */
    private final String providerName;

    /**
     * @param providerName the provider that threw the error
     * @param message      human-readable description of the failure
     */
    public ProviderException(String providerName, String message) {
        super(message);
        this.providerName = providerName;
    }

    /**
     * @param providerName the provider that threw the error
     * @param message      human-readable description of the failure
     * @param cause        the underlying exception (e.g., a simulated IOException)
     */
    public ProviderException(String providerName, String message, Throwable cause) {
        super(message, cause);
        this.providerName = providerName;
    }

    public String getProviderName() {
        return providerName;
    }
}

