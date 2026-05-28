package org.example.routing;

import org.example.entity.PaymentMethod;
import org.example.provider.PaymentProviderConnector;
import org.example.provider.ProviderAConnector;
import org.example.provider.ProviderBConnector;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.when;

/**
 * RoutingEngineTest — unit tests for {@link RoutingEngine}.
 *
 * <p>Verifies that the routing switch correctly maps each {@link PaymentMethod}
 * to the expected {@link PaymentProviderConnector} implementation.
 */
@ExtendWith(MockitoExtension.class)
@DisplayName("RoutingEngine — Unit Tests")
class RoutingEngineTest {

    @Mock
    private ProviderAConnector providerAConnector;

    @Mock
    private ProviderBConnector providerBConnector;

    private RoutingEngine routingEngine;

    @BeforeEach
    void setUp() {
        routingEngine = new RoutingEngine(providerAConnector, providerBConnector);
    }

    @Test
    @DisplayName("CARD payment method routes to ProviderAConnector")
    void cardPayment_routesToProviderA() {
        when(providerAConnector.getProviderId()).thenReturn("PROVIDER_A");

        PaymentProviderConnector result = routingEngine.route(PaymentMethod.CARD);

        assertThat(result).isSameAs(providerAConnector);
        assertThat(result.getProviderId()).isEqualTo("PROVIDER_A");
    }

    @Test
    @DisplayName("UPI payment method routes to ProviderBConnector")
    void upiPayment_routesToProviderB() {
        when(providerBConnector.getProviderId()).thenReturn("PROVIDER_B");

        PaymentProviderConnector result = routingEngine.route(PaymentMethod.UPI);

        assertThat(result).isSameAs(providerBConnector);
        assertThat(result.getProviderId()).isEqualTo("PROVIDER_B");
    }

    @Test
    @DisplayName("CARD never routes to ProviderB")
    void cardPayment_neverRoutesToProviderB() {
        PaymentProviderConnector result = routingEngine.route(PaymentMethod.CARD);

        assertThat(result).isNotSameAs(providerBConnector);
    }

    @Test
    @DisplayName("UPI never routes to ProviderA")
    void upiPayment_neverRoutesToProviderA() {
        PaymentProviderConnector result = routingEngine.route(PaymentMethod.UPI);

        assertThat(result).isNotSameAs(providerAConnector);
    }

    @Test
    @DisplayName("Routing returns a non-null connector for all defined payment methods")
    void allDefinedPaymentMethods_returnNonNullConnector() {
        for (PaymentMethod method : PaymentMethod.values()) {
            PaymentProviderConnector connector = routingEngine.route(method);
            assertThat(connector)
                    .as("Connector for payment method %s must not be null", method)
                    .isNotNull();
        }
    }
}

