package org.example.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.example.dto.PaymentEvent;
import org.example.entity.Payment;
import org.example.entity.PaymentMethod;
import org.example.entity.PaymentStatus;
import org.example.exception.ProviderException;
import org.example.provider.PaymentProviderConnector;
import org.example.provider.ProviderAConnector;
import org.example.provider.ProviderBConnector;
import org.example.repository.PaymentRepository;
import org.example.routing.RoutingEngine;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * PaymentRetryConsumerTest — direct unit tests for {@link PaymentRetryConsumer}.
 *
 * <p>Tests invoke {@code processPayment()} and {@code handleDlt()} directly without
 * any Kafka broker. All dependencies are Mockito mocks. Spring Kafka header injection
 * is simulated by passing the {@code deliveryAttempt} parameter directly.
 *
 * <p>This class covers the core logic paths of the consumer in fast, isolated unit tests.
 * End-to-end Kafka routing and retry-topic forwarding are covered separately in
 * {@link PaymentConsumerIntegrationTest}.
 */
@ExtendWith(MockitoExtension.class)
@DisplayName("PaymentRetryConsumer — Unit Tests")
class PaymentRetryConsumerTest {

    @Mock private PaymentRepository paymentRepository;
    @Mock private RoutingEngine routingEngine;
    @Mock private ProviderAConnector providerAConnector;
    @Mock private ProviderBConnector providerBConnector;

    private PaymentRetryConsumer consumer;

    private static final String PAYMENT_ID    = "unit-test-pay-id-001";
    private static final String IDEM_KEY      = "unit-test-idem-key-001";
    private static final String PROVIDER_A_ID = "PROVIDER_A";
    private static final String PROVIDER_REF  = "PROVA-UNIT-TEST-REF";
    private static final BigDecimal AMOUNT    = new BigDecimal("200.00");
    private static final String CURRENCY      = "USD";
    private static final String TOPIC         = "payment-main-topic";

    @BeforeEach
    void setUp() {
        consumer = new PaymentRetryConsumer(paymentRepository, routingEngine);
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    private ConsumerRecord<String, PaymentEvent> record(int retryCount) {
        return new ConsumerRecord<>(TOPIC, 0, 0L, PAYMENT_ID,
                new PaymentEvent(PAYMENT_ID, AMOUNT, CURRENCY, PaymentMethod.CARD, retryCount));
    }

    private Payment processingPayment(int retryCount, long version) {
        return Payment.builder()
                .id(PAYMENT_ID).idempotencyKey(IDEM_KEY)
                .amount(AMOUNT).currency(CURRENCY)
                .paymentMethod(PaymentMethod.CARD).status(PaymentStatus.PROCESSING)
                .providerId(PROVIDER_A_ID).retryCount(retryCount).version(version)
                .createdAt(LocalDateTime.now()).updatedAt(LocalDateTime.now())
                .build();
    }

    // =========================================================================
    // processPayment — success path
    // =========================================================================

    @Nested
    @DisplayName("processPayment — Success Path")
    class SuccessPathTests {

        @Test
        @DisplayName("Provider succeeds → updateOnSuccess called with correct args, no re-throw")
        void providerSucceeds_updatesStatusToSuccess() {
            Payment payment = processingPayment(1, 1L);
            when(paymentRepository.findById(PAYMENT_ID)).thenReturn(Optional.of(payment));
            when(routingEngine.route(PaymentMethod.CARD)).thenReturn(providerAConnector);
            when(providerAConnector.getProviderId()).thenReturn(PROVIDER_A_ID);
            when(providerAConnector.processPayment(PAYMENT_ID, AMOUNT, CURRENCY))
                    .thenReturn(PROVIDER_REF);
            when(paymentRepository.updateOnSuccess(
                    eq(PAYMENT_ID), eq(PaymentStatus.SUCCESS),
                    eq(PROVIDER_A_ID), eq(PROVIDER_REF), eq(1), eq(1L)))
                    .thenReturn(1);

            assertThatCode(() ->
                    consumer.processPayment(record(1), TOPIC, 1)
            ).doesNotThrowAnyException();

            verify(paymentRepository).updateOnSuccess(
                    PAYMENT_ID, PaymentStatus.SUCCESS, PROVIDER_A_ID, PROVIDER_REF, 1, 1L);
            verify(paymentRepository, never()).updateStatusWithVersionCheck(
                    anyString(), any(), anyInt(), anyLong());
        }

        @Test
        @DisplayName("Optimistic lock conflict (rowsUpdated=0) on success → logs and returns normally without re-throw")
        void optimisticLockConflictOnSuccess_logsAndReturnsNormally() {
            Payment payment = processingPayment(0, 2L);
            when(paymentRepository.findById(PAYMENT_ID)).thenReturn(Optional.of(payment));
            when(routingEngine.route(PaymentMethod.CARD)).thenReturn(providerAConnector);
            when(providerAConnector.getProviderId()).thenReturn(PROVIDER_A_ID);
            when(providerAConnector.processPayment(PAYMENT_ID, AMOUNT, CURRENCY))
                    .thenReturn(PROVIDER_REF);
            when(paymentRepository.updateOnSuccess(anyString(), any(), anyString(), anyString(), anyInt(), anyLong()))
                    .thenReturn(0); // version conflict

            assertThatCode(() -> consumer.processPayment(record(0), TOPIC, 1))
                    .doesNotThrowAnyException();
        }

        @Test
        @DisplayName("deliveryAttempt header absent (null) defaults to 1 for first delivery")
        void nullDeliveryAttempt_defaultsToValue1() {
            Payment payment = processingPayment(0, 0L);
            when(paymentRepository.findById(PAYMENT_ID)).thenReturn(Optional.of(payment));
            when(routingEngine.route(PaymentMethod.CARD)).thenReturn(providerAConnector);
            when(providerAConnector.getProviderId()).thenReturn(PROVIDER_A_ID);
            when(providerAConnector.processPayment(PAYMENT_ID, AMOUNT, CURRENCY))
                    .thenReturn(PROVIDER_REF);
            when(paymentRepository.updateOnSuccess(
                    eq(PAYMENT_ID), eq(PaymentStatus.SUCCESS),
                    eq(PROVIDER_A_ID), eq(PROVIDER_REF),
                    eq(1),   // deliveryAttempt=null → defaults to 1
                    eq(0L)))
                    .thenReturn(1);

            assertThatCode(() -> consumer.processPayment(record(0), TOPIC, null))
                    .doesNotThrowAnyException();

            verify(paymentRepository).updateOnSuccess(
                    PAYMENT_ID, PaymentStatus.SUCCESS, PROVIDER_A_ID, PROVIDER_REF, 1, 0L);
        }
    }

    // =========================================================================
    // processPayment — failure / skip paths
    // =========================================================================

    @Nested
    @DisplayName("processPayment — Failure and Skip Paths")
    class FailureAndSkipTests {

        @Test
        @DisplayName("Provider throws ProviderException → updateStatusWithVersionCheck(PROCESSING) called → re-throws")
        void providerFails_updatesProcessingAndRethrows() {
            Payment payment = processingPayment(0, 0L);
            when(paymentRepository.findById(PAYMENT_ID)).thenReturn(Optional.of(payment));
            when(routingEngine.route(PaymentMethod.CARD)).thenReturn(providerAConnector);
            when(providerAConnector.getProviderId()).thenReturn(PROVIDER_A_ID);
            when(providerAConnector.processPayment(PAYMENT_ID, AMOUNT, CURRENCY))
                    .thenThrow(new ProviderException(PROVIDER_A_ID, "504 Gateway Timeout"));
            when(paymentRepository.updateStatusWithVersionCheck(
                    eq(PAYMENT_ID), eq(PaymentStatus.PROCESSING), eq(1), eq(0L)))
                    .thenReturn(1);

            assertThatThrownBy(() -> consumer.processPayment(record(0), TOPIC, 1))
                    .isInstanceOf(ProviderException.class)
                    .hasMessageContaining("504");

            verify(paymentRepository).updateStatusWithVersionCheck(
                    PAYMENT_ID, PaymentStatus.PROCESSING, 1, 0L);
            verify(paymentRepository, never()).updateOnSuccess(
                    anyString(), any(), anyString(), anyString(), anyInt(), anyLong());
        }

        @Test
        @DisplayName("Payment not found in DB → returns immediately without any update or provider call")
        void paymentNotFound_skipsCleanly() {
            when(paymentRepository.findById(PAYMENT_ID)).thenReturn(Optional.empty());

            assertThatCode(() -> consumer.processPayment(record(0), TOPIC, 1))
                    .doesNotThrowAnyException();

            verify(routingEngine, never()).route(any());
            verify(paymentRepository, never()).updateOnSuccess(
                    anyString(), any(), anyString(), anyString(), anyInt(), anyLong());
            verify(paymentRepository, never()).updateStatusWithVersionCheck(
                    anyString(), any(), anyInt(), anyLong());
        }

        @Test
        @DisplayName("Payment already SUCCESS → skips provider call and returns without update")
        void paymentAlreadySuccess_skipsProcessing() {
            Payment success = Payment.builder()
                    .id(PAYMENT_ID).idempotencyKey(IDEM_KEY)
                    .amount(AMOUNT).currency(CURRENCY)
                    .paymentMethod(PaymentMethod.CARD).status(PaymentStatus.SUCCESS)
                    .retryCount(1).version(1L)
                    .createdAt(LocalDateTime.now()).updatedAt(LocalDateTime.now())
                    .build();
            when(paymentRepository.findById(PAYMENT_ID)).thenReturn(Optional.of(success));

            assertThatCode(() -> consumer.processPayment(record(0), TOPIC, 2))
                    .doesNotThrowAnyException();

            verify(routingEngine, never()).route(any());
            verify(paymentRepository, never()).updateOnSuccess(
                    anyString(), any(), anyString(), anyString(), anyInt(), anyLong());
        }

        @Test
        @DisplayName("Payment already FAILED → skips provider call and returns without update")
        void paymentAlreadyFailed_skipsProcessing() {
            Payment failed = Payment.builder()
                    .id(PAYMENT_ID).idempotencyKey(IDEM_KEY)
                    .amount(AMOUNT).currency(CURRENCY)
                    .paymentMethod(PaymentMethod.CARD).status(PaymentStatus.FAILED)
                    .retryCount(4).version(4L)
                    .createdAt(LocalDateTime.now()).updatedAt(LocalDateTime.now())
                    .build();
            when(paymentRepository.findById(PAYMENT_ID)).thenReturn(Optional.of(failed));

            assertThatCode(() -> consumer.processPayment(record(4), TOPIC, 3))
                    .doesNotThrowAnyException();

            verify(routingEngine, never()).route(any());
        }
    }

    // =========================================================================
    // handleDlt
    // =========================================================================

    @Nested
    @DisplayName("handleDlt — Dead Letter Topic Handler")
    class DltHandlerTests {

        private ConsumerRecord<String, PaymentEvent> dltRecord() {
            return new ConsumerRecord<>(TOPIC + "-dlt", 0, 0L, PAYMENT_ID,
                    new PaymentEvent(PAYMENT_ID, AMOUNT, CURRENCY, PaymentMethod.CARD, 4));
        }

        @Test
        @DisplayName("Payment in PROCESSING → marks as FAILED with deliveryAttempt count")
        void processingPayment_markedFailed_withDeliveryAttemptCount() {
            Payment processing = processingPayment(3, 3L);
            when(paymentRepository.findById(PAYMENT_ID)).thenReturn(Optional.of(processing));
            when(paymentRepository.updateStatusWithVersionCheck(
                    eq(PAYMENT_ID), eq(PaymentStatus.FAILED), eq(4), eq(3L)))
                    .thenReturn(1);

            consumer.handleDlt(dltRecord(), null, 4);

            verify(paymentRepository).updateStatusWithVersionCheck(
                    PAYMENT_ID, PaymentStatus.FAILED, 4, 3L);
        }

        @Test
        @DisplayName("Null deliveryAttempt falls back to TOTAL_CONFIGURED_ATTEMPTS (4)")
        void nullDeliveryAttempt_fallsBackToTotalConfiguredAttempts() {
            Payment processing = processingPayment(3, 3L);
            when(paymentRepository.findById(PAYMENT_ID)).thenReturn(Optional.of(processing));
            when(paymentRepository.updateStatusWithVersionCheck(
                    eq(PAYMENT_ID), eq(PaymentStatus.FAILED), eq(4), eq(3L)))
                    .thenReturn(1);

            // Pass null deliveryAttempt — should default to TOTAL_CONFIGURED_ATTEMPTS = 4
            consumer.handleDlt(dltRecord(), null, null);

            verify(paymentRepository).updateStatusWithVersionCheck(
                    PAYMENT_ID, PaymentStatus.FAILED, 4, 3L);
        }

        @Test
        @DisplayName("Payment already SUCCESS in DLT → skips FAILED update (concurrent success wins)")
        void paymentAlreadySuccess_dltSkipsUpdate() {
            Payment success = Payment.builder()
                    .id(PAYMENT_ID).idempotencyKey(IDEM_KEY)
                    .amount(AMOUNT).currency(CURRENCY)
                    .paymentMethod(PaymentMethod.CARD).status(PaymentStatus.SUCCESS)
                    .retryCount(1).version(1L)
                    .createdAt(LocalDateTime.now()).updatedAt(LocalDateTime.now())
                    .build();
            when(paymentRepository.findById(PAYMENT_ID)).thenReturn(Optional.of(success));

            assertThatCode(() -> consumer.handleDlt(dltRecord(), null, 4))
                    .doesNotThrowAnyException();

            verify(paymentRepository, never()).updateStatusWithVersionCheck(
                    anyString(), eq(PaymentStatus.FAILED), anyInt(), anyLong());
        }

        @Test
        @DisplayName("Payment not found in DLT handler → returns without NPE")
        void paymentNotFound_dltReturnsCleanly() {
            when(paymentRepository.findById(PAYMENT_ID)).thenReturn(Optional.empty());

            assertThatCode(() -> consumer.handleDlt(dltRecord(), null, 4))
                    .doesNotThrowAnyException();

            verify(paymentRepository, never()).updateStatusWithVersionCheck(
                    anyString(), any(), anyInt(), anyLong());
        }

        @Test
        @DisplayName("Exception stacktrace header is handled when present")
        void exceptionHeader_handledGracefully() {
            Payment processing = processingPayment(3, 3L);
            when(paymentRepository.findById(PAYMENT_ID)).thenReturn(Optional.of(processing));
            when(paymentRepository.updateStatusWithVersionCheck(anyString(), any(), anyInt(), anyLong()))
                    .thenReturn(1);

            byte[] stackTrace = "java.io.IOException: 504 Timeout\n\tat org.example.Test".getBytes();
            assertThatCode(() -> consumer.handleDlt(dltRecord(), stackTrace, 4))
                    .doesNotThrowAnyException();
        }
    }
}

