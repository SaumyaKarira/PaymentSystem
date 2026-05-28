package org.example.service;

import org.example.dto.CreatePaymentRequest;
import org.example.dto.PaymentEvent;
import org.example.dto.PaymentResponse;
import org.example.entity.Payment;
import org.example.entity.PaymentMethod;
import org.example.entity.PaymentStatus;
import org.example.exception.IdempotencyConflictException;
import org.example.exception.PaymentNotFoundException;
import org.example.exception.ProviderException;
import org.example.provider.ProviderAConnector;
import org.example.provider.ProviderBConnector;
import org.example.repository.PaymentRepository;
import org.example.routing.RoutingEngine;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.util.ReflectionTestUtils;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

// Unit tests for the core orchestration layer.
// All dependencies are mocked with Mockito. Service constructed manually to avoid @InjectMocks ambiguity.
@ExtendWith(MockitoExtension.class)
@DisplayName("PaymentOrchestratorService — Unit Tests")
class PaymentOrchestratorServiceTest {

    @Mock private PaymentRepository paymentRepository;
    @Mock private IdempotencyService idempotencyService;
    @Mock private RoutingEngine routingEngine;
    @Mock private KafkaTemplate<String, PaymentEvent> kafkaTemplate;

    // Typed as concrete classes so RoutingEngine can be constructed with them directly
    @Mock private ProviderAConnector providerAConnector;
    @Mock private ProviderBConnector providerBConnector;

    private PaymentOrchestratorService orchestratorService;

    private static final String IDEMPOTENCY_KEY  = "test-idem-key-abc";
    private static final String PAYMENT_ID       = "550e8400-e29b-41d4-a716-446655440000";
    private static final String PROVIDER_A_ID    = "PROVIDER_A";
    private static final String PROVIDER_B_ID    = "PROVIDER_B";
    private static final String PROVIDER_A_REF   = "PROVA-ABC123DEF456";
    private static final String PROVIDER_B_REF   = "PROVB-XYZ789GHI012";
    private static final String MAIN_TOPIC       = "payment-main-topic";

    @BeforeEach
    void setUp() {
        // Construct the service manually — no @InjectMocks ambiguity
        orchestratorService = new PaymentOrchestratorService(
                paymentRepository, idempotencyService, routingEngine, kafkaTemplate);
        // Inject the @Value field that would normally come from application.yml
        ReflectionTestUtils.setField(orchestratorService, "mainTopic", MAIN_TOPIC);
    }

    // ── Helper builders ───────────────────────────────────────────────────────

    private CreatePaymentRequest cardRequest() {
        return new CreatePaymentRequest(new BigDecimal("150.00"), "USD", PaymentMethod.CARD);
    }

    private CreatePaymentRequest upiRequest() {
        return new CreatePaymentRequest(new BigDecimal("500.00"), "INR", PaymentMethod.UPI);
    }

    // Simulates the Payment entity returned by paymentRepository.save() with version=0 (fresh INSERT)
    private Payment savedInitiatedPayment(PaymentMethod method) {
        return Payment.builder()
                .id(PAYMENT_ID)
                .idempotencyKey(IDEMPOTENCY_KEY)
                .amount(method == PaymentMethod.CARD ? new BigDecimal("150.00") : new BigDecimal("500.00"))
                .currency(method == PaymentMethod.CARD ? "USD" : "INR")
                .paymentMethod(method)
                .status(PaymentStatus.INITIATED)
                .retryCount(0)
                .version(0L)
                .createdAt(LocalDateTime.now())
                .updatedAt(LocalDateTime.now())
                .build();
    }

    private Payment successPayment(PaymentMethod method, String providerId, String providerRef) {
        return Payment.builder()
                .id(PAYMENT_ID)
                .idempotencyKey(IDEMPOTENCY_KEY)
                .amount(method == PaymentMethod.CARD ? new BigDecimal("150.00") : new BigDecimal("500.00"))
                .currency(method == PaymentMethod.CARD ? "USD" : "INR")
                .paymentMethod(method)
                .status(PaymentStatus.SUCCESS)
                .providerId(providerId)
                .providerReferenceId(providerRef)
                .retryCount(0)
                .version(1L)
                .createdAt(LocalDateTime.now())
                .updatedAt(LocalDateTime.now())
                .build();
    }

    private Payment processingPayment(PaymentMethod method, String providerId) {
        return Payment.builder()
                .id(PAYMENT_ID)
                .idempotencyKey(IDEMPOTENCY_KEY)
                .amount(method == PaymentMethod.CARD ? new BigDecimal("150.00") : new BigDecimal("500.00"))
                .currency(method == PaymentMethod.CARD ? "USD" : "INR")
                .paymentMethod(method)
                .status(PaymentStatus.PROCESSING)
                .providerId(providerId)
                .retryCount(0)
                .version(1L)
                .createdAt(LocalDateTime.now())
                .updatedAt(LocalDateTime.now())
                .build();
    }

    // =========================================================================
    // CATEGORY A — Positive / Happy Path
    // =========================================================================

    @Nested
    @DisplayName("Category A — Happy Path Scenarios")
    class HappyPathTests {

        // CARD payment → ProviderA succeeds → MySQL SUCCESS → response returned
        @Test
        @DisplayName("CARD routes to ProviderA, succeeds, returns SUCCESS response")
        void cardPaymentSuccessViaProviderA() {
            Payment saved    = savedInitiatedPayment(PaymentMethod.CARD);
            Payment success  = successPayment(PaymentMethod.CARD, PROVIDER_A_ID, PROVIDER_A_REF);

            when(idempotencyService.acquireInFlightLock(IDEMPOTENCY_KEY)).thenReturn(true);
            when(paymentRepository.save(any(Payment.class))).thenReturn(saved);
            when(routingEngine.route(PaymentMethod.CARD)).thenReturn(providerAConnector);
            when(providerAConnector.getProviderId()).thenReturn(PROVIDER_A_ID);
            when(providerAConnector.processPayment(PAYMENT_ID, saved.getAmount(), "USD"))
                    .thenReturn(PROVIDER_A_REF);
            when(paymentRepository.updateOnSuccess(
                    eq(PAYMENT_ID), eq(PaymentStatus.SUCCESS),
                    eq(PROVIDER_A_ID), eq(PROVIDER_A_REF), eq(0), eq(0L)))
                    .thenReturn(1);
            when(paymentRepository.findById(PAYMENT_ID)).thenReturn(Optional.of(success));

            PaymentResponse response = orchestratorService.createPayment(cardRequest(), IDEMPOTENCY_KEY);

            assertThat(response.status()).isEqualTo(PaymentStatus.SUCCESS);
            assertThat(response.providerId()).isEqualTo(PROVIDER_A_ID);
            assertThat(response.providerReferenceId()).isEqualTo(PROVIDER_A_REF);
            assertThat(response.paymentMethod()).isEqualTo(PaymentMethod.CARD);

            verify(routingEngine, times(1)).route(PaymentMethod.CARD);
            verify(providerAConnector, times(1)).processPayment(PAYMENT_ID, saved.getAmount(), "USD");
            verify(providerBConnector, never()).processPayment(anyString(), any(), anyString());
            verify(kafkaTemplate, never()).send(anyString(), anyString(), any(PaymentEvent.class));

            ArgumentCaptor<PaymentResponse> cacheCaptor = ArgumentCaptor.forClass(PaymentResponse.class);
            verify(idempotencyService).storeCompletedResponse(eq(IDEMPOTENCY_KEY), cacheCaptor.capture());
            assertThat(cacheCaptor.getValue().status()).isEqualTo(PaymentStatus.SUCCESS);
        }

        // UPI payment → ProviderB succeeds → ProviderA never called
        @Test
        @DisplayName("UPI routes to ProviderB, succeeds, ProviderA never called")
        void upiPaymentSuccessViaProviderB() {
            Payment saved   = savedInitiatedPayment(PaymentMethod.UPI);
            Payment success = successPayment(PaymentMethod.UPI, PROVIDER_B_ID, PROVIDER_B_REF);

            when(idempotencyService.acquireInFlightLock(IDEMPOTENCY_KEY)).thenReturn(true);
            when(paymentRepository.save(any(Payment.class))).thenReturn(saved);
            when(routingEngine.route(PaymentMethod.UPI)).thenReturn(providerBConnector);
            when(providerBConnector.getProviderId()).thenReturn(PROVIDER_B_ID);
            when(providerBConnector.processPayment(PAYMENT_ID, saved.getAmount(), "INR"))
                    .thenReturn(PROVIDER_B_REF);
            when(paymentRepository.updateOnSuccess(
                    eq(PAYMENT_ID), eq(PaymentStatus.SUCCESS),
                    eq(PROVIDER_B_ID), eq(PROVIDER_B_REF), eq(0), eq(0L)))
                    .thenReturn(1);
            when(paymentRepository.findById(PAYMENT_ID)).thenReturn(Optional.of(success));

            PaymentResponse response = orchestratorService.createPayment(upiRequest(), IDEMPOTENCY_KEY);

            assertThat(response.status()).isEqualTo(PaymentStatus.SUCCESS);
            assertThat(response.providerId()).isEqualTo(PROVIDER_B_ID);
            verify(providerAConnector, never()).processPayment(anyString(), any(), anyString());
            verify(kafkaTemplate, never()).send(anyString(), anyString(), any(PaymentEvent.class));
        }

        // GET payment fetches real-time MySQL state
        @Test
        @DisplayName("GET payment returns real-time status, retryCount, version from MySQL")
        void getPaymentFetchesRealTimeState() {
            Payment processing = Payment.builder()
                    .id(PAYMENT_ID).idempotencyKey(IDEMPOTENCY_KEY)
                    .amount(new BigDecimal("150.00")).currency("USD")
                    .paymentMethod(PaymentMethod.CARD).status(PaymentStatus.PROCESSING)
                    .providerId(PROVIDER_A_ID).retryCount(2).version(2L)
                    .createdAt(LocalDateTime.now().minusMinutes(5))
                    .updatedAt(LocalDateTime.now().minusSeconds(4))
                    .build();

            when(paymentRepository.findById(PAYMENT_ID)).thenReturn(Optional.of(processing));

            PaymentResponse response = orchestratorService.getPayment(PAYMENT_ID);

            assertThat(response.status()).isEqualTo(PaymentStatus.PROCESSING);
            assertThat(response.retryCount()).isEqualTo(2);
            assertThat(response.providerReferenceId()).isNull();
            verify(paymentRepository, times(1)).findById(PAYMENT_ID);
            verify(idempotencyService, never()).getCachedResponse(anyString());
        }

        // Duplicate request with completed key → returns cached response, zero processing
        @Test
        @DisplayName("Idempotency cache hit returns cached response without any DB/provider calls")
        void idempotencyCacheHitBypassesAllLayers() {
            PaymentResponse cached = new PaymentResponse(
                    PAYMENT_ID, IDEMPOTENCY_KEY, new BigDecimal("150.00"), "USD",
                    PaymentMethod.CARD, PaymentStatus.SUCCESS,
                    PROVIDER_A_ID, PROVIDER_A_REF, 0, LocalDateTime.now(), LocalDateTime.now());

            when(idempotencyService.acquireInFlightLock(IDEMPOTENCY_KEY)).thenReturn(false);
            when(idempotencyService.getCachedResponse(IDEMPOTENCY_KEY)).thenReturn(Optional.of(cached));

            PaymentResponse response = orchestratorService.createPayment(cardRequest(), IDEMPOTENCY_KEY);

            assertThat(response).isEqualTo(cached);
            verify(paymentRepository, never()).save(any(Payment.class));
            verify(routingEngine, never()).route(any(PaymentMethod.class));
            verify(kafkaTemplate, never()).send(anyString(), anyString(), any(PaymentEvent.class));
        }
    }

    // =========================================================================
    // CATEGORY B — Negative / Error Boundaries
    // =========================================================================

    @Nested
    @DisplayName("Category B — Negative and Error Boundary Scenarios")
    class NegativeTests {

        // In-flight lock not acquired, no cached response → IdempotencyConflictException
        @Test
        @DisplayName("In-flight key with no cached value throws IdempotencyConflictException")
        void inFlightKeyThrowsConflict() {
            when(idempotencyService.acquireInFlightLock(IDEMPOTENCY_KEY)).thenReturn(false);
            when(idempotencyService.getCachedResponse(IDEMPOTENCY_KEY)).thenReturn(Optional.empty());

            assertThatThrownBy(() -> orchestratorService.createPayment(cardRequest(), IDEMPOTENCY_KEY))
                    .isInstanceOf(IdempotencyConflictException.class)
                    .hasMessageContaining(IDEMPOTENCY_KEY);

            verify(paymentRepository, never()).save(any(Payment.class));
            verify(kafkaTemplate, never()).send(anyString(), anyString(), any(PaymentEvent.class));
        }

        // GET with non-existent ID throws PaymentNotFoundException
        @Test
        @DisplayName("TC-08: GET with unknown UUID throws PaymentNotFoundException")
        void unknownIdThrowsNotFoundException() {
            String unknownId = "00000000-0000-0000-0000-000000000000";
            when(paymentRepository.findById(unknownId)).thenReturn(Optional.empty());

            assertThatThrownBy(() -> orchestratorService.getPayment(unknownId))
                    .isInstanceOf(PaymentNotFoundException.class)
                    .hasMessageContaining(unknownId);
        }

        // Redis outage propagates exception and releases key
        @Test
        @DisplayName("Redis connection failure propagates exception without touching DB")
        void redisOutagePropagatesException() {
            when(idempotencyService.acquireInFlightLock(IDEMPOTENCY_KEY))
                    .thenThrow(new org.springframework.data.redis.RedisConnectionFailureException("Redis down"));

            assertThatThrownBy(() -> orchestratorService.createPayment(cardRequest(), IDEMPOTENCY_KEY))
                    .isInstanceOf(org.springframework.data.redis.RedisConnectionFailureException.class);

            verify(paymentRepository, never()).save(any(Payment.class));
        }
    }

    // =========================================================================
    // CATEGORY C — Resiliency: Kafka hand-off
    // =========================================================================

    @Nested
    @DisplayName("Category C — Resiliency and Kafka Hand-Off")
    class ResiliencyTests {

        // Provider throws 504 → PROCESSING in MySQL → event on Kafka → client gets PROCESSING immediately
        @Test
        @DisplayName("Provider 504 → PROCESSING in MySQL → event on Kafka → 201 PROCESSING")
        void providerFailureTransitionsToProcessingAndPublishesToKafka() {
            Payment saved       = savedInitiatedPayment(PaymentMethod.CARD);
            Payment processing  = processingPayment(PaymentMethod.CARD, PROVIDER_A_ID);

            when(idempotencyService.acquireInFlightLock(IDEMPOTENCY_KEY)).thenReturn(true);
            when(paymentRepository.save(any(Payment.class))).thenReturn(saved);
            when(routingEngine.route(PaymentMethod.CARD)).thenReturn(providerAConnector);
            when(providerAConnector.getProviderId()).thenReturn(PROVIDER_A_ID);
            when(providerAConnector.processPayment(PAYMENT_ID, saved.getAmount(), "USD"))
                    .thenThrow(new ProviderException(PROVIDER_A_ID, "504 Gateway Timeout"));
            when(paymentRepository.updateStatusWithVersionCheck(
                    eq(PAYMENT_ID), eq(PaymentStatus.PROCESSING), eq(0), eq(0L)))
                    .thenReturn(1);
            when(paymentRepository.findById(PAYMENT_ID)).thenReturn(Optional.of(processing));

            PaymentResponse response = orchestratorService.createPayment(cardRequest(), IDEMPOTENCY_KEY);

            // Client gets PROCESSING immediately — Kafka will retry asynchronously
            assertThat(response.status()).isEqualTo(PaymentStatus.PROCESSING);
            assertThat(response.providerReferenceId()).isNull();

            // MySQL was updated to PROCESSING
            verify(paymentRepository).updateStatusWithVersionCheck(
                    PAYMENT_ID, PaymentStatus.PROCESSING, 0, 0L);

            // Exactly one Kafka message published to the main topic
            ArgumentCaptor<PaymentEvent> eventCaptor = ArgumentCaptor.forClass(PaymentEvent.class);
            ArgumentCaptor<String> topicCaptor = ArgumentCaptor.forClass(String.class);
            ArgumentCaptor<String> keyCaptor = ArgumentCaptor.forClass(String.class);
            verify(kafkaTemplate, times(1)).send(topicCaptor.capture(), keyCaptor.capture(), eventCaptor.capture());

            assertThat(topicCaptor.getValue()).isEqualTo(MAIN_TOPIC);
            assertThat(keyCaptor.getValue()).isEqualTo(PAYMENT_ID);
            assertThat(eventCaptor.getValue().paymentId()).isEqualTo(PAYMENT_ID);
            assertThat(eventCaptor.getValue().paymentMethod()).isEqualTo(PaymentMethod.CARD);

            // SUCCESS path must NOT have been called
            verify(paymentRepository, never())
                    .updateOnSuccess(anyString(), any(), anyString(), anyString(), anyInt(), anyLong());
        }

        // DLT handler marks payment FAILED in MySQL after all retries exhausted
        @Test
        @DisplayName("DLT handler marks payment as FAILED with correct version guard")
        void dltHandlerMarksPaymentFailed() {
            org.example.kafka.PaymentRetryConsumer retryConsumer =
                    new org.example.kafka.PaymentRetryConsumer(paymentRepository, routingEngine);

            Payment exhausted = Payment.builder()
                    .id(PAYMENT_ID).idempotencyKey(IDEMPOTENCY_KEY)
                    .amount(new BigDecimal("150.00")).currency("USD")
                    .paymentMethod(PaymentMethod.CARD).status(PaymentStatus.PROCESSING)
                    .retryCount(4).version(4L)
                    .createdAt(LocalDateTime.now()).updatedAt(LocalDateTime.now())
                    .build();

            when(paymentRepository.findById(PAYMENT_ID)).thenReturn(Optional.of(exhausted));
            when(paymentRepository.updateStatusWithVersionCheck(
                    eq(PAYMENT_ID), eq(PaymentStatus.FAILED), eq(4), eq(4L)))
                    .thenReturn(1);

            org.apache.kafka.clients.consumer.ConsumerRecord<String, PaymentEvent> dltRecord =
                    new org.apache.kafka.clients.consumer.ConsumerRecord<>(
                            MAIN_TOPIC + "-dlt", 0, 0L, PAYMENT_ID,
                            new PaymentEvent(PAYMENT_ID, new BigDecimal("150.00"), "USD",
                                    PaymentMethod.CARD, 4));

            retryConsumer.handleDlt(dltRecord,
                    "java.io.IOException: 504\n\tat org.example...".getBytes(),
                    null);  // null deliveryAttempt → falls back to TOTAL_CONFIGURED_ATTEMPTS (4) inside handleDlt

            verify(paymentRepository).updateStatusWithVersionCheck(
                    PAYMENT_ID, PaymentStatus.FAILED, 4, 4L);
            verify(providerAConnector, never()).processPayment(anyString(), any(), anyString());
            verify(providerBConnector, never()).processPayment(anyString(), any(), anyString());
        }
    }
}
