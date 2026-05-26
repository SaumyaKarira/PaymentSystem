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

/**
 * PaymentOrchestratorServiceTest — unit tests for the core orchestration layer.
 *
 * <p>All external dependencies are mocked with Mockito. The service under test is
 * constructed manually so we control every collaborator precisely.
 *
 * <p>Why NOT @InjectMocks: PaymentOrchestratorService's constructor now takes exactly
 * 4 args (PaymentRepository, IdempotencyService, RoutingEngine, KafkaTemplate).
 * We construct it explicitly below so there is no ambiguity about which mock goes where.
 */
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

    /**
     * Simulates the Payment entity returned by paymentRepository.save().
     * version=0L because it is a fresh INSERT — Hibernate sets it to 0.
     */
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

        /**
         * TC-01: CARD payment → ProviderA succeeds → MySQL SUCCESS → 201
         */
        @Test
        @DisplayName("TC-01: CARD routes to ProviderA, succeeds, returns SUCCESS response")
        void tc01_cardPaymentSuccessViaProviderA() {
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

        /**
         * TC-02: UPI payment → ProviderB succeeds → ProviderA never called
         */
        @Test
        @DisplayName("TC-02: UPI routes to ProviderB, succeeds, ProviderA never called")
        void tc02_upiPaymentSuccessViaProviderB() {
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

        /**
         * TC-03: GET payment fetches real-time MySQL state
         */
        @Test
        @DisplayName("TC-03: GET payment returns real-time status, retryCount, version from MySQL")
        void tc03_getPaymentFetchesRealTimeState() {
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

        /**
         * TC-04: Duplicate request with completed key → returns cached response, zero processing
         */
        @Test
        @DisplayName("TC-04: Idempotency cache hit returns cached response without any DB/provider calls")
        void tc04_idempotencyCacheHitBypassesAllLayers() {
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

        /**
         * TC-06: In-flight lock not acquired, no cached response → IdempotencyConflictException
         */
        @Test
        @DisplayName("TC-06: In-flight key with no cached value throws IdempotencyConflictException")
        void tc06_inFlightKeyThrowsConflict() {
            when(idempotencyService.acquireInFlightLock(IDEMPOTENCY_KEY)).thenReturn(false);
            when(idempotencyService.getCachedResponse(IDEMPOTENCY_KEY)).thenReturn(Optional.empty());

            assertThatThrownBy(() -> orchestratorService.createPayment(cardRequest(), IDEMPOTENCY_KEY))
                    .isInstanceOf(IdempotencyConflictException.class)
                    .hasMessageContaining(IDEMPOTENCY_KEY);

            verify(paymentRepository, never()).save(any(Payment.class));
            verify(kafkaTemplate, never()).send(anyString(), anyString(), any(PaymentEvent.class));
        }

        /**
         * TC-08: GET with non-existent ID throws PaymentNotFoundException
         */
        @Test
        @DisplayName("TC-08: GET with unknown UUID throws PaymentNotFoundException")
        void tc08_unknownIdThrowsNotFoundException() {
            String unknownId = "00000000-0000-0000-0000-000000000000";
            when(paymentRepository.findById(unknownId)).thenReturn(Optional.empty());

            assertThatThrownBy(() -> orchestratorService.getPayment(unknownId))
                    .isInstanceOf(PaymentNotFoundException.class)
                    .hasMessageContaining(unknownId);
        }

        /**
         * TC-09: Redis outage propagates exception and releases key
         */
        @Test
        @DisplayName("TC-09: Redis connection failure propagates exception without touching DB")
        void tc09_redisOutagePropagatesException() {
            when(idempotencyService.acquireInFlightLock(IDEMPOTENCY_KEY))
                    .thenThrow(new org.springframework.data.redis.RedisConnectionFailureException("Redis down"));

            assertThatThrownBy(() -> orchestratorService.createPayment(cardRequest(), IDEMPOTENCY_KEY))
                    .isInstanceOf(org.springframework.data.redis.RedisConnectionFailureException.class);

            verify(paymentRepository, never()).save(any(Payment.class));
        }
    }

    // =========================================================================
    // CATEGORY C — Resiliency: Kafka hand-off and failover routing
    // =========================================================================

    @Nested
    @DisplayName("Category C — Resiliency, Kafka Hand-Off, and Failover")
    class ResiliencyTests {

        /**
         * TC-10A: Provider throws 504 → status transitions to PROCESSING →
         * event published to Kafka → client receives PROCESSING response immediately
         */
        @Test
        @DisplayName("TC-10A: Provider 504 → PROCESSING in MySQL → event on Kafka → 201 PROCESSING")
        void tc10a_providerFailureTransitionsToProcessingAndPublishesToKafka() {
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

        /**
         * TC-10B: RoutingEngine correctly switches to failover provider
         */
        @Test
        @DisplayName("TC-10B: RoutingEngine returns ProviderB for CARD failover, ProviderA for UPI failover")
        void tc10b_routingEngineFailoverSwitchesProvider() {
            // Use a real RoutingEngine instance with the mocked concrete connectors
            RoutingEngine realRouter = new RoutingEngine(providerAConnector, providerBConnector);

            assertThat(realRouter.route(PaymentMethod.CARD, false))
                    .as("CARD primary → ProviderA").isSameAs(providerAConnector);
            assertThat(realRouter.route(PaymentMethod.CARD, true))
                    .as("CARD failover → ProviderB").isSameAs(providerBConnector);
            assertThat(realRouter.route(PaymentMethod.UPI, false))
                    .as("UPI primary → ProviderB").isSameAs(providerBConnector);
            assertThat(realRouter.route(PaymentMethod.UPI, true))
                    .as("UPI failover → ProviderA").isSameAs(providerAConnector);
        }

        /**
         * TC-11: Kafka retry consumer uses failover provider on attempt #2 and succeeds
         */
        @Test
        @DisplayName("TC-11: Kafka consumer attempt #2 uses failover ProviderB and marks SUCCESS")
        void tc11_kafkaConsumerUsesFailoverOnAttempt2() {
            RoutingEngine realRouter = new RoutingEngine(providerAConnector, providerBConnector);
            org.example.kafka.PaymentRetryConsumer retryConsumer =
                    new org.example.kafka.PaymentRetryConsumer(paymentRepository, realRouter);

            Payment paymentInProcessing = Payment.builder()
                    .id(PAYMENT_ID).idempotencyKey(IDEMPOTENCY_KEY)
                    .amount(new BigDecimal("150.00")).currency("USD")
                    .paymentMethod(PaymentMethod.CARD).status(PaymentStatus.PROCESSING)
                    .providerId(PROVIDER_A_ID).retryCount(1).version(1L)
                    .createdAt(LocalDateTime.now()).updatedAt(LocalDateTime.now())
                    .build();

            when(paymentRepository.findById(PAYMENT_ID)).thenReturn(Optional.of(paymentInProcessing));
            when(providerBConnector.getProviderId()).thenReturn(PROVIDER_B_ID);
            when(providerBConnector.processPayment(PAYMENT_ID, new BigDecimal("150.00"), "USD"))
                    .thenReturn(PROVIDER_B_REF);
            when(paymentRepository.updateOnSuccess(
                    eq(PAYMENT_ID), eq(PaymentStatus.SUCCESS),
                    eq(PROVIDER_B_ID), eq(PROVIDER_B_REF), eq(2), eq(1L)))
                    .thenReturn(1);

            org.apache.kafka.clients.consumer.ConsumerRecord<String, PaymentEvent> record =
                    new org.apache.kafka.clients.consumer.ConsumerRecord<>(
                            MAIN_TOPIC, 0, 0L, PAYMENT_ID,
                            new PaymentEvent(PAYMENT_ID, new BigDecimal("150.00"), "USD",
                                    PaymentMethod.CARD, 1, 1L));

            retryConsumer.processPayment(record, MAIN_TOPIC, 2);

            // ProviderA never called on attempt #2 (failover=true → ProviderB)
            verify(providerAConnector, never()).processPayment(anyString(), any(), anyString());
            verify(providerBConnector, times(1))
                    .processPayment(PAYMENT_ID, new BigDecimal("150.00"), "USD");
            verify(paymentRepository).updateOnSuccess(
                    PAYMENT_ID, PaymentStatus.SUCCESS, PROVIDER_B_ID, PROVIDER_B_REF, 2, 1L);
        }

        /**
         * TC-DLT: DLT handler marks payment FAILED in MySQL after all retries exhausted
         */
        @Test
        @DisplayName("TC-DLT: DLT handler marks payment as FAILED with correct version guard")
        void tcDlt_dltHandlerMarksPaymentFailed() {
            RoutingEngine realRouter = new RoutingEngine(providerAConnector, providerBConnector);
            org.example.kafka.PaymentRetryConsumer retryConsumer =
                    new org.example.kafka.PaymentRetryConsumer(paymentRepository, realRouter);

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
                                    PaymentMethod.CARD, 4, 4L));

            retryConsumer.handleDlt(dltRecord,
                    "java.io.IOException: 504\n\tat org.example...".getBytes());

            verify(paymentRepository).updateStatusWithVersionCheck(
                    PAYMENT_ID, PaymentStatus.FAILED, 4, 4L);
            verify(providerAConnector, never()).processPayment(anyString(), any(), anyString());
            verify(providerBConnector, never()).processPayment(anyString(), any(), anyString());
        }
    }
}
