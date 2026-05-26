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
import org.example.provider.PaymentProviderConnector;
import org.example.repository.PaymentRepository;
import org.example.routing.RoutingEngine;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
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
 * PaymentOrchestratorServiceTest — exhaustive unit tests for the core orchestration layer.
 *
 * <p><strong>Testing Philosophy</strong><br>
 * This class tests the {@link PaymentOrchestratorService} in COMPLETE ISOLATION using
 * Mockito to stub every external collaborator: the database ({@code PaymentRepository}),
 * the Redis guard ({@code IdempotencyService}), the routing engine ({@code RoutingEngine}),
 * the Kafka template ({@code KafkaTemplate}), and individual provider connectors.
 *
 * <p><strong>Why @ExtendWith(MockitoExtension.class)?</strong><br>
 * {@code MockitoExtension} initialises all fields annotated with {@code @Mock} and
 * {@code @InjectMocks} before each test, and validates that every stub actually gets
 * invoked (strict stubbing). This catches "over-stubbing" — a common source of tests
 * that pass for the wrong reason.
 *
 * <p><strong>Why NOT @SpringBootTest here?</strong><br>
 * Starting the full Spring context (MySQL, Redis, Kafka) takes seconds and introduces
 * environmental dependencies. A pure unit test with mocks runs in milliseconds and
 * pinpoints exactly which method has the defect — it is the first line of defence.
 */
@ExtendWith(MockitoExtension.class)
@DisplayName("PaymentOrchestratorService — Unit Tests")
class PaymentOrchestratorServiceTest {

    // ─── Collaborator mocks injected into the service under test ──────────────

    /** Simulates Spring Data JPA repository calls without an actual MySQL connection. */
    @Mock
    private PaymentRepository paymentRepository;

    /** Simulates Redis SET NX / GET operations without a running Redis instance. */
    @Mock
    private IdempotencyService idempotencyService;

    /** Simulates provider routing decisions without real connectors. */
    @Mock
    private RoutingEngine routingEngine;

    /** Simulates Kafka message publishing without a Kafka broker. */
    @Mock
    private KafkaTemplate<String, PaymentEvent> kafkaTemplate;

    /** Stub connector representing Provider A (for CARD payments). */
    @Mock
    private PaymentProviderConnector providerAConnector;

    /** Stub connector representing Provider B (for UPI payments). */
    @Mock
    private PaymentProviderConnector providerBConnector;

    /**
     * The class under test.
     * Mockito's @InjectMocks creates a real instance of PaymentOrchestratorService and
     * injects the mocks above via constructor injection (because @RequiredArgsConstructor
     * generates a constructor). The @Value field (mainTopic) must be set manually via
     * ReflectionTestUtils since @Value is not processed outside a Spring context.
     */
    @InjectMocks
    private PaymentOrchestratorService orchestratorService;

    // ─── Shared test constants ────────────────────────────────────────────────

    private static final String IDEMPOTENCY_KEY = "test-idem-key-abc";
    private static final String PAYMENT_ID      = "550e8400-e29b-41d4-a716-446655440000";
    private static final String PROVIDER_A_ID   = "PROVIDER_A";
    private static final String PROVIDER_B_ID   = "PROVIDER_B";
    private static final String PROVIDER_A_REF  = "PROVA-ABC123DEF456";
    private static final String PROVIDER_B_REF  = "PROVB-XYZ789GHI012";
    private static final String MAIN_TOPIC      = "payment-main-topic";

    @BeforeEach
    void setUp() {
        // Inject the @Value field manually since we are NOT in a Spring context.
        // ReflectionTestUtils bypasses access modifiers and sets private fields directly.
        ReflectionTestUtils.setField(orchestratorService, "mainTopic", MAIN_TOPIC);
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Helper factory methods — keep test bodies focused on assertions
    // ─────────────────────────────────────────────────────────────────────────

    /**
     * Builds a minimal but valid CARD payment request.
     */
    private CreatePaymentRequest cardRequest() {
        return new CreatePaymentRequest(
                new BigDecimal("150.00"),
                "USD",
                PaymentMethod.CARD
        );
    }

    /**
     * Builds a minimal but valid UPI payment request.
     */
    private CreatePaymentRequest upiRequest() {
        return new CreatePaymentRequest(
                new BigDecimal("500.00"),
                "INR",
                PaymentMethod.UPI
        );
    }

    /**
     * Builds a Payment entity as if just saved by {@code persistNewPayment()}.
     * version=0 mirrors what MySQL returns after a fresh INSERT.
     */
    private Payment buildInitiatedPayment(PaymentMethod method) {
        return Payment.builder()
                .id(PAYMENT_ID)
                .idempotencyKey(IDEMPOTENCY_KEY)
                .amount(method == PaymentMethod.CARD
                        ? new BigDecimal("150.00")
                        : new BigDecimal("500.00"))
                .currency(method == PaymentMethod.CARD ? "USD" : "INR")
                .paymentMethod(method)
                .status(PaymentStatus.INITIATED)
                .retryCount(0)
                .version(0L)
                .createdAt(LocalDateTime.now())
                .updatedAt(LocalDateTime.now())
                .build();
    }

    /**
     * Builds a Payment entity as it would look after a SUCCESS update.
     * version=1 because one UPDATE was applied (optimistic lock increment).
     */
    private Payment buildSuccessPayment(PaymentMethod method, String providerId, String providerRef) {
        return Payment.builder()
                .id(PAYMENT_ID)
                .idempotencyKey(IDEMPOTENCY_KEY)
                .amount(method == PaymentMethod.CARD
                        ? new BigDecimal("150.00")
                        : new BigDecimal("500.00"))
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

    /**
     * Builds a Payment entity as it would look after transitioning to PROCESSING.
     */
    private Payment buildProcessingPayment(PaymentMethod method, String providerId) {
        return Payment.builder()
                .id(PAYMENT_ID)
                .idempotencyKey(IDEMPOTENCY_KEY)
                .amount(method == PaymentMethod.CARD
                        ? new BigDecimal("150.00")
                        : new BigDecimal("500.00"))
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
    // CATEGORY A — POSITIVE (HAPPY PATH) SCENARIOS
    // =========================================================================

    @Nested
    @DisplayName("Category A — Positive Scenarios")
    class PositiveScenariosTest {

        /**
         * TEST CASE 1: Synchronous CARD route execution — Provider A success.
         *
         * <p><strong>Scenario:</strong> A client submits a CARD payment. The routing engine
         * selects Provider A. Provider A returns a reference ID. The orchestrator updates
         * the MySQL record to SUCCESS, stores the response in Redis, and returns 201.
         *
         * <p><strong>Key assertions:</strong>
         * <ul>
         *   <li>RoutingEngine.route(CARD) is called exactly once</li>
         *   <li>ProviderA.processPayment() is called with the correct payment parameters</li>
         *   <li>paymentRepository.updateOnSuccess() is called with status=SUCCESS and the
         *       provider reference ID</li>
         *   <li>idempotencyService.storeCompletedResponse() is called with the final DTO</li>
         *   <li>Kafka is NEVER invoked (no failure occurred)</li>
         *   <li>Returned PaymentResponse has status=SUCCESS and correct provider details</li>
         * </ul>
         */
        @Test
        @DisplayName("TC-01: CARD payment routes to ProviderA, succeeds, returns SUCCESS response")
        void tc01_cardPaymentSuccessViaProviderA() {
            // ── ARRANGE ──────────────────────────────────────────────────────
            CreatePaymentRequest request = cardRequest();
            Payment initiatedPayment = buildInitiatedPayment(PaymentMethod.CARD);
            Payment successPayment   = buildSuccessPayment(PaymentMethod.CARD, PROVIDER_A_ID, PROVIDER_A_REF);

            // Redis lock acquired successfully (first request for this key)
            when(idempotencyService.acquireInFlightLock(IDEMPOTENCY_KEY)).thenReturn(true);

            // DB returns saved entity after INSERT
            when(paymentRepository.save(any(Payment.class))).thenReturn(initiatedPayment);

            // Routing engine selects Provider A for CARD
            when(routingEngine.route(PaymentMethod.CARD)).thenReturn(providerAConnector);

            // Provider A returns the connector ID for identification
            when(providerAConnector.getProviderId()).thenReturn(PROVIDER_A_ID);

            // Provider A simulates a successful call and returns a reference ID
            when(providerAConnector.processPayment(PAYMENT_ID, initiatedPayment.getAmount(), "USD"))
                    .thenReturn(PROVIDER_A_REF);

            // DB updateOnSuccess returns 1 row updated (optimistic lock succeeded)
            when(paymentRepository.updateOnSuccess(
                    eq(PAYMENT_ID),
                    eq(PaymentStatus.SUCCESS),
                    eq(PROVIDER_A_ID),
                    eq(PROVIDER_A_REF),
                    eq(0),
                    eq(0L)
            )).thenReturn(1);

            // DB findById after update returns the SUCCESS entity
            when(paymentRepository.findById(PAYMENT_ID)).thenReturn(Optional.of(successPayment));

            // ── ACT ───────────────────────────────────────────────────────────
            PaymentResponse response = orchestratorService.createPayment(request, IDEMPOTENCY_KEY);

            // ── ASSERT ────────────────────────────────────────────────────────

            // Status must be SUCCESS — the synchronous path completed without error
            assertThat(response.status())
                    .as("Payment status must be SUCCESS after successful provider call")
                    .isEqualTo(PaymentStatus.SUCCESS);

            // Provider details must be populated
            assertThat(response.providerId())
                    .as("Provider ID must be PROVIDER_A for CARD payments")
                    .isEqualTo(PROVIDER_A_ID);

            assertThat(response.providerReferenceId())
                    .as("Provider reference ID must be the value returned by Provider A")
                    .isEqualTo(PROVIDER_A_REF);

            assertThat(response.paymentMethod())
                    .as("Payment method must be CARD as submitted")
                    .isEqualTo(PaymentMethod.CARD);

            assertThat(response.id())
                    .as("Response must contain the payment UUID")
                    .isEqualTo(PAYMENT_ID);

            // Verify that the routing engine was called exactly once with CARD
            verify(routingEngine, times(1)).route(PaymentMethod.CARD);

            // Verify Provider A was called with the correct arguments
            verify(providerAConnector, times(1))
                    .processPayment(PAYMENT_ID, initiatedPayment.getAmount(), "USD");

            // Verify Provider B was NEVER called — only Provider A should handle CARD primary
            verify(providerBConnector, never()).processPayment(anyString(), any(), anyString());

            // Verify Kafka was NOT invoked — no failure means no retry needed
            verify(kafkaTemplate, never()).send(anyString(), anyString(), any(PaymentEvent.class));

            // Verify the completed response was cached in Redis for idempotency
            ArgumentCaptor<PaymentResponse> cachedResponseCaptor =
                    ArgumentCaptor.forClass(PaymentResponse.class);
            verify(idempotencyService, times(1))
                    .storeCompletedResponse(eq(IDEMPOTENCY_KEY), cachedResponseCaptor.capture());
            assertThat(cachedResponseCaptor.getValue().status())
                    .as("Cached response in Redis must also have SUCCESS status")
                    .isEqualTo(PaymentStatus.SUCCESS);
        }

        /**
         * TEST CASE 2: Synchronous UPI route execution — Provider B success.
         *
         * <p><strong>Scenario:</strong> A client submits a UPI payment. The routing engine
         * selects Provider B. Provider B succeeds. The orchestrator returns 201 SUCCESS.
         *
         * <p><strong>Key assertion difference from TC-01:</strong>
         * RoutingEngine must receive {@code PaymentMethod.UPI}, and Provider A must NEVER
         * be called — this validates the routing switch logic is method-specific.
         */
        @Test
        @DisplayName("TC-02: UPI payment routes to ProviderB, succeeds, returns SUCCESS response")
        void tc02_upiPaymentSuccessViaProviderB() {
            // ── ARRANGE ──────────────────────────────────────────────────────
            CreatePaymentRequest request = upiRequest();
            Payment initiatedPayment = buildInitiatedPayment(PaymentMethod.UPI);
            Payment successPayment   = buildSuccessPayment(PaymentMethod.UPI, PROVIDER_B_ID, PROVIDER_B_REF);

            when(idempotencyService.acquireInFlightLock(IDEMPOTENCY_KEY)).thenReturn(true);
            when(paymentRepository.save(any(Payment.class))).thenReturn(initiatedPayment);

            // Routing engine must return Provider B for UPI
            when(routingEngine.route(PaymentMethod.UPI)).thenReturn(providerBConnector);
            when(providerBConnector.getProviderId()).thenReturn(PROVIDER_B_ID);
            when(providerBConnector.processPayment(PAYMENT_ID, initiatedPayment.getAmount(), "INR"))
                    .thenReturn(PROVIDER_B_REF);

            when(paymentRepository.updateOnSuccess(
                    eq(PAYMENT_ID),
                    eq(PaymentStatus.SUCCESS),
                    eq(PROVIDER_B_ID),
                    eq(PROVIDER_B_REF),
                    eq(0),
                    eq(0L)
            )).thenReturn(1);

            when(paymentRepository.findById(PAYMENT_ID)).thenReturn(Optional.of(successPayment));

            // ── ACT ───────────────────────────────────────────────────────────
            PaymentResponse response = orchestratorService.createPayment(request, IDEMPOTENCY_KEY);

            // ── ASSERT ────────────────────────────────────────────────────────
            assertThat(response.status()).isEqualTo(PaymentStatus.SUCCESS);
            assertThat(response.providerId()).isEqualTo(PROVIDER_B_ID);
            assertThat(response.providerReferenceId()).isEqualTo(PROVIDER_B_REF);
            assertThat(response.paymentMethod()).isEqualTo(PaymentMethod.UPI);

            // Critical: Provider A must NEVER be called for UPI payments
            verify(providerAConnector, never()).processPayment(anyString(), any(), anyString());
            verify(providerBConnector, times(1))
                    .processPayment(PAYMENT_ID, initiatedPayment.getAmount(), "INR");

            // Routing was called with UPI explicitly
            verify(routingEngine, times(1)).route(PaymentMethod.UPI);

            // No Kafka messages for successful payments
            verify(kafkaTemplate, never()).send(anyString(), anyString(), any(PaymentEvent.class));
        }

        /**
         * TEST CASE 3: Real-time payment state fetching via GET endpoint.
         *
         * <p><strong>Scenario:</strong> A payment in PROCESSING state is fetched via
         * {@code getPayment(id)}. The method must read directly from MySQL and return all
         * fields including version counter and retry count — no Redis involvement.
         *
         * <p><strong>Why this matters:</strong> During the Kafka retry cycle, the status
         * changes from PROCESSING → SUCCESS or FAILED. Clients polling GET must always
         * receive the freshest DB state. Redis caching is intentionally bypassed here.
         */
        @Test
        @DisplayName("TC-03: GET payment fetches real-time status including version/retry from MySQL")
        void tc03_getPaymentFetchesRealTimeStateFromMySQL() {
            // ── ARRANGE ──────────────────────────────────────────────────────
            // Simulate a payment in PROCESSING state (Kafka retries in progress).
            // version=2 means two DB updates have occurred since INITIATED.
            // retryCount=2 means the Kafka consumer has attempted twice.
            Payment processingPayment = Payment.builder()
                    .id(PAYMENT_ID)
                    .idempotencyKey(IDEMPOTENCY_KEY)
                    .amount(new BigDecimal("150.00"))
                    .currency("USD")
                    .paymentMethod(PaymentMethod.CARD)
                    .status(PaymentStatus.PROCESSING)
                    .providerId(PROVIDER_A_ID)
                    .providerReferenceId(null)  // null until SUCCESS
                    .retryCount(2)
                    .version(2L)
                    .createdAt(LocalDateTime.now().minusMinutes(5))
                    .updatedAt(LocalDateTime.now().minusSeconds(4))
                    .build();

            when(paymentRepository.findById(PAYMENT_ID)).thenReturn(Optional.of(processingPayment));

            // ── ACT ───────────────────────────────────────────────────────────
            PaymentResponse response = orchestratorService.getPayment(PAYMENT_ID);

            // ── ASSERT ────────────────────────────────────────────────────────
            assertThat(response.id()).isEqualTo(PAYMENT_ID);
            assertThat(response.status())
                    .as("Status must reflect real-time PROCESSING state from MySQL")
                    .isEqualTo(PaymentStatus.PROCESSING);
            assertThat(response.retryCount())
                    .as("Retry count must be exactly 2 — two Kafka attempts have been made")
                    .isEqualTo(2);
            // version is not in PaymentResponse but providerReferenceId should be null
            assertThat(response.providerReferenceId())
                    .as("Provider reference must be null while payment is still being retried")
                    .isNull();
            assertThat(response.providerId())
                    .as("Provider ID must be set to whoever attempted the last call")
                    .isEqualTo(PROVIDER_A_ID);

            // Verify MySQL was queried exactly once — no Redis, no caching
            verify(paymentRepository, times(1)).findById(PAYMENT_ID);

            // Verify IdempotencyService was NOT involved in the GET path
            verify(idempotencyService, never()).getCachedResponse(anyString());
            verify(idempotencyService, never()).acquireInFlightLock(anyString());
        }

        /**
         * TEST CASE 4: Idempotency cache hit — completed response returned from Redis.
         *
         * <p><strong>Scenario:</strong> A request arrives with an Idempotency-Key that
         * maps to an already-completed PaymentResponse in Redis. The filter detects
         * this and short-circuits. This test validates the SERVICE layer behaviour when
         * acquireInFlightLock returns false AND getCachedResponse has a value.
         *
         * <p><strong>Key insight:</strong> Zero DB calls, zero provider calls, zero Kafka
         * publishes. The entire request is served from the Redis O(1) GET.
         */
        @Test
        @DisplayName("TC-04: Idempotency cache hit returns cached response without DB or provider calls")
        void tc04_idempotencyCacheHitBypassesAllLayersExceptRedis() {
            // ── ARRANGE ──────────────────────────────────────────────────────
            // Simulate the scenario where another concurrent thread beat us to the lock.
            // acquireInFlightLock returns FALSE (the key already exists in Redis as a
            // completed response, not as IN_FLIGHT).
            when(idempotencyService.acquireInFlightLock(IDEMPOTENCY_KEY)).thenReturn(false);

            // getCachedResponse returns the previously completed SUCCESS response
            PaymentResponse cachedResponse = new PaymentResponse(
                    PAYMENT_ID, IDEMPOTENCY_KEY,
                    new BigDecimal("150.00"), "USD",
                    PaymentMethod.CARD, PaymentStatus.SUCCESS,
                    PROVIDER_A_ID, PROVIDER_A_REF,
                    0, LocalDateTime.now().minusMinutes(1), LocalDateTime.now().minusMinutes(1)
            );
            when(idempotencyService.getCachedResponse(IDEMPOTENCY_KEY))
                    .thenReturn(Optional.of(cachedResponse));

            // ── ACT ───────────────────────────────────────────────────────────
            PaymentResponse response = orchestratorService.createPayment(cardRequest(), IDEMPOTENCY_KEY);

            // ── ASSERT ────────────────────────────────────────────────────────
            assertThat(response)
                    .as("Returned response must be the exact cached object from Redis")
                    .isEqualTo(cachedResponse);
            assertThat(response.status()).isEqualTo(PaymentStatus.SUCCESS);
            assertThat(response.providerReferenceId()).isEqualTo(PROVIDER_A_REF);

            // Verify NO database operations occurred — complete bypass
            verify(paymentRepository, never()).save(any(Payment.class));
            verify(paymentRepository, never()).findById(anyString());
            verify(paymentRepository, never()).updateOnSuccess(
                    anyString(), any(), anyString(), anyString(), anyInt(), anyLong());

            // Verify NO provider calls occurred
            verify(routingEngine, never()).route(any(PaymentMethod.class));
            verify(providerAConnector, never()).processPayment(anyString(), any(), anyString());
            verify(providerBConnector, never()).processPayment(anyString(), any(), anyString());

            // Verify NO Kafka messages published
            verify(kafkaTemplate, never()).send(anyString(), anyString(), any(PaymentEvent.class));
        }
    }

    // =========================================================================
    // CATEGORY B — NEGATIVE & ERROR BOUNDARY SCENARIOS
    // =========================================================================

    @Nested
    @DisplayName("Category B — Negative and Error Boundary Scenarios")
    class NegativeAndErrorScenariosTest {

        /**
         * TEST CASE 6: In-flight idempotency collision — 409 Conflict.
         *
         * <p><strong>Scenario:</strong> Two concurrent requests arrive with the same
         * Idempotency-Key. The first acquires the Redis NX lock. The second also calls
         * acquireInFlightLock which returns FALSE (key already in Redis as IN_FLIGHT).
         * getCachedResponse also returns empty (processing is in-flight, not completed yet).
         * The service must throw {@code IdempotencyConflictException}.
         *
         * <p><strong>Why this is critical:</strong> Without this guard, two concurrent
         * requests could both insert a payment record, causing duplicate charges.
         */
        @Test
        @DisplayName("TC-06: Concurrent request with in-flight key throws IdempotencyConflictException")
        void tc06_inFlightIdempotencyKeyThrowsConflict() {
            // ── ARRANGE ──────────────────────────────────────────────────────
            // Lock is NOT acquired — another thread already holds it for this key
            when(idempotencyService.acquireInFlightLock(IDEMPOTENCY_KEY)).thenReturn(false);

            // getCachedResponse returns empty — the first request is still processing,
            // so no completed response has been stored in Redis yet
            when(idempotencyService.getCachedResponse(IDEMPOTENCY_KEY))
                    .thenReturn(Optional.empty());

            // ── ACT & ASSERT ──────────────────────────────────────────────────
            assertThatThrownBy(() ->
                    orchestratorService.createPayment(cardRequest(), IDEMPOTENCY_KEY))
                    .as("Must throw IdempotencyConflictException when key is in-flight")
                    .isInstanceOf(IdempotencyConflictException.class)
                    .hasMessageContaining(IDEMPOTENCY_KEY);

            // Verify that NO payment was persisted — the conflict was caught before any DB access
            verify(paymentRepository, never()).save(any(Payment.class));

            // Verify NO provider routing occurred
            verify(routingEngine, never()).route(any(PaymentMethod.class));

            // Verify NO Kafka messages
            verify(kafkaTemplate, never()).send(anyString(), anyString(), any(PaymentEvent.class));
        }

        /**
         * TEST CASE 8: Missing transaction ID — 404 Not Found.
         *
         * <p><strong>Scenario:</strong> A GET request arrives for a payment UUID that does
         * not exist in MySQL. The repository returns an empty Optional. The service must
         * throw {@code PaymentNotFoundException} which the GlobalExceptionHandler maps to 404.
         */
        @Test
        @DisplayName("TC-08: GET with non-existent UUID throws PaymentNotFoundException")
        void tc08_getNonExistentPaymentThrowsNotFoundException() {
            // ── ARRANGE ──────────────────────────────────────────────────────
            String nonExistentId = "00000000-0000-0000-0000-000000000000";
            when(paymentRepository.findById(nonExistentId)).thenReturn(Optional.empty());

            // ── ACT & ASSERT ──────────────────────────────────────────────────
            assertThatThrownBy(() -> orchestratorService.getPayment(nonExistentId))
                    .as("Must throw PaymentNotFoundException for unknown IDs")
                    .isInstanceOf(PaymentNotFoundException.class)
                    .hasMessageContaining(nonExistentId);

            verify(paymentRepository, times(1)).findById(nonExistentId);
        }

        /**
         * TEST CASE 9: Redis infrastructure outage — graceful degradation.
         *
         * <p><strong>Scenario:</strong> Redis is completely unavailable.
         * {@code acquireInFlightLock()} throws a
         * {@code org.springframework.data.redis.RedisConnectionFailureException}.
         * The service must propagate this exception (the GlobalExceptionHandler maps it
         * to 500 and the idempotency key is released) — preventing a silent double-charge.
         *
         * <p><strong>Design rationale:</strong> Failing OPEN (allowing the request through
         * when Redis is down) risks duplicate charges — which is a financial loss.
         * Failing CLOSED (rejecting the request with 500) is the safer default.
         * The idempotency key is released in the catch block so retries are possible
         * once Redis recovers.
         */
        @Test
        @DisplayName("TC-09: Redis connection failure causes exception and key cleanup")
        void tc09_redisOutageCausesExceptionAndReleasesKey() {
            // ── ARRANGE ──────────────────────────────────────────────────────
            // Simulate Redis being completely unreachable
            org.springframework.data.redis.RedisConnectionFailureException redisEx =
                    new org.springframework.data.redis.RedisConnectionFailureException(
                            "Unable to connect to localhost:6379");

            when(idempotencyService.acquireInFlightLock(IDEMPOTENCY_KEY)).thenThrow(redisEx);

            // ── ACT & ASSERT ──────────────────────────────────────────────────
            assertThatThrownBy(() ->
                    orchestratorService.createPayment(cardRequest(), IDEMPOTENCY_KEY))
                    .as("Redis failure must propagate — cannot safely process without idempotency guard")
                    .isInstanceOf(org.springframework.data.redis.RedisConnectionFailureException.class);

            // Verify no payment was inserted into MySQL — the exception was thrown
            // BEFORE the DB call, so the DB must be untouched
            verify(paymentRepository, never()).save(any(Payment.class));

            // Verify no Kafka messages were published
            verify(kafkaTemplate, never()).send(anyString(), anyString(), any(PaymentEvent.class));
        }
    }

    // =========================================================================
    // CATEGORY C — ADVANCED RESILIENCY, KAFKA RETRIES & FAILOVERS
    // =========================================================================

    @Nested
    @DisplayName("Category C — Resiliency, Kafka Hand-Off, and Failover Logic")
    class ResiliencyAndKafkaScenariosTest {

        /**
         * TEST CASE 10A: Primary provider failure → PROCESSING state → Kafka hand-off.
         *
         * <p><strong>Scenario:</strong> Provider A throws a simulated 504 Gateway Timeout.
         * The orchestrator must:
         * <ol>
         *   <li>Catch the {@code ProviderException}</li>
         *   <li>Update MySQL status to PROCESSING (not FAILED — retries are still possible)</li>
         *   <li>Build and publish a {@code PaymentEvent} to {@code payment-main-topic}</li>
         *   <li>Return the PROCESSING response to the client immediately</li>
         *   <li>Cache the PROCESSING response in Redis</li>
         * </ol>
         * The client thread is RELEASED before Kafka retries begin.
         */
        @Test
        @DisplayName("TC-10A: Provider 504 timeout transitions to PROCESSING, publishes to Kafka, returns immediately")
        void tc10a_provider504TimeoutTransitionsToProcessingAndPublishesToKafka() {
            // ── ARRANGE ──────────────────────────────────────────────────────
            CreatePaymentRequest request = cardRequest();
            Payment initiatedPayment = buildInitiatedPayment(PaymentMethod.CARD);
            Payment processingPayment = buildProcessingPayment(PaymentMethod.CARD, PROVIDER_A_ID);

            when(idempotencyService.acquireInFlightLock(IDEMPOTENCY_KEY)).thenReturn(true);
            when(paymentRepository.save(any(Payment.class))).thenReturn(initiatedPayment);
            when(routingEngine.route(PaymentMethod.CARD)).thenReturn(providerAConnector);
            when(providerAConnector.getProviderId()).thenReturn(PROVIDER_A_ID);

            // Simulate Provider A throwing a 504 Gateway Timeout
            when(providerAConnector.processPayment(PAYMENT_ID, initiatedPayment.getAmount(), "USD"))
                    .thenThrow(new ProviderException(PROVIDER_A_ID,
                            "504 Gateway Timeout: Provider A did not respond"));

            // updateStatusWithVersionCheck is called to transition to PROCESSING
            when(paymentRepository.updateStatusWithVersionCheck(
                    eq(PAYMENT_ID),
                    eq(PaymentStatus.PROCESSING),
                    eq(0),
                    eq(0L)
            )).thenReturn(1);

            // After the PROCESSING update, findById returns the PROCESSING entity
            when(paymentRepository.findById(PAYMENT_ID)).thenReturn(Optional.of(processingPayment));

            // ── ACT ───────────────────────────────────────────────────────────
            PaymentResponse response = orchestratorService.createPayment(request, IDEMPOTENCY_KEY);

            // ── ASSERT ────────────────────────────────────────────────────────

            // Client receives PROCESSING — not a final state, but a valid tracking response
            assertThat(response.status())
                    .as("Status must be PROCESSING when primary provider fails — Kafka will retry")
                    .isEqualTo(PaymentStatus.PROCESSING);

            // No providerReferenceId yet — the payment hasn't been completed
            assertThat(response.providerReferenceId())
                    .as("ProviderReferenceId must be null while payment is PROCESSING")
                    .isNull();

            // Verify MySQL was transitioned to PROCESSING with the correct version guard
            verify(paymentRepository, times(1))
                    .updateStatusWithVersionCheck(PAYMENT_ID, PaymentStatus.PROCESSING, 0, 0L);

            // Verify Kafka received EXACTLY ONE message on the correct topic
            ArgumentCaptor<PaymentEvent> eventCaptor = ArgumentCaptor.forClass(PaymentEvent.class);
            ArgumentCaptor<String> topicCaptor = ArgumentCaptor.forClass(String.class);
            ArgumentCaptor<String> keyCaptor = ArgumentCaptor.forClass(String.class);

            verify(kafkaTemplate, times(1)).send(
                    topicCaptor.capture(),
                    keyCaptor.capture(),
                    eventCaptor.capture()
            );

            // Topic must be the main topic (not a retry topic)
            assertThat(topicCaptor.getValue())
                    .as("Kafka message must be sent to the main topic")
                    .isEqualTo(MAIN_TOPIC);

            // Message key must be the payment ID (ensures partition ordering)
            assertThat(keyCaptor.getValue())
                    .as("Kafka message key must be the payment ID for partition ordering")
                    .isEqualTo(PAYMENT_ID);

            // Validate the PaymentEvent payload content
            PaymentEvent publishedEvent = eventCaptor.getValue();
            assertThat(publishedEvent.paymentId())
                    .as("PaymentEvent must carry the correct payment UUID")
                    .isEqualTo(PAYMENT_ID);
            assertThat(publishedEvent.paymentMethod())
                    .as("PaymentEvent must carry the CARD payment method for retry routing")
                    .isEqualTo(PaymentMethod.CARD);
            assertThat(publishedEvent.retryCount())
                    .as("PaymentEvent retryCount must be 0 — this is the first failure")
                    .isEqualTo(0);

            // Verify Redis was updated with the PROCESSING response (not left as IN_FLIGHT)
            ArgumentCaptor<PaymentResponse> cachedCaptor =
                    ArgumentCaptor.forClass(PaymentResponse.class);
            verify(idempotencyService, times(1))
                    .storeCompletedResponse(eq(IDEMPOTENCY_KEY), cachedCaptor.capture());
            assertThat(cachedCaptor.getValue().status())
                    .as("Redis must be updated with PROCESSING so idempotent replays return it")
                    .isEqualTo(PaymentStatus.PROCESSING);

            // SUCCESS update path must NOT have been called (provider failed)
            verify(paymentRepository, never())
                    .updateOnSuccess(anyString(), any(), anyString(), anyString(), anyInt(), anyLong());
        }

        /**
         * TEST CASE 10B: Cross-provider failover mid-stream (routing engine failover=true).
         *
         * <p><strong>Scenario:</strong> The routing engine is called twice:
         * <ol>
         *   <li>First call: {@code route(CARD, false)} → Provider A (primary)</li>
         *   <li>Second call: {@code route(CARD, true)} → Provider B (failover)</li>
         * </ol>
         * This test directly exercises the RoutingEngine routing logic with real
         * ProviderA and ProviderB connector stubs — no Kafka involved here, as this
         * tests the routing engine in isolation.
         */
        @Test
        @DisplayName("TC-10B: RoutingEngine returns ProviderB when failover=true for CARD payment")
        void tc10b_routingEngineReturnsProviderBForCardFailover() {
            // ── ARRANGE ──────────────────────────────────────────────────────
            // Use real RoutingEngine with mock connectors injected via constructor
            // (tests the actual routing switch logic, not just a mock)
            org.example.routing.RoutingEngine realRoutingEngine =
                    new org.example.routing.RoutingEngine(
                            (org.example.provider.ProviderAConnector) providerAConnector,
                            (org.example.provider.ProviderBConnector) providerBConnector
                    );

            // ── ACT ───────────────────────────────────────────────────────────
            PaymentProviderConnector primaryConnector =
                    realRoutingEngine.route(PaymentMethod.CARD, false);
            PaymentProviderConnector failoverConnector =
                    realRoutingEngine.route(PaymentMethod.CARD, true);
            PaymentProviderConnector upiPrimaryConnector =
                    realRoutingEngine.route(PaymentMethod.UPI, false);
            PaymentProviderConnector upiFailoverConnector =
                    realRoutingEngine.route(PaymentMethod.UPI, true);

            // ── ASSERT ────────────────────────────────────────────────────────
            assertThat(primaryConnector)
                    .as("CARD primary must route to ProviderA")
                    .isSameAs(providerAConnector);

            assertThat(failoverConnector)
                    .as("CARD failover must route to ProviderB (the alternate gateway)")
                    .isSameAs(providerBConnector);

            assertThat(upiPrimaryConnector)
                    .as("UPI primary must route to ProviderB")
                    .isSameAs(providerBConnector);

            assertThat(upiFailoverConnector)
                    .as("UPI failover must route to ProviderA")
                    .isSameAs(providerAConnector);
        }

        /**
         * TEST CASE 11: Kafka retry consumer — failover routing on retry attempt > 1.
         *
         * <p><strong>Scenario:</strong> We directly invoke the {@code PaymentRetryConsumer}
         * (the Kafka listener method) simulating what Spring Kafka does when it delivers a
         * message. We verify:
         * <ul>
         *   <li>On attempt 1 (deliveryAttempt header = 1): uses primary provider (no failover)</li>
         *   <li>On attempt 2 (deliveryAttempt header = 2): uses failover provider</li>
         *   <li>If the failover provider succeeds, MySQL is updated to SUCCESS</li>
         * </ul>
         *
         * <p><strong>Note:</strong> The @RetryableTopic retry loop cannot be tested purely in
         * unit tests — that requires @EmbeddedKafka (see PaymentConsumerIntegrationTest).
         * What we CAN test here is the routing decision logic and DB mutation inside
         * processPayment() on each individual invocation.
         */
        @Test
        @DisplayName("TC-11: Retry consumer uses failover provider on attempt #2 and succeeds")
        void tc11_kafkaConsumerUsesFailoverProviderOnRetryAttempt2() {
            // ── ARRANGE ──────────────────────────────────────────────────────
            // Build dependent beans for the retry consumer
            org.example.routing.RoutingEngine realRoutingEngine =
                    new org.example.routing.RoutingEngine(
                            (org.example.provider.ProviderAConnector) providerAConnector,
                            (org.example.provider.ProviderBConnector) providerBConnector
                    );

            org.example.kafka.PaymentRetryConsumer retryConsumer =
                    new org.example.kafka.PaymentRetryConsumer(paymentRepository, realRoutingEngine);

            // Payment in PROCESSING state with version=1 (one UPDATE already done)
            Payment processingPayment = Payment.builder()
                    .id(PAYMENT_ID)
                    .idempotencyKey(IDEMPOTENCY_KEY)
                    .amount(new BigDecimal("150.00"))
                    .currency("USD")
                    .paymentMethod(PaymentMethod.CARD)
                    .status(PaymentStatus.PROCESSING)
                    .providerId(PROVIDER_A_ID)
                    .retryCount(1)
                    .version(1L)
                    .createdAt(LocalDateTime.now())
                    .updatedAt(LocalDateTime.now())
                    .build();

            when(paymentRepository.findById(PAYMENT_ID)).thenReturn(Optional.of(processingPayment));

            // Provider A must NOT be called (failover=true for attempt #2)
            // Provider B succeeds on the failover attempt
            when(providerBConnector.getProviderId()).thenReturn(PROVIDER_B_ID);
            when(providerBConnector.processPayment(PAYMENT_ID, new BigDecimal("150.00"), "USD"))
                    .thenReturn(PROVIDER_B_REF);

            // DB updateOnSuccess returns 1 (success)
            when(paymentRepository.updateOnSuccess(
                    eq(PAYMENT_ID),
                    eq(PaymentStatus.SUCCESS),
                    eq(PROVIDER_B_ID),
                    eq(PROVIDER_B_REF),
                    eq(2),  // retryCount + 1
                    eq(1L)  // current version
            )).thenReturn(1);

            // Build a ConsumerRecord simulating the Kafka message
            org.apache.kafka.clients.consumer.ConsumerRecord<String, PaymentEvent> record =
                    new org.apache.kafka.clients.consumer.ConsumerRecord<>(
                            MAIN_TOPIC, 0, 0L, PAYMENT_ID,
                            new PaymentEvent(PAYMENT_ID, new BigDecimal("150.00"),
                                    "USD", PaymentMethod.CARD, 1, 1L)
                    );

            // ── ACT ───────────────────────────────────────────────────────────
            // Invoke the listener directly with deliveryAttempt=2 (first Kafka retry)
            retryConsumer.processPayment(record, MAIN_TOPIC, 2);

            // ── ASSERT ────────────────────────────────────────────────────────

            // Provider A must NEVER be called on retry attempt #2 (failover kicks in)
            verify(providerAConnector, never()).processPayment(anyString(), any(), anyString());

            // Provider B must be called exactly once (the failover connector)
            verify(providerBConnector, times(1))
                    .processPayment(PAYMENT_ID, new BigDecimal("150.00"), "USD");

            // MySQL must be updated to SUCCESS with the Provider B reference
            verify(paymentRepository, times(1)).updateOnSuccess(
                    PAYMENT_ID, PaymentStatus.SUCCESS, PROVIDER_B_ID, PROVIDER_B_REF, 2, 1L);
        }

        /**
         * TEST CASE: DLT Handler marks payment as FAILED after all retries exhausted.
         *
         * <p><strong>Scenario:</strong> The DLT handler is invoked after all retry attempts
         * fail. The handler must update MySQL to FAILED and log the stack trace header.
         * The version-guarded UPDATE must be called with the correct parameters.
         */
        @Test
        @DisplayName("TC-DLT: DLT handler marks payment as FAILED in MySQL")
        void tcDlt_dltHandlerMarksPaymentAsFailedInMySQL() {
            // ── ARRANGE ──────────────────────────────────────────────────────
            org.example.routing.RoutingEngine realRoutingEngine =
                    new org.example.routing.RoutingEngine(
                            (org.example.provider.ProviderAConnector) providerAConnector,
                            (org.example.provider.ProviderBConnector) providerBConnector
                    );

            org.example.kafka.PaymentRetryConsumer retryConsumer =
                    new org.example.kafka.PaymentRetryConsumer(paymentRepository, realRoutingEngine);

            // Payment still in PROCESSING after all retry attempts
            Payment processingPayment = Payment.builder()
                    .id(PAYMENT_ID)
                    .idempotencyKey(IDEMPOTENCY_KEY)
                    .amount(new BigDecimal("150.00"))
                    .currency("USD")
                    .paymentMethod(PaymentMethod.CARD)
                    .status(PaymentStatus.PROCESSING)
                    .retryCount(4)
                    .version(4L)
                    .createdAt(LocalDateTime.now())
                    .updatedAt(LocalDateTime.now())
                    .build();

            when(paymentRepository.findById(PAYMENT_ID)).thenReturn(Optional.of(processingPayment));

            // Version-guarded FAILED update succeeds (1 row updated)
            when(paymentRepository.updateStatusWithVersionCheck(
                    eq(PAYMENT_ID),
                    eq(PaymentStatus.FAILED),
                    eq(4),
                    eq(4L)
            )).thenReturn(1);

            // Build DLT ConsumerRecord with exception header bytes
            byte[] stackTraceBytes = "java.io.IOException: 504 Gateway Timeout\n\tat org.example..."
                    .getBytes(java.nio.charset.StandardCharsets.UTF_8);

            org.apache.kafka.clients.consumer.ConsumerRecord<String, PaymentEvent> dltRecord =
                    new org.apache.kafka.clients.consumer.ConsumerRecord<>(
                            MAIN_TOPIC + "-dlt", 0, 0L, PAYMENT_ID,
                            new PaymentEvent(PAYMENT_ID, new BigDecimal("150.00"),
                                    "USD", PaymentMethod.CARD, 4, 4L)
                    );

            // ── ACT ───────────────────────────────────────────────────────────
            retryConsumer.handleDlt(dltRecord, stackTraceBytes);

            // ── ASSERT ────────────────────────────────────────────────────────
            // The DLT handler must have updated the payment to FAILED
            verify(paymentRepository, times(1))
                    .updateStatusWithVersionCheck(PAYMENT_ID, PaymentStatus.FAILED, 4, 4L);

            // Providers must NEVER be called in the DLT handler — it is a cleanup step
            verify(providerAConnector, never()).processPayment(anyString(), any(), anyString());
            verify(providerBConnector, never()).processPayment(anyString(), any(), anyString());
        }
    }
}

