package org.example.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.example.config.KafkaConsumerConfig;
import org.example.config.KafkaProducerConfig;
import org.example.dto.PaymentEvent;
import org.example.entity.Payment;
import org.example.entity.PaymentMethod;
import org.example.entity.PaymentStatus;
import org.example.exception.ProviderException;
import org.example.provider.PaymentProviderConnector;
import org.example.repository.PaymentRepository;
import org.example.routing.RoutingEngine;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.ContainerTestUtils;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.TestPropertySource;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * PaymentConsumerIntegrationTest — full Spring Boot integration tests for the Kafka
 * retry consumer pipeline.
 *
 * <h2>Testing Strategy</h2>
 * <p>This class uses:
 * <ul>
 *   <li>{@code @SpringBootTest} — starts the FULL Spring ApplicationContext including
 *       all beans (KafkaTemplate, KafkaConsumerConfig, PaymentRetryConsumer, etc.)
 *       but uses an embedded in-memory Kafka broker instead of localhost:9092.</li>
 *   <li>{@code @EmbeddedKafka} — provides an in-process Kafka broker that lives only
 *       for the duration of the test class. No external Kafka installation required.</li>
 *   <li>{@code @MockBean} for repository and connectors — we do NOT want a real MySQL
 *       connection; we use Mockito stubs to simulate DB state.</li>
 *   <li>Awaitility for async assertions — since Kafka consumer processing is asynchronous,
 *       we cannot use simple synchronous verify() calls. Awaitility polls until the expected
 *       mock interaction occurs or a timeout is exceeded.</li>
 * </ul>
 *
 * <h2>@EmbeddedKafka Configuration</h2>
 * <p>The embedded broker is configured with:
 * <ul>
 *   <li>{@code partitions=1} — single partition ensures message ordering for deterministic tests</li>
 *   <li>{@code topics} — the main topic and all known retry/DLT topics are pre-created so
 *       the consumer can join partitions immediately without waiting for topic auto-creation</li>
 *   <li>{@code brokerProperties} — sets {@code auto.create.topics.enable=false} to prevent
 *       accidental topic creation outside our declared set</li>
 * </ul>
 *
 * <h2>@DirtiesContext</h2>
 * <p>Marks the Spring context as dirty after each test class, forcing a rebuild. This is
 * necessary because the embedded Kafka broker accumulates state (committed offsets) between
 * tests. Without DirtiesContext, test #2 might pick up unconsumed messages from test #1.
 *
 * <h2>@TestPropertySource Overrides</h2>
 * <p>Overrides the Kafka bootstrap server to point to the embedded broker's dynamic port
 * (injected via {@code Spring.embedded.kafka.brokers} system property). Also disables
 * Redis and MySQL auto-configuration to prevent connection failures during the test.
 */
@SpringBootTest(
        // Load the full context but excluding beans that need real external infrastructure.
        // We use MockBean for the DB layer and provide embedded Kafka for the broker.
        classes = {
                org.example.Main.class
        },
        webEnvironment = org.springframework.boot.test.context.SpringBootTest.WebEnvironment.NONE
)
@EmbeddedKafka(
        partitions = 1,
        // Pre-create all topics the @RetryableTopic annotation will use.
        // If these do not exist, the consumer waits for auto-creation which can cause
        // flaky timing issues in tests.
        topics = {
                "payment-main-topic",
                "payment-main-topic-retry-0",
                "payment-main-topic-retry-1",
                "payment-main-topic-retry-2",
                "payment-main-topic-dlt"
        },
        brokerProperties = {
                "auto.create.topics.enable=false",
                // Keep log segments small so test cleanup is fast
                "log.segment.bytes=1048576"
        }
)
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD)
@TestPropertySource(properties = {
        // Point Kafka producer/consumer to the embedded broker (dynamic port assigned by Spring)
        "spring.kafka.bootstrap-servers=${spring.embedded.kafka.brokers}",
        // Override the topic name property to match the @EmbeddedKafka topics above
        "payment.kafka.main-topic=payment-main-topic",
        // Disable MySQL auto-configuration — we use MockBean for the repository
        "spring.autoconfigure.exclude=" +
                "org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration," +
                "org.springframework.boot.autoconfigure.orm.jpa.HibernateJpaAutoConfiguration," +
                "org.springframework.boot.autoconfigure.data.jpa.JpaRepositoriesAutoConfiguration," +
                // Disable Redis auto-configuration — no Redis needed for these tests
                "org.springframework.boot.autoconfigure.data.redis.RedisAutoConfiguration," +
                "org.springframework.boot.autoconfigure.data.redis.RedisRepositoriesAutoConfiguration",
        // Disable Hibernate schema validation (no real DB present)
        "spring.jpa.hibernate.ddl-auto=none",
        // Speed up consumer polls for faster test execution
        "spring.kafka.consumer.auto-offset-reset=earliest"
})
@DisplayName("PaymentConsumerIntegrationTest — Kafka Retry Pipeline Integration Tests")
class PaymentConsumerIntegrationTest {

    /** The embedded Kafka broker — injected by @EmbeddedKafka. */
    @Autowired
    private EmbeddedKafkaBroker embeddedKafkaBroker;

    /**
     * The real KafkaTemplate wired to the embedded broker.
     * Used in tests to publish PaymentEvent messages directly (simulating what the
     * orchestrator service does in production after a provider failure).
     */
    @Autowired
    private KafkaTemplate<String, PaymentEvent> kafkaTemplate;

    /**
     * KafkaListenerEndpointRegistry — provides access to all active listener containers.
     * Used to wait for the consumer container to be fully assigned to partitions before
     * publishing test messages (avoiding partition.assignment race conditions in tests).
     */
    @Autowired
    private KafkaListenerEndpointRegistry registry;

    /**
     * @MockBean replaces the real PaymentRepository in the Spring context.
     * The real repository would require a MySQL connection; we simulate it with stubs.
     */
    @MockBean
    private PaymentRepository paymentRepository;

    /**
     * @MockBean replaces the real RoutingEngine — we inject controlled connectors.
     */
    @MockBean
    private RoutingEngine routingEngine;

    /**
     * @MockBean provider connectors — we control exactly when they succeed or fail.
     */
    @MockBean(name = "providerAConnector")
    private PaymentProviderConnector providerAConnector;

    @MockBean(name = "providerBConnector")
    private PaymentProviderConnector providerBConnector;

    // ─── Also mock any beans that require real infrastructure connections ────
    // These are needed because @SpringBootTest loads ALL beans; without mocking them,
    // Spring Boot tries to connect to localhost:6379 (Redis) and localhost:3306 (MySQL)
    // during test startup.

    @MockBean
    private org.example.service.IdempotencyService idempotencyService;

    // ─────────────────────────────────────────────────────────────────────────
    // Test constants
    // ─────────────────────────────────────────────────────────────────────────

    private static final String PAYMENT_ID      = "test-payment-id-kafka-001";
    private static final String IDEMPOTENCY_KEY = "test-idem-key-kafka-001";
    private static final String MAIN_TOPIC      = "payment-main-topic";
    private static final String PROVIDER_A_ID   = "PROVIDER_A";
    private static final String PROVIDER_B_ID   = "PROVIDER_B";
    private static final String PROVIDER_B_REF  = "PROVB-KAFKA-TEST-SUCCESS";
    private static final BigDecimal AMOUNT      = new BigDecimal("250.00");
    private static final String CURRENCY        = "USD";

    /**
     * Awaitility timeout — how long to wait for async Kafka consumer invocations.
     * Set to 30 seconds to accommodate exponential backoff in non-blocking retries.
     * In test environments with short backoff overrides, this can be much lower.
     */
    private static final long AWAIT_TIMEOUT_SECONDS = 30L;

    @BeforeEach
    void waitForConsumerAssignment() {
        // Wait for ALL listener containers to be assigned to partitions on the embedded broker.
        // ContainerTestUtils.waitForAssignment() blocks until the consumer has joined the
        // consumer group and received its partition assignments.
        // Without this wait, test messages published immediately after @BeforeEach may not
        // be seen by the consumer (the consumer hasn't connected yet).
        registry.getListenerContainers().forEach(container ->
                ContainerTestUtils.waitForAssignment(container,
                        embeddedKafkaBroker.getPartitionsPerTopic()));
    }

    @AfterEach
    void resetMocks() {
        // Reset Mockito interaction counters after each test to prevent
        // cross-test contamination (e.g., verify(mock, times(2)) in test B
        // accidentally counting invocations from test A).
        org.mockito.Mockito.reset(paymentRepository, routingEngine,
                providerAConnector, providerBConnector);
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Helper: Build a Payment entity in PROCESSING state
    // ─────────────────────────────────────────────────────────────────────────

    private Payment buildProcessingPayment(int retryCount, long version) {
        return Payment.builder()
                .id(PAYMENT_ID)
                .idempotencyKey(IDEMPOTENCY_KEY)
                .amount(AMOUNT)
                .currency(CURRENCY)
                .paymentMethod(PaymentMethod.CARD)
                .status(PaymentStatus.PROCESSING)
                .providerId(PROVIDER_A_ID)
                .retryCount(retryCount)
                .version(version)
                .createdAt(LocalDateTime.now().minusMinutes(1))
                .updatedAt(LocalDateTime.now())
                .build();
    }

    // =========================================================================
    // TEST CASE 10: Primary Provider Failure → SUCCESS on Kafka Retry via Failover
    //
    // Full lifecycle:
    //   1. Message published to payment-main-topic (simulating orchestrator failure hand-off)
    //   2. Consumer attempt #1: routes to ProviderA (primary), FAILS
    //   3. @RetryableTopic routes to retry-0 topic after 2s backoff
    //   4. Consumer attempt #2: routes to ProviderB (failover), SUCCEEDS
    //   5. MySQL updateOnSuccess is called
    // =========================================================================

    @Test
    @DisplayName("TC-10: CARD payment fails on ProviderA (attempt 1), succeeds on ProviderB (attempt 2 failover)")
    void tc10_cardPaymentFailsOnProviderAThenSucceedsOnProviderBFailover() throws Exception {

        // ── ARRANGE ──────────────────────────────────────────────────────────

        // Payment is in PROCESSING — findById will always return this stub
        Payment processingPayment = buildProcessingPayment(0, 1L);
        when(paymentRepository.findById(PAYMENT_ID)).thenReturn(Optional.of(processingPayment));

        // Routing: attempt 1 (failover=false) → ProviderA; attempt 2+ (failover=true) → ProviderB
        when(routingEngine.route(PaymentMethod.CARD, false)).thenReturn(providerAConnector);
        when(routingEngine.route(PaymentMethod.CARD, true)).thenReturn(providerBConnector);

        // Provider A ALWAYS fails — simulating persistent 504 Gateway Timeout
        when(providerAConnector.getProviderId()).thenReturn(PROVIDER_A_ID);
        when(providerAConnector.processPayment(eq(PAYMENT_ID), eq(AMOUNT), eq(CURRENCY)))
                .thenThrow(new ProviderException(PROVIDER_A_ID,
                        "504 Gateway Timeout: Provider A unreachable"));

        // Provider B SUCCEEDS on the failover attempt
        when(providerBConnector.getProviderId()).thenReturn(PROVIDER_B_ID);
        when(providerBConnector.processPayment(eq(PAYMENT_ID), eq(AMOUNT), eq(CURRENCY)))
                .thenReturn(PROVIDER_B_REF);

        // updateStatusWithVersionCheck: called when retrying (PROCESSING state update)
        when(paymentRepository.updateStatusWithVersionCheck(
                eq(PAYMENT_ID), eq(PaymentStatus.PROCESSING), anyInt(), anyLong()))
                .thenReturn(1);

        // updateOnSuccess: called when Provider B succeeds
        when(paymentRepository.updateOnSuccess(
                eq(PAYMENT_ID),
                eq(PaymentStatus.SUCCESS),
                eq(PROVIDER_B_ID),
                eq(PROVIDER_B_REF),
                anyInt(),
                anyLong()
        )).thenReturn(1);

        // ── ACT: Publish the payment event to the main Kafka topic ────────────
        // This simulates what PaymentOrchestratorService does after a synchronous
        // provider failure — it publishes to Kafka and releases the REST thread.
        PaymentEvent event = new PaymentEvent(PAYMENT_ID, AMOUNT, CURRENCY, PaymentMethod.CARD, 0, 1L);
        kafkaTemplate.send(MAIN_TOPIC, PAYMENT_ID, event);

        // ── ASSERT: Await async Kafka consumer invocations ────────────────────
        // Awaitility polls every 500ms until the condition is met or timeout is reached.
        // This pattern is essential for async Kafka tests — Thread.sleep() is fragile.

        // Wait until ProviderA has been called at least once (attempt 1)
        await()
                .atMost(AWAIT_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                .pollInterval(500, TimeUnit.MILLISECONDS)
                .untilAsserted(() ->
                        verify(providerAConnector, atLeast(1))
                                .processPayment(PAYMENT_ID, AMOUNT, CURRENCY)
                );

        // Wait until updateOnSuccess is called — this signals Provider B succeeded
        await()
                .atMost(AWAIT_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                .pollInterval(500, TimeUnit.MILLISECONDS)
                .untilAsserted(() ->
                        verify(paymentRepository, times(1)).updateOnSuccess(
                                PAYMENT_ID,
                                PaymentStatus.SUCCESS,
                                PROVIDER_B_ID,
                                PROVIDER_B_REF,
                                anyInt(),
                                anyLong()
                        )
                );

        // Verify the FAILED status was NEVER set — success was achieved before DLT
        verify(paymentRepository, never()).updateStatusWithVersionCheck(
                eq(PAYMENT_ID), eq(PaymentStatus.FAILED), anyInt(), anyLong());
    }

    // =========================================================================
    // TEST CASE 11: Full Kafka Non-Blocking Retry Lifecycle → DLT terminal FAILED
    //
    // All 4 attempts (1 original + 3 retries) fail → DLT handler marks FAILED.
    // Verifies @RetryableTopic generates retry topics and DLT handler fires.
    // =========================================================================

    @Test
    @DisplayName("TC-11: All 4 Kafka retry attempts fail → DLT handler marks payment FAILED")
    void tc11_allKafkaRetryAttemptsExhaustedDltHandlerMarksFailed() throws Exception {

        // ── ARRANGE ──────────────────────────────────────────────────────────

        Payment processingPayment = buildProcessingPayment(0, 1L);
        when(paymentRepository.findById(PAYMENT_ID)).thenReturn(Optional.of(processingPayment));

        // RoutingEngine: always return ProviderA for non-failover, ProviderB for failover
        when(routingEngine.route(PaymentMethod.CARD, false)).thenReturn(providerAConnector);
        when(routingEngine.route(PaymentMethod.CARD, true)).thenReturn(providerBConnector);

        // BOTH providers ALWAYS fail — forcing all retries to exhaust and reach the DLT
        when(providerAConnector.getProviderId()).thenReturn(PROVIDER_A_ID);
        when(providerAConnector.processPayment(anyString(), any(), anyString()))
                .thenThrow(new ProviderException(PROVIDER_A_ID, "504 Gateway Timeout"));

        when(providerBConnector.getProviderId()).thenReturn(PROVIDER_B_ID);
        when(providerBConnector.processPayment(anyString(), any(), anyString()))
                .thenThrow(new ProviderException(PROVIDER_B_ID, "500 Internal Server Error"));

        // updateStatusWithVersionCheck: called to increment retry count on each failure
        when(paymentRepository.updateStatusWithVersionCheck(
                anyString(), any(PaymentStatus.class), anyInt(), anyLong()))
                .thenReturn(1);

        // ── ACT ───────────────────────────────────────────────────────────────
        PaymentEvent event = new PaymentEvent(PAYMENT_ID, AMOUNT, CURRENCY, PaymentMethod.CARD, 0, 1L);
        kafkaTemplate.send(MAIN_TOPIC, PAYMENT_ID, event);

        // ── ASSERT ────────────────────────────────────────────────────────────

        // Wait until the DLT handler fires — this means all retries were exhausted.
        // The DLT handler calls updateStatusWithVersionCheck(FAILED, ...).
        await()
                .atMost(AWAIT_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                .pollInterval(500, TimeUnit.MILLISECONDS)
                .untilAsserted(() ->
                        verify(paymentRepository, atLeast(1)).updateStatusWithVersionCheck(
                                eq(PAYMENT_ID),
                                eq(PaymentStatus.FAILED),
                                anyInt(),
                                anyLong()
                        )
                );

        // Verify that providers were called multiple times (original + retries)
        // At minimum, providerA is called on attempt 1. ProviderB on retries 2-4.
        await()
                .atMost(AWAIT_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                .untilAsserted(() -> {
                    verify(providerAConnector, atLeast(1))
                            .processPayment(anyString(), any(), anyString());
                    verify(providerBConnector, atLeast(1))
                            .processPayment(anyString(), any(), anyString());
                });

        // SUCCESS update should NEVER have been called — all providers failed
        verify(paymentRepository, never()).updateOnSuccess(
                anyString(), any(), anyString(), anyString(), anyInt(), anyLong());
    }

    // =========================================================================
    // TEST CASE: Kafka consumer skips terminal payments (SUCCESS or FAILED)
    //
    // If a payment reaches the consumer but is already in a terminal state
    // (e.g., another thread/request already resolved it), the consumer must
    // skip processing without calling providers or updating the DB.
    // =========================================================================

    @Test
    @DisplayName("Consumer skips processing if payment is already in SUCCESS state")
    void consumerSkipsAlreadySuccessfulPayment() throws Exception {

        // ── ARRANGE ──────────────────────────────────────────────────────────
        // Payment is already SUCCESS — simulating the case where the sync API
        // or a previous retry somehow resolved it, and a stale Kafka message arrives
        Payment successPayment = Payment.builder()
                .id(PAYMENT_ID)
                .idempotencyKey(IDEMPOTENCY_KEY)
                .amount(AMOUNT)
                .currency(CURRENCY)
                .paymentMethod(PaymentMethod.CARD)
                .status(PaymentStatus.SUCCESS)   // already terminal
                .providerId(PROVIDER_A_ID)
                .providerReferenceId("PROVA-ALREADY-DONE")
                .retryCount(0)
                .version(1L)
                .createdAt(LocalDateTime.now())
                .updatedAt(LocalDateTime.now())
                .build();

        when(paymentRepository.findById(PAYMENT_ID)).thenReturn(Optional.of(successPayment));

        // ── ACT ───────────────────────────────────────────────────────────────
        PaymentEvent event = new PaymentEvent(PAYMENT_ID, AMOUNT, CURRENCY, PaymentMethod.CARD, 0, 0L);
        kafkaTemplate.send(MAIN_TOPIC, PAYMENT_ID, event);

        // Give the consumer time to receive and process the message
        // (we use a short fixed wait here since we're asserting ABSENCE of calls)
        Thread.sleep(5_000);

        // ── ASSERT ────────────────────────────────────────────────────────────

        // Neither provider should have been called — consumer detected SUCCESS and returned early
        verify(providerAConnector, never()).processPayment(anyString(), any(), anyString());
        verify(providerBConnector, never()).processPayment(anyString(), any(), anyString());

        // No DB updates — the payment was already in a terminal state
        verify(paymentRepository, never()).updateOnSuccess(
                anyString(), any(), anyString(), anyString(), anyInt(), anyLong());
        verify(paymentRepository, never()).updateStatusWithVersionCheck(
                anyString(), eq(PaymentStatus.FAILED), anyInt(), anyLong());
    }

    // =========================================================================
    // TEST CASE: DLT handler is a no-op if payment is already FAILED
    //
    // Edge case: A stale DLT message arrives for a payment already marked FAILED
    // (perhaps from a previous retry cycle). The handler must detect this and skip.
    // =========================================================================

    @Test
    @DisplayName("DLT handler is a no-op when payment is already FAILED")
    void dltHandlerIsNoOpWhenPaymentAlreadyFailed() {

        // ── ARRANGE ──────────────────────────────────────────────────────────
        // Build the consumer directly (not via Kafka — testing the DLT method in isolation)
        PaymentRetryConsumer retryConsumer = new PaymentRetryConsumer(paymentRepository,
                new RoutingEngine(
                        (org.example.provider.ProviderAConnector) providerAConnector,
                        (org.example.provider.ProviderBConnector) providerBConnector
                ));

        Payment failedPayment = Payment.builder()
                .id(PAYMENT_ID)
                .idempotencyKey(IDEMPOTENCY_KEY)
                .amount(AMOUNT)
                .currency(CURRENCY)
                .paymentMethod(PaymentMethod.CARD)
                .status(PaymentStatus.FAILED)   // already FAILED
                .retryCount(4)
                .version(5L)
                .createdAt(LocalDateTime.now())
                .updatedAt(LocalDateTime.now())
                .build();

        when(paymentRepository.findById(PAYMENT_ID)).thenReturn(Optional.of(failedPayment));

        // ── ACT ───────────────────────────────────────────────────────────────
        ConsumerRecord<String, PaymentEvent> dltRecord = new ConsumerRecord<>(
                MAIN_TOPIC + "-dlt", 0, 100L, PAYMENT_ID,
                new PaymentEvent(PAYMENT_ID, AMOUNT, CURRENCY, PaymentMethod.CARD, 4, 4L)
        );
        retryConsumer.handleDlt(dltRecord, null);  // no exception header

        // ── ASSERT ────────────────────────────────────────────────────────────
        // updateStatusWithVersionCheck must NOT be called — payment is already FAILED
        verify(paymentRepository, never()).updateStatusWithVersionCheck(
                anyString(), eq(PaymentStatus.FAILED), anyInt(), anyLong());
    }

    // =========================================================================
    // TEST CASE: DLT handler handles null payment (defensive null-check)
    //
    // Edge case: A DLT message arrives for a payment ID that no longer exists in the DB.
    // The handler must log and return without throwing NullPointerException.
    // =========================================================================

    @Test
    @DisplayName("DLT handler handles null payment from DB without throwing NPE")
    void dltHandlerHandlesNullPaymentGracefully() {

        // ── ARRANGE ──────────────────────────────────────────────────────────
        PaymentRetryConsumer retryConsumer = new PaymentRetryConsumer(paymentRepository,
                new RoutingEngine(
                        (org.example.provider.ProviderAConnector) providerAConnector,
                        (org.example.provider.ProviderBConnector) providerBConnector
                ));

        // Payment does not exist in the DB (e.g., deleted by admin cleanup)
        when(paymentRepository.findById(PAYMENT_ID)).thenReturn(Optional.empty());

        ConsumerRecord<String, PaymentEvent> dltRecord = new ConsumerRecord<>(
                MAIN_TOPIC + "-dlt", 0, 200L, PAYMENT_ID,
                new PaymentEvent(PAYMENT_ID, AMOUNT, CURRENCY, PaymentMethod.CARD, 4, 4L)
        );

        // ── ACT & ASSERT ──────────────────────────────────────────────────────
        // Must NOT throw any exception
        org.assertj.core.api.Assertions.assertThatCode(() ->
                retryConsumer.handleDlt(dltRecord, null)
        ).doesNotThrowAnyException();

        // No DB mutation attempts
        verify(paymentRepository, never()).updateStatusWithVersionCheck(
                anyString(), any(), anyInt(), anyLong());
    }
}

