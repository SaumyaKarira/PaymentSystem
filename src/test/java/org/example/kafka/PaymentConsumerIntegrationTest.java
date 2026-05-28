package org.example.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.example.dto.PaymentEvent;
import org.example.entity.Payment;
import org.example.entity.PaymentMethod;
import org.example.entity.PaymentStatus;
import org.example.exception.ProviderException;
import org.example.provider.ProviderAConnector;
import org.example.provider.ProviderBConnector;
import org.example.repository.PaymentRepository;
import org.example.routing.RoutingEngine;
import org.example.service.IdempotencyService;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.ContainerTestUtils;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.TestPropertySource;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * PaymentConsumerIntegrationTest — full integration tests for the Kafka retry pipeline.
 *
 * <h2>Infrastructure</h2>
 * <ul>
 *   <li>{@code @EmbeddedKafka} — in-process Kafka broker, no external Kafka needed</li>
 *   <li>{@code @MockBean} for all DB/Redis dependencies — no MySQL or Redis needed</li>
 *   <li>{@code @SpringBootTest(webEnvironment=NONE)} — full context minus HTTP server</li>
 * </ul>
 *
 * <h2>@DirtiesContext</h2>
 * <p>Forces a fresh Spring context after each test method. This prevents the Kafka
 * consumer from seeing uncommitted offsets from a previous test's published messages.
 *
 * <h2>@TestPropertySource</h2>
 * <p>Points Kafka bootstrap-servers at the embedded broker's dynamically assigned port.
 * Also disables JPA/Redis auto-configuration since all those dependencies are @MockBean.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE)
@EmbeddedKafka(
        partitions = 1,
        topics = {
                "payment-main-topic",
                "payment-main-topic-retry-0",
                "payment-main-topic-retry-1",
                "payment-main-topic-retry-2",
                "payment-main-topic-dlt"
        },
        brokerProperties = {"auto.create.topics.enable=false"}
)
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD)
@TestPropertySource(properties = {
        "spring.kafka.bootstrap-servers=${spring.embedded.kafka.brokers}",
        "payment.kafka.main-topic=payment-main-topic",
        "spring.autoconfigure.exclude=" +
                "org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration," +
                "org.springframework.boot.autoconfigure.orm.jpa.HibernateJpaAutoConfiguration," +
                "org.springframework.boot.autoconfigure.data.jpa.JpaRepositoriesAutoConfiguration," +
                "org.springframework.boot.autoconfigure.data.redis.RedisAutoConfiguration," +
                "org.springframework.boot.autoconfigure.data.redis.RedisRepositoriesAutoConfiguration",
        "spring.jpa.hibernate.ddl-auto=none",
        "spring.kafka.consumer.auto-offset-reset=earliest",
        "spring.main.allow-bean-definition-overriding=true"

})
@DisplayName("PaymentConsumerIntegrationTest — Kafka Retry Pipeline Integration Tests")
class PaymentConsumerIntegrationTest {

    @Autowired
    private EmbeddedKafkaBroker embeddedKafkaBroker;

    @Autowired
    private KafkaTemplate<String, PaymentEvent> kafkaTemplate;

    @Autowired
    private KafkaListenerEndpointRegistry registry;

    // ── @MockBean for all external dependencies ───────────────────────────────

    @MockBean
    private PaymentRepository paymentRepository;

    // Mock as concrete types so RoutingEngine can be wired by Spring directly
    @MockBean
    private ProviderAConnector providerAConnector;

    @MockBean
    private ProviderBConnector providerBConnector;

    @MockBean
    private IdempotencyService idempotencyService;

    // RedisTemplate is required by IdempotencyService constructor — mock it so
    // the context can start without a real Redis connection
    @MockBean
    private RedisTemplate<String, String> redisTemplate;

    // ── Constants ───────────────────────────���─────────────────────────────────

    private static final String PAYMENT_ID    = "test-payment-id-kafka-001";
    private static final String IDEM_KEY      = "test-idem-key-kafka-001";
    private static final String MAIN_TOPIC    = "payment-main-topic";
    private static final String PROVIDER_A_ID = "PROVIDER_A";
    private static final String PROVIDER_B_ID = "PROVIDER_B";
    private static final String PROVIDER_B_REF = "PROVB-KAFKA-TEST-SUCCESS";
    private static final BigDecimal AMOUNT    = new BigDecimal("250.00");
    private static final String CURRENCY      = "USD";
    private static final long AWAIT_TIMEOUT_SECONDS = 30L;

    @BeforeEach
    void waitForConsumerAssignment() {
        // Wait until each listener container has been fully assigned to its partitions
        // on the embedded broker before publishing test messages — prevents race conditions
        // where a message is published before the consumer has joined the consumer group.
        registry.getListenerContainers().forEach(container ->
                ContainerTestUtils.waitForAssignment(container,
                        embeddedKafkaBroker.getPartitionsPerTopic()));
    }

    @AfterEach
    void resetMocks() {
        reset(paymentRepository, providerAConnector, providerBConnector);
    }

    // ── Helper ────────────────────────────────────────────────────────────────

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
    // All 4 attempts fail → DLT fires → payment marked FAILED
    // =========================================================================

    @Test
    @DisplayName("All 4 Kafka attempts fail → DLT fires → payment marked FAILED")
    void tc11_allAttemptsExhausted_dltMarksPaymentFailed() {
        when(paymentRepository.findById(PAYMENT_ID))
                .thenReturn(Optional.of(processingPayment(0, 1L)));

        // Both providers always fail — forces all retries to exhaust
        when(providerAConnector.getProviderId()).thenReturn(PROVIDER_A_ID);
        when(providerAConnector.processPayment(anyString(), any(), anyString()))
                .thenThrow(new ProviderException(PROVIDER_A_ID, "504 Gateway Timeout"));

        when(providerBConnector.getProviderId()).thenReturn(PROVIDER_B_ID);
        when(providerBConnector.processPayment(anyString(), any(), anyString()))
                .thenThrow(new ProviderException(PROVIDER_B_ID, "500 Internal Server Error"));

        when(paymentRepository.updateStatusWithVersionCheck(
                anyString(), any(PaymentStatus.class), anyInt(), anyLong()))
                .thenReturn(1);

        kafkaTemplate.send(MAIN_TOPIC, PAYMENT_ID,
                new PaymentEvent(PAYMENT_ID, AMOUNT, CURRENCY, PaymentMethod.CARD, 0, 1L));

        // Wait until the DLT handler fires and marks the payment FAILED
        await().atMost(AWAIT_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                .untilAsserted(() ->
                        verify(paymentRepository, atLeast(1)).updateStatusWithVersionCheck(
                                eq(PAYMENT_ID), eq(PaymentStatus.FAILED), anyInt(), anyLong()));

        // SUCCESS must never have been called — all providers failed
        verify(paymentRepository, never()).updateOnSuccess(
                anyString(), any(), anyString(), anyString(), anyInt(), anyLong());
    }

    // =========================================================================
    // Consumer skips payments already in SUCCESS state
    // =========================================================================

    @Test
    @DisplayName("Consumer is a no-op when payment is already in SUCCESS state")
    void consumerSkipsAlreadySuccessfulPayment() throws Exception {
        Payment alreadySuccess = Payment.builder()
                .id(PAYMENT_ID).idempotencyKey(IDEM_KEY).amount(AMOUNT).currency(CURRENCY)
                .paymentMethod(PaymentMethod.CARD).status(PaymentStatus.SUCCESS)
                .providerId(PROVIDER_A_ID).providerReferenceId("PROVA-ALREADY-DONE")
                .retryCount(0).version(1L)
                .createdAt(LocalDateTime.now()).updatedAt(LocalDateTime.now())
                .build();

        when(paymentRepository.findById(PAYMENT_ID)).thenReturn(Optional.of(alreadySuccess));

        kafkaTemplate.send(MAIN_TOPIC, PAYMENT_ID,
                new PaymentEvent(PAYMENT_ID, AMOUNT, CURRENCY, PaymentMethod.CARD, 0, 0L));

        // Give the consumer time to process (fixed short wait since we assert absence)
        Thread.sleep(5_000);

        verify(providerAConnector, never()).processPayment(anyString(), any(), anyString());
        verify(providerBConnector, never()).processPayment(anyString(), any(), anyString());
        verify(paymentRepository, never()).updateOnSuccess(
                anyString(), any(), anyString(), anyString(), anyInt(), anyLong());
    }

    // =========================================================================
    // DLT handler is a no-op when payment is already FAILED (direct invocation test)
    // =========================================================================

    @Test
    @DisplayName("DLT handler is no-op when payment is already in FAILED state")
    void dltHandler_noOp_whenPaymentAlreadyFailed() {
        RoutingEngine realRouter = new RoutingEngine(providerAConnector, providerBConnector);
        PaymentRetryConsumer retryConsumer =
                new PaymentRetryConsumer(paymentRepository, realRouter);

        Payment alreadyFailed = Payment.builder()
                .id(PAYMENT_ID).idempotencyKey(IDEM_KEY).amount(AMOUNT).currency(CURRENCY)
                .paymentMethod(PaymentMethod.CARD).status(PaymentStatus.FAILED)
                .retryCount(4).version(5L)
                .createdAt(LocalDateTime.now()).updatedAt(LocalDateTime.now())
                .build();

        when(paymentRepository.findById(PAYMENT_ID)).thenReturn(Optional.of(alreadyFailed));

        ConsumerRecord<String, PaymentEvent> dltRecord = new ConsumerRecord<>(
                MAIN_TOPIC + "-dlt", 0, 100L, PAYMENT_ID,
                new PaymentEvent(PAYMENT_ID, AMOUNT, CURRENCY, PaymentMethod.CARD, 4, 4L));

        retryConsumer.handleDlt(dltRecord, null);

        // Already FAILED → updateStatusWithVersionCheck must NOT be called again
        verify(paymentRepository, never()).updateStatusWithVersionCheck(
                anyString(), eq(PaymentStatus.FAILED), anyInt(), anyLong());
    }

    // =========================================================================
    // DLT handler handles missing payment gracefully without NPE
    // =========================================================================

    @Test
    @DisplayName("DLT handler handles non-existent payment without throwing NPE")
    void dltHandler_handlesNonExistentPaymentGracefully() {
        RoutingEngine realRouter = new RoutingEngine(providerAConnector, providerBConnector);
        PaymentRetryConsumer retryConsumer =
                new PaymentRetryConsumer(paymentRepository, realRouter);

        when(paymentRepository.findById(PAYMENT_ID)).thenReturn(Optional.empty());

        ConsumerRecord<String, PaymentEvent> dltRecord = new ConsumerRecord<>(
                MAIN_TOPIC + "-dlt", 0, 200L, PAYMENT_ID,
                new PaymentEvent(PAYMENT_ID, AMOUNT, CURRENCY, PaymentMethod.CARD, 4, 4L));

        assertThatCode(() -> retryConsumer.handleDlt(dltRecord, null))
                .doesNotThrowAnyException();

        verify(paymentRepository, never()).updateStatusWithVersionCheck(
                anyString(), any(), anyInt(), anyLong());
    }
}

