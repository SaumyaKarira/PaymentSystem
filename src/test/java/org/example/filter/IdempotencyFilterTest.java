package org.example.filter;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.example.controller.PaymentController;
import org.example.dto.CreatePaymentRequest;
import org.example.dto.PaymentResponse;
import org.example.entity.PaymentMethod;
import org.example.entity.PaymentStatus;
import org.example.exception.GlobalExceptionHandler;
import org.example.exception.PaymentNotFoundException;
import org.example.service.IdempotencyService;
import org.example.service.PaymentOrchestratorService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * IdempotencyFilterTest — web-slice tests for {@link IdempotencyFilter}.
 *
 * <h2>Why @WebMvcTest(controllers = PaymentController.class)</h2>
 * <p>{@code @WebMvcTest} starts only the Spring MVC layer — no JPA, no Redis, no Kafka.
 * Providing {@code controllers = PaymentController.class} explicitly tells Spring which
 * {@code @RestController} to register with the DispatcherServlet. This is the key step
 * that binds the route {@code /v1/payments} so it is recognized as a controller endpoint
 * rather than a phantom static-resource path. Without it (or without the controller being
 * picked up by component scan), any request to {@code /v1/payments} falls through to
 * Spring's default static-resource handler and throws {@code NoResourceFoundException}.
 *
 * <h2>Why @Import({IdempotencyFilter.class, GlobalExceptionHandler.class, ...})</h2>
 * <p>{@code @WebMvcTest} does NOT auto-scan {@code @Component} beans outside the narrow
 * web layer. We must explicitly import:
 * <ul>
 *   <li>{@link IdempotencyFilter} — the Servlet filter under test. Importing it as a bean
 *       causes Spring Boot's test infrastructure to register it in the MockMvc filter chain
 *       automatically, because it extends {@code OncePerRequestFilter}.</li>
 *   <li>{@link GlobalExceptionHandler} — the {@code @RestControllerAdvice} that maps
 *       exceptions thrown by the controller to structured JSON error responses. Without this,
 *       exceptions bubble up as raw 500s with no JSON body.</li>
 *   <li>{@code TestRedisObjectMapperConfig} — provides the {@code @Qualifier("redisObjectMapper")}
 *       bean that {@link IdempotencyFilter}'s constructor requires. Without this, the
 *       application context fails to start with a {@code NoSuchBeanDefinitionException}.</li>
 * </ul>
 *
 * <h2>Why @MockBean for services</h2>
 * <p>{@code @MockBean} replaces real beans in the application context with Mockito mocks,
 * preventing any attempt to connect to Redis, MySQL, or Kafka. The filter and controller
 * are the only real beans in this slice; every other collaborator is a test double.
 */
@WebMvcTest(controllers = PaymentController.class)
@Import({
        IdempotencyFilter.class,
        GlobalExceptionHandler.class,
        IdempotencyFilterTest.TestRedisObjectMapperConfig.class
})
@DisplayName("IdempotencyFilter — Web Slice Tests")
class IdempotencyFilterTest {

    // =========================================================================
    // Inner configuration — provides the redisObjectMapper bean
    // =========================================================================

    /**
     * Minimal {@code @Configuration} supplying the {@code redisObjectMapper} bean
     * required by {@link IdempotencyFilter}'s constructor.
     *
     * <p>In production this bean lives in {@code RedisConfig}. Since {@code @WebMvcTest}
     * does not load infrastructure configuration classes, we provide a test-local
     * replacement that is imported via {@code @Import} at the class level.
     */
    @Configuration
    static class TestRedisObjectMapperConfig {
        @Bean("redisObjectMapper")
        public ObjectMapper redisObjectMapper() {
            return new ObjectMapper()
                    .registerModule(new JavaTimeModule())
                    .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
        }
    }

    // =========================================================================
    // Test Fixtures
    // =========================================================================

    @Autowired
    private MockMvc mockMvc;

    @MockBean
    private IdempotencyService idempotencyService;

    @MockBean
    private PaymentOrchestratorService paymentOrchestratorService;

    private ObjectMapper objectMapper;

    private static final String PAYMENTS_URL    = "/v1/payments";
    private static final String IDEM_HEADER     = "Idempotency-Key";
    private static final String PAYMENT_ID      = "550e8400-e29b-41d4-a716-446655440001";
    private static final String IDEMPOTENCY_KEY = "unique-key-for-test-001";
    private static final String PROVIDER_A_ID   = "PROVIDER_A";
    private static final String PROVIDER_A_REF  = "PROVA-CACHEDREF12345";

    @BeforeEach
    void setUp() {
        objectMapper = new ObjectMapper()
                .registerModule(new JavaTimeModule())
                .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
        // Reset all mock state before each test to prevent cross-test pollution.
        reset(idempotencyService, paymentOrchestratorService);
    }

    // =========================================================================
    // Helpers
    // =========================================================================

    private String validCardBody() {
        return """
                {"amount": 150.00, "currency": "USD", "paymentMethod": "CARD"}
                """;
    }

    private PaymentResponse cachedSuccessResponse() {
        return new PaymentResponse(
                PAYMENT_ID, IDEMPOTENCY_KEY, new BigDecimal("150.00"), "USD",
                PaymentMethod.CARD, PaymentStatus.SUCCESS,
                PROVIDER_A_ID, PROVIDER_A_REF, 0,
                LocalDateTime.now().minusMinutes(2), LocalDateTime.now().minusMinutes(2));
    }

    // =========================================================================
    // CATEGORY A — Filter pass-through (valid requests reach the controller)
    // =========================================================================

    @Nested
    @DisplayName("Category A — Filter Pass-Through Scenarios")
    class FilterPassThroughTests {

        /**
         * TC-01: A valid POST with a fresh Idempotency-Key (not in-flight, not cached)
         * must pass all filter checks, reach the controller, and return HTTP 201.
         */
        @Test
        @DisplayName("TC-01: Valid POST with new Idempotency-Key passes filter → HTTP 201 Created")
        void validPostPassesThroughFilter() throws Exception {
            when(idempotencyService.isInFlight(IDEMPOTENCY_KEY)).thenReturn(false);
            when(idempotencyService.getCachedResponse(IDEMPOTENCY_KEY)).thenReturn(Optional.empty());
            when(paymentOrchestratorService.createPayment(any(CreatePaymentRequest.class), anyString()))
                    .thenReturn(cachedSuccessResponse());

            mockMvc.perform(post(PAYMENTS_URL)
                            .header(IDEM_HEADER, IDEMPOTENCY_KEY)
                            .contentType(MediaType.APPLICATION_JSON)
                            .content(validCardBody()))
                    .andExpect(status().isCreated())
                    .andExpect(jsonPath("$.status").value("SUCCESS"))
                    .andExpect(jsonPath("$.id").value(PAYMENT_ID))
                    .andExpect(jsonPath("$.paymentMethod").value("CARD"));

            verify(idempotencyService).isInFlight(IDEMPOTENCY_KEY);
            verify(idempotencyService).getCachedResponse(IDEMPOTENCY_KEY);
            verify(paymentOrchestratorService).createPayment(any(), anyString());
        }

        /**
         * TC-02: GET requests must bypass IdempotencyFilter — the filter's scope check
         * ({@code !"POST".equalsIgnoreCase(method)}) passes them through immediately.
         */
        @Test
        @DisplayName("TC-02: GET requests bypass IdempotencyFilter — no idempotency checks performed")
        void getRequestsBypassFilter() throws Exception {
            when(paymentOrchestratorService.getPayment(PAYMENT_ID))
                    .thenReturn(cachedSuccessResponse());

            mockMvc.perform(get(PAYMENTS_URL + "/" + PAYMENT_ID))
                    .andExpect(status().isOk())
                    .andExpect(jsonPath("$.id").value(PAYMENT_ID));

            verify(idempotencyService, never()).isInFlight(anyString());
            verify(idempotencyService, never()).getCachedResponse(anyString());
        }
    }

    // =========================================================================
    // CATEGORY B — Filter short-circuits (rejected requests)
    // =========================================================================

    @Nested
    @DisplayName("Category B — Filter Rejection Scenarios")
    class FilterRejectionTests {

        /**
         * TC-03A: Missing Idempotency-Key header — filter writes 400 directly and
         * never calls the filter chain, so the controller is never reached.
         */
        @Test
        @DisplayName("TC-03A: Missing Idempotency-Key header → HTTP 400 Bad Request")
        void missingIdempotencyKeyHeader_returns400() throws Exception {
            mockMvc.perform(post(PAYMENTS_URL)
                            .contentType(MediaType.APPLICATION_JSON)
                            .content(validCardBody()))
                    .andExpect(status().isBadRequest())
                    .andExpect(jsonPath("$.message", containsString("Idempotency-Key")));

            verify(paymentOrchestratorService, never())
                    .createPayment(any(CreatePaymentRequest.class), anyString());
        }

        /**
         * TC-03B: Whitespace-only Idempotency-Key — {@code isBlank()} returns true,
         * filter rejects with 400 before reaching the controller.
         */
        @Test
        @DisplayName("TC-03B: Blank Idempotency-Key header → HTTP 400 Bad Request")
        void blankIdempotencyKeyHeader_returns400() throws Exception {
            mockMvc.perform(post(PAYMENTS_URL)
                            .header(IDEM_HEADER, "   ")
                            .contentType(MediaType.APPLICATION_JSON)
                            .content(validCardBody()))
                    .andExpect(status().isBadRequest());

            verify(paymentOrchestratorService, never())
                    .createPayment(any(CreatePaymentRequest.class), anyString());
        }

        /**
         * TC-03C: Header is valid so the filter passes the request through, but a
         * negative amount fails {@code @Valid} Bean Validation in the controller.
         * {@code GlobalExceptionHandler} returns 400 with a {@code fieldErrors} array.
         */
        @Test
        @DisplayName("TC-03C: Negative amount fails Bean Validation → HTTP 400 with fieldErrors")
        void negativeAmount_returns400WithFieldErrors() throws Exception {
            // The filter passes — stub it to let the request through
            when(idempotencyService.isInFlight("valid-key-bad-body")).thenReturn(false);
            when(idempotencyService.getCachedResponse("valid-key-bad-body")).thenReturn(Optional.empty());

            String body = """
                    {"amount": -50.00, "currency": "USD", "paymentMethod": "CARD"}
                    """;

            mockMvc.perform(post(PAYMENTS_URL)
                            .header(IDEM_HEADER, "valid-key-bad-body")
                            .contentType(MediaType.APPLICATION_JSON)
                            .content(body))
                    .andExpect(status().isBadRequest())
                    .andExpect(jsonPath("$.fieldErrors").isArray());

            verify(paymentOrchestratorService, never())
                    .createPayment(any(CreatePaymentRequest.class), anyString());
        }

        /**
         * TC-03D: Invalid enum value for {@code paymentMethod} causes
         * {@code HttpMessageNotReadableException} during JSON deserialization.
         * Mapped to 400 by {@code GlobalExceptionHandler.handleMalformedBody()}.
         */
        @Test
        @DisplayName("TC-03D: Invalid paymentMethod enum value → HTTP 400 Bad Request")
        void invalidPaymentMethodEnum_returns400() throws Exception {
            when(idempotencyService.isInFlight("valid-key-enum-test")).thenReturn(false);
            when(idempotencyService.getCachedResponse("valid-key-enum-test")).thenReturn(Optional.empty());

            String body = """
                    {"amount": 100.00, "currency": "USD", "paymentMethod": "WIRE"}
                    """;

            mockMvc.perform(post(PAYMENTS_URL)
                            .header(IDEM_HEADER, "valid-key-enum-test")
                            .contentType(MediaType.APPLICATION_JSON)
                            .content(body))
                    .andExpect(status().isBadRequest());

            verify(paymentOrchestratorService, never())
                    .createPayment(any(CreatePaymentRequest.class), anyString());
        }

        /**
         * TC-03E: Missing {@code currency} field fails Bean Validation. The
         * {@code fieldErrors} array must contain an entry for the {@code currency} field.
         */
        @Test
        @DisplayName("TC-03E: Null currency fails Bean Validation → HTTP 400 with currency field error")
        void nullCurrency_returns400WithCurrencyFieldError() throws Exception {
            when(idempotencyService.isInFlight("valid-key-no-currency")).thenReturn(false);
            when(idempotencyService.getCachedResponse("valid-key-no-currency")).thenReturn(Optional.empty());

            String body = """
                    {"amount": 100.00, "paymentMethod": "CARD"}
                    """;

            mockMvc.perform(post(PAYMENTS_URL)
                            .header(IDEM_HEADER, "valid-key-no-currency")
                            .contentType(MediaType.APPLICATION_JSON)
                            .content(body))
                    .andExpect(status().isBadRequest())
                    .andExpect(jsonPath("$.fieldErrors").isArray())
                    .andExpect(jsonPath("$.fieldErrors[?(@.field == 'currency')]").exists());

            verify(paymentOrchestratorService, never())
                    .createPayment(any(CreatePaymentRequest.class), anyString());
        }

        /**
         * TC-05: In-flight Idempotency-Key → HTTP 409 Conflict, controller not reached.
         *
         * <p>When {@code isInFlight(key)} returns {@code true}, the filter writes a 409
         * directly to the response and returns without calling {@code filterChain.doFilter()}.
         * Therefore the controller and {@code getCachedResponse()} are never invoked.
         */
        @Test
        @DisplayName("TC-05: In-flight Idempotency-Key → HTTP 409 Conflict, controller not reached")
        void inFlightKey_returns409Conflict() throws Exception {
            when(idempotencyService.isInFlight(IDEMPOTENCY_KEY)).thenReturn(true);

            mockMvc.perform(post(PAYMENTS_URL)
                            .header(IDEM_HEADER, IDEMPOTENCY_KEY)
                            .contentType(MediaType.APPLICATION_JSON)
                            .content(validCardBody()))
                    .andExpect(status().isConflict())
                    .andExpect(jsonPath("$.message", containsString(IDEMPOTENCY_KEY)));

            // Controller must NOT have been reached
            verify(paymentOrchestratorService, never())
                    .createPayment(any(CreatePaymentRequest.class), anyString());
            // getCachedResponse must NOT be called — in-flight check short-circuits first
            verify(idempotencyService, never()).getCachedResponse(anyString());
        }

        /**
         * TC-04: Redis cache hit → HTTP 200 OK with cached body, controller bypassed.
         *
         * <p>When {@code getCachedResponse(key)} returns a non-empty Optional, the filter
         * serializes the {@link PaymentResponse} directly to the HTTP response (status 200)
         * and returns without calling {@code filterChain.doFilter()}. The controller is
         * never invoked — no new payment is created.
         */
        @Test
        @DisplayName("TC-04: Redis cache hit → HTTP 200 with cached body, controller bypassed")
        void cacheHit_returns200WithCachedBody() throws Exception {
            when(idempotencyService.isInFlight(IDEMPOTENCY_KEY)).thenReturn(false);
            when(idempotencyService.getCachedResponse(IDEMPOTENCY_KEY))
                    .thenReturn(Optional.of(cachedSuccessResponse()));

            MvcResult result = mockMvc.perform(post(PAYMENTS_URL)
                            .header(IDEM_HEADER, IDEMPOTENCY_KEY)
                            .contentType(MediaType.APPLICATION_JSON)
                            .content(validCardBody()))
                    .andExpect(status().isOk())
                    .andReturn();

            PaymentResponse returned = objectMapper.readValue(
                    result.getResponse().getContentAsString(), PaymentResponse.class);

            assertThat(returned.id()).isEqualTo(PAYMENT_ID);
            assertThat(returned.status()).isEqualTo(PaymentStatus.SUCCESS);
            assertThat(returned.paymentMethod()).isEqualTo(PaymentMethod.CARD);
            assertThat(returned.amount()).isEqualByComparingTo(new BigDecimal("150.00"));
            assertThat(returned.currency()).isEqualTo("USD");
            assertThat(returned.providerId()).isEqualTo(PROVIDER_A_ID);
            assertThat(returned.providerReferenceId()).isEqualTo(PROVIDER_A_REF);
            assertThat(returned.retryCount()).isEqualTo(0);

            // Controller must NOT have been reached — filter short-circuited on cache hit
            verify(paymentOrchestratorService, never())
                    .createPayment(any(CreatePaymentRequest.class), anyString());
        }
    }

    // =========================================================================
    // CATEGORY C — GlobalExceptionHandler error mapping
    // =========================================================================

    @Nested
    @DisplayName("Category C — GlobalExceptionHandler Error Mapping")
    class GlobalExceptionHandlerTests {

        /**
         * TC-08: GET with an unknown payment ID — service throws
         * {@link PaymentNotFoundException}, mapped to HTTP 404 with a structured body.
         */
        @Test
        @DisplayName("TC-08: GET with unknown payment ID → HTTP 404 structured response")
        void unknownPaymentId_returns404() throws Exception {
            String unknownId = "00000000-ffff-0000-ffff-000000000000";
            when(paymentOrchestratorService.getPayment(unknownId))
                    .thenThrow(new PaymentNotFoundException(unknownId));

            mockMvc.perform(get(PAYMENTS_URL + "/" + unknownId))
                    .andExpect(status().isNotFound())
                    .andExpect(jsonPath("$.status").value(404))
                    .andExpect(jsonPath("$.error").value("Not Found"))
                    .andExpect(jsonPath("$.message", containsString(unknownId)));
        }

        /**
         * TC-09: Error response {@code timestamp} must be an ISO-8601 string — not a
         * numeric array — validating that {@code JavaTimeModule} is active in the context.
         */
        @Test
        @DisplayName("TC-09: Error response timestamp is an ISO-8601 string")
        void errorResponseTimestamp_isIso8601String() throws Exception {
            when(paymentOrchestratorService.getPayment(anyString()))
                    .thenThrow(new PaymentNotFoundException("any-id"));

            mockMvc.perform(get(PAYMENTS_URL + "/any-id"))
                    .andExpect(status().isNotFound())
                    .andExpect(jsonPath("$.timestamp").isString());
        }
    }
}

