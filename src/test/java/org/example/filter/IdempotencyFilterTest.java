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
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * IdempotencyFilterTest — web slice tests for IdempotencyFilter.
 *
 * <h2>Why @WebMvcTest with explicit @Import</h2>
 * <p>@WebMvcTest starts only the web layer. We explicitly import:
 * <ul>
 *   <li>{@code IdempotencyFilter} — the component under test</li>
 *   <li>{@code GlobalExceptionHandler} — so 404/409/400 responses are structured JSON</li>
 *   <li>{@code TestRedisObjectMapperConfig} — provides the {@code redisObjectMapper} bean
 *       that {@code IdempotencyFilter} requires via {@code @Qualifier("redisObjectMapper")}.
 *       Without this, the filter cannot be wired and the context fails to start.</li>
 * </ul>
 *
 * <h2>@MockBean usage</h2>
 * <p>{@code IdempotencyService} and {@code PaymentOrchestratorService} are replaced with
 * Mockito mocks so no real Redis or DB calls are made during web slice tests.
 */
@WebMvcTest(controllers = PaymentController.class)
@Import({
        IdempotencyFilter.class,
        GlobalExceptionHandler.class,
        IdempotencyFilterTest.TestRedisObjectMapperConfig.class
})
@DisplayName("IdempotencyFilter — Web Slice Tests")
class IdempotencyFilterTest {

    /**
     * Inner @Configuration class that provides the redisObjectMapper bean
     * required by IdempotencyFilter in the @WebMvcTest slice context.
     * In production this bean comes from RedisConfig, but RedisConfig is not
     * loaded by @WebMvcTest. This minimal config provides only what the filter needs.
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

    @Autowired
    private MockMvc mockMvc;

    @MockBean
    private IdempotencyService idempotencyService;

    @MockBean
    private PaymentOrchestratorService paymentOrchestratorService;

    // Used to deserialize cached responses in cache-hit assertions
    private ObjectMapper objectMapper;

    private static final String PAYMENTS_URL    = "/v1/payments";
    private static final String IDEM_HEADER     = "Idempotency-Key";
    private static final String PAYMENT_ID      = "550e8400-e29b-41d4-a716-446655440001";
    private static final String IDEMPOTENCY_KEY = "unique-key-for-test-001";

    @BeforeEach
    void setUp() {
        objectMapper = new ObjectMapper()
                .registerModule(new JavaTimeModule())
                .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    private String validCardBody() {
        return """
                {"amount": 150.00, "currency": "USD", "paymentMethod": "CARD"}
                """;
    }

    private PaymentResponse cachedSuccessResponse() {
        return new PaymentResponse(
                PAYMENT_ID, IDEMPOTENCY_KEY, new BigDecimal("150.00"), "USD",
                PaymentMethod.CARD, PaymentStatus.SUCCESS,
                "PROVIDER_A", "PROVA-CACHEDREF12345", 0,
                LocalDateTime.now().minusMinutes(2), LocalDateTime.now().minusMinutes(2));
    }

    // =========================================================================
    // CATEGORY A — Filter pass-through (valid requests reach the controller)
    // =========================================================================

    @Nested
    @DisplayName("Category A — Filter pass-through for valid requests")
    class FilterPassThroughTests {

        @Test
        @DisplayName("Valid POST with new Idempotency-Key passes filter → controller called → HTTP 201")
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
                    .andExpect(jsonPath("$.id").value(PAYMENT_ID));

            verify(idempotencyService).isInFlight(IDEMPOTENCY_KEY);
            verify(idempotencyService).getCachedResponse(IDEMPOTENCY_KEY);
        }

        @Test
        @DisplayName("GET requests bypass IdempotencyFilter — no idempotency checks performed")
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
    @DisplayName("Category B — Filter rejection scenarios")
    class FilterRejectionTests {

        @Test
        @DisplayName("TC-07A: Missing Idempotency-Key header → HTTP 400, controller not reached")
        void missingIdempotencyKeyHeader_returns400() throws Exception {
            mockMvc.perform(post(PAYMENTS_URL)
                            .contentType(MediaType.APPLICATION_JSON)
                            .content(validCardBody()))
                    .andExpect(status().isBadRequest())
                    .andExpect(jsonPath("$.message", containsString("Idempotency-Key")));

            verify(paymentOrchestratorService, never())
                    .createPayment(any(CreatePaymentRequest.class), anyString());
        }

        @Test
        @DisplayName("TC-07B: Blank Idempotency-Key header → HTTP 400")
        void blankIdempotencyKeyHeader_returns400() throws Exception {
            mockMvc.perform(post(PAYMENTS_URL)
                            .header(IDEM_HEADER, "   ")
                            .contentType(MediaType.APPLICATION_JSON)
                            .content(validCardBody()))
                    .andExpect(status().isBadRequest());

            verify(paymentOrchestratorService, never())
                    .createPayment(any(CreatePaymentRequest.class), anyString());
        }

        @Test
        @DisplayName("TC-07C: Negative amount fails Bean Validation → HTTP 400 with fieldErrors")
        void negativeAmount_returns400WithFieldErrors() throws Exception {
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

        @Test
        @DisplayName("TC-07D: Invalid paymentMethod enum → HTTP 400 Bad Request")
        void invalidPaymentMethodEnum_returns400() throws Exception {
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

        @Test
        @DisplayName("TC-07E: Null currency fails Bean Validation → HTTP 400 fieldErrors contains 'currency'")
        void nullCurrency_returns400WithCurrencyFieldError() throws Exception {
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

        @Test
        @DisplayName("TC-06: In-flight Idempotency-Key → HTTP 409, controller not reached")
        void inFlightKey_returns409Conflict() throws Exception {
            when(idempotencyService.isInFlight(IDEMPOTENCY_KEY)).thenReturn(true);

            mockMvc.perform(post(PAYMENTS_URL)
                            .header(IDEM_HEADER, IDEMPOTENCY_KEY)
                            .contentType(MediaType.APPLICATION_JSON)
                            .content(validCardBody()))
                    .andExpect(status().isConflict())
                    .andExpect(jsonPath("$.message", containsString(IDEMPOTENCY_KEY)));

            verify(paymentOrchestratorService, never())
                    .createPayment(any(CreatePaymentRequest.class), anyString());
            verify(idempotencyService, never()).getCachedResponse(anyString());
        }

        @Test
        @DisplayName("TC-04: Cache hit in Redis → HTTP 200 with cached body, controller bypassed")
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
            assertThat(returned.providerReferenceId()).isEqualTo("PROVA-CACHEDREF12345");

            verify(paymentOrchestratorService, never())
                    .createPayment(any(CreatePaymentRequest.class), anyString());
        }
    }

    // =========================================================================
    // CATEGORY C — GlobalExceptionHandler mapping
    // =========================================================================

    @Nested
    @DisplayName("Category C — GlobalExceptionHandler error mapping")
    class GlobalExceptionHandlerTests {

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

        @Test
        @DisplayName("Error response timestamp is a string, not a numeric array")
        void errorResponseTimestamp_isIso8601String() throws Exception {
            when(paymentOrchestratorService.getPayment(anyString()))
                    .thenThrow(new PaymentNotFoundException("any-id"));

            mockMvc.perform(get(PAYMENTS_URL + "/any-id"))
                    .andExpect(status().isNotFound())
                    .andExpect(jsonPath("$.timestamp").isString());
        }
    }
}

