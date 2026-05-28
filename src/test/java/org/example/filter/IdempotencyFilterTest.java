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
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.http.MediaType;
import org.springframework.http.converter.json.MappingJackson2HttpMessageConverter;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

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

// Standalone MockMvc tests for IdempotencyFilter.
// Uses standaloneSetup (not @WebMvcTest) to avoid Spring context issues with the filter's bean wiring.
// Filter is constructed manually with mocked dependencies and added via .addFilters().
@ExtendWith(MockitoExtension.class)
@DisplayName("IdempotencyFilter — Standalone MockMvc Tests")
class IdempotencyFilterTest {

    // ── Mocks injected by MockitoExtension ────────────────────────────────────

    @Mock
    private IdempotencyService idempotencyService;

    @Mock
    private PaymentOrchestratorService paymentOrchestratorService;

    // ── Test infrastructure ───────────────────────────────────────────────────

    private MockMvc mockMvc;
    private ObjectMapper objectMapper;

    // ── Constants ──────────────────────────────────────────────────────────��──

    private static final String PAYMENTS_URL    = "/v1/payments";
    private static final String IDEM_HEADER     = "Idempotency-Key";
    private static final String PAYMENT_ID      = "550e8400-e29b-41d4-a716-446655440001";
    private static final String IDEMPOTENCY_KEY = "unique-key-for-test-001";
    private static final String PROVIDER_A_ID   = "PROVIDER_A";
    private static final String PROVIDER_A_REF  = "PROVA-CACHEDREF12345";

    @BeforeEach
    void setUp() {
        // JavaTimeModule: LocalDateTime serialized as ISO-8601 string, not a numeric array
        objectMapper = new ObjectMapper()
                .registerModule(new JavaTimeModule())
                .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);

        // Filter and controller are real instances with mocked collaborators
        IdempotencyFilter filter = new IdempotencyFilter(idempotencyService, objectMapper);
        PaymentController controller = new PaymentController(paymentOrchestratorService);

        // standaloneSetup: wires controller + filter + exception handler without a Spring context
        mockMvc = MockMvcBuilders
                .standaloneSetup(controller)
                .addFilters(filter)
                .setControllerAdvice(new GlobalExceptionHandler())
                .setMessageConverters(new MappingJackson2HttpMessageConverter(objectMapper))
                .build();
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

        // Fresh key, not in-flight, not cached — request passes through to controller → 201
        @Test
        @DisplayName("Valid POST with new Idempotency-Key passes filter → HTTP 201 Created")
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

        // GET bypasses the filter entirely — no idempotency checks performed
        @Test
        @DisplayName("GET requests bypass IdempotencyFilter — no idempotency checks performed")
        void getRequestsBypassFilter() throws Exception {
            when(paymentOrchestratorService.getPayment(PAYMENT_ID))
                    .thenReturn(cachedSuccessResponse());

            mockMvc.perform(get(PAYMENTS_URL + "/" + PAYMENT_ID))
                    .andExpect(status().isOk())
                    .andExpect(jsonPath("$.id").value(PAYMENT_ID))
                    .andExpect(jsonPath("$.status").value("SUCCESS"));

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

        // Missing Idempotency-Key header → filter rejects with 400, controller not reached
        @Test
        @DisplayName("Missing Idempotency-Key header → HTTP 400 Bad Request")
        void missingIdempotencyKeyHeader_returns400() throws Exception {
            mockMvc.perform(post(PAYMENTS_URL)
                            .contentType(MediaType.APPLICATION_JSON)
                            .content(validCardBody()))
                    .andExpect(status().isBadRequest())
                    .andExpect(jsonPath("$.message", containsString("Idempotency-Key")));

            verify(paymentOrchestratorService, never())
                    .createPayment(any(CreatePaymentRequest.class), anyString());
        }

        // Whitespace-only key → isBlank() true → filter rejects with 400
        @Test
        @DisplayName("Blank Idempotency-Key header → HTTP 400 Bad Request")
        void blankIdempotencyKeyHeader_returns400() throws Exception {
            mockMvc.perform(post(PAYMENTS_URL)
                            .header(IDEM_HEADER, "   ")
                            .contentType(MediaType.APPLICATION_JSON)
                            .content(validCardBody()))
                    .andExpect(status().isBadRequest());

            verify(paymentOrchestratorService, never())
                    .createPayment(any(CreatePaymentRequest.class), anyString());
        }

        // Valid header passes filter but negative amount fails @Valid → 400 with fieldErrors
        @Test
        @DisplayName("Negative amount fails Bean Validation → HTTP 400 with fieldErrors")
        void negativeAmount_returns400WithFieldErrors() throws Exception {
            // Filter passes — stubs let request reach the controller
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

        // Invalid enum value → Jackson HttpMessageNotReadableException → 400
        @Test
        @DisplayName("Invalid paymentMethod enum value → HTTP 400 Bad Request")
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

        // Missing currency field fails @Valid → 400 with currency in fieldErrors
        @Test
        @DisplayName("Null currency fails Bean Validation → HTTP 400 with currency field error")
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

        // In-flight key → filter short-circuits with 409, getCachedResponse never called
        @Test
        @DisplayName("In-flight Idempotency-Key → HTTP 409 Conflict, controller not reached")
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
            // getCachedResponse must NOT be called — in-flight check short-circuits first
            verify(idempotencyService, never()).getCachedResponse(anyString());
        }

        // Cache hit → filter returns 200 with cached body, controller bypassed
        @Test
        @DisplayName("Redis cache hit → HTTP 200 with cached body, controller bypassed")
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

        // Unknown payment ID → service throws PaymentNotFoundException → 404
        @Test
        @DisplayName("GET with unknown payment ID → HTTP 404 structured response")
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

        // Error response timestamp must be ISO-8601 string, not a numeric array
        @Test
        @DisplayName("Error response timestamp")
        void errorResponseTimestamp_isIso8601String() throws Exception {
            when(paymentOrchestratorService.getPayment(anyString()))
                    .thenThrow(new PaymentNotFoundException("any-id"));

            mockMvc.perform(get(PAYMENTS_URL + "/any-id"))
                    .andExpect(status().isNotFound())
                    .andExpect(jsonPath("$.timestamp").isString());
        }
    }
}
