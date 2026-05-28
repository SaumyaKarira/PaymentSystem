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

/**
 * IdempotencyFilterTest — standalone MockMvc tests for {@link IdempotencyFilter}.
 *
 * <h2>Why standaloneSetup instead of @WebMvcTest</h2>
 * <p>{@code @WebMvcTest} loads a partial Spring context. When {@code IdempotencyFilter}
 * (which requires a {@code redisObjectMapper} bean) is added via {@code @Import}, its
 * constructor injection may fail silently during context initialization, causing the
 * {@code DispatcherServlet} to not register controller routes — every request then falls
 * through to Spring's static resource handler and returns {@code NoResourceFoundException}.
 *
 * <p>{@code MockMvcBuilders.standaloneSetup(controller)} avoids this by building MockMvc
 * directly from the controller instance — no Spring context, no auto-configuration, no
 * bean wiring surprises. The filter is constructed manually with its mocked dependencies
 * and added via {@code .addFilters(filter)}, giving us full control over the filter chain.
 *
 * <h2>Test Infrastructure</h2>
 * <ul>
 *   <li>{@link MockitoExtension} — provides {@code @Mock} field injection without Spring</li>
 *   <li>{@link MockMvcBuilders#standaloneSetup} — registers the controller and filter chain</li>
 *   <li>{@link GlobalExceptionHandler} — added via {@code .setControllerAdvice()} so all
 *       exception-to-HTTP mappings work exactly as in production</li>
 *   <li>A shared {@link ObjectMapper} with {@code JavaTimeModule} is passed to both the
 *       filter and the HTTP message converter, guaranteeing consistent JSON serialization</li>
 * </ul>
 */
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
        // ── ObjectMapper: shared between filter and HTTP message converter ────
        // JavaTimeModule ensures LocalDateTime is serialized as an ISO-8601 string
        // ("2026-05-28T10:30:00") rather than a numeric array ([2026,5,28,...]).
        // Both the filter's writeCachedResponse() and the controller's JSON responses
        // must use the SAME mapper configuration for consistent serialization.
        objectMapper = new ObjectMapper()
                .registerModule(new JavaTimeModule())
                .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);

        // ── IdempotencyFilter: manually constructed with mocked dependencies ──
        // This bypasses Spring's bean wiring entirely. The filter gets real logic
        // (it's the real IdempotencyFilter class) but its collaborators are Mockito mocks.
        IdempotencyFilter filter = new IdempotencyFilter(idempotencyService, objectMapper);

        // ── PaymentController: real instance with a mocked service ────────────
        PaymentController controller = new PaymentController(paymentOrchestratorService);

        // ── MockMvc: standalone setup wiring controller + filter + advice ─────
        // standaloneSetup() registers the controller's @RequestMapping routes with the
        // DispatcherServlet WITHOUT needing a Spring ApplicationContext. Routes are
        // discovered via reflection from the controller instance — 100% reliable.
        //
        // .addFilters(filter)          → inserts IdempotencyFilter into the filter chain
        //                                before the DispatcherServlet, exactly as in prod
        // .setControllerAdvice(...)    → registers GlobalExceptionHandler so @ExceptionHandler
        //                                methods produce proper JSON error responses
        // .setMessageConverters(...)   → installs the configured ObjectMapper so LocalDateTime
        //                                fields serialize as ISO-8601 strings in error responses
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

        /**
         * TC-01: A valid POST with a fresh Idempotency-Key (not in-flight, not cached)
         * must pass all filter checks, reach the controller, and return HTTP 201.
         *
         * <p>Flow: filter checks isInFlight → false → getCachedResponse → empty
         * → filterChain.doFilter() → controller.createPayment() → 201
         */
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

        /**
         * TC-02: GET requests must bypass IdempotencyFilter — the filter's scope check
         * ({@code !"POST".equalsIgnoreCase(method)}) calls filterChain.doFilter() immediately.
         * No idempotency checks are performed.
         */
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

        /**
         * TC-03A: POST without an Idempotency-Key header — filter writes 400 directly
         * to the response and returns. The controller is never reached.
         */
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

        /**
         * TC-03B: Whitespace-only Idempotency-Key — {@code isBlank()} returns true,
         * filter rejects with 400 before reaching the controller.
         */
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

        /**
         * TC-03C: Header is valid so the filter passes the request through, but a
         * negative amount fails {@code @Valid} Bean Validation in the controller.
         * {@code GlobalExceptionHandler} returns 400 with a {@code fieldErrors} array.
         */
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

        /**
         * TC-03D: Invalid enum value — Jackson throws {@code HttpMessageNotReadableException},
         * mapped to 400 by {@code GlobalExceptionHandler.handleMalformedBody()}.
         */
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

        /**
         * TC-03E: Missing {@code currency} field fails Bean Validation.
         * The {@code fieldErrors} array must contain an entry for the {@code currency} field.
         */
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

        /**
         * TC-05: In-flight Idempotency-Key → HTTP 409 Conflict.
         *
         * <p>When {@code isInFlight(key)} returns {@code true}, the filter writes a 409
         * directly and returns. Both the controller and {@code getCachedResponse()} are
         * never invoked.
         */
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

        /**
         * TC-04: Redis cache hit → HTTP 200 OK with cached body, controller bypassed.
         *
         * <p>When {@code getCachedResponse(key)} returns a non-empty Optional, the filter
         * serializes the {@link PaymentResponse} to the HTTP response (status 200) and
         * returns without calling {@code filterChain.doFilter()}.
         */
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

        /**
         * TC-08: GET with an unknown payment ID — service throws
         * {@link PaymentNotFoundException}, mapped to HTTP 404 with a structured JSON body.
         */
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

        /**
         * TC-09: Error response {@code timestamp} must be an ISO-8601 string — not a
         * numeric array — validating that {@code JavaTimeModule} is configured correctly
         * on the shared {@code ObjectMapper} used by the message converter.
         */
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
