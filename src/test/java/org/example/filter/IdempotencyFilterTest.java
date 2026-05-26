package org.example.filter;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.example.dto.PaymentResponse;
import org.example.entity.PaymentMethod;
import org.example.entity.PaymentStatus;
import org.example.service.IdempotencyService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.context.annotation.Import;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.Optional;

import org.example.dto.CreatePaymentRequest;

import static org.assertj.core.api.Assertions.assertThat;
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
 * IdempotencyFilterTest — Web layer slice tests for the IdempotencyFilter.
 *
 * <h2>Why @WebMvcTest(PaymentController.class)?</h2>
 * <p>{@code @WebMvcTest} starts only the Spring MVC slice: DispatcherServlet, filters,
 * controllers, exception handlers, and argument resolvers. It does NOT start:
 * <ul>
 *   <li>The full ApplicationContext (no JPA, no Redis beans, no Kafka)</li>
 *   <li>The real IdempotencyService (which we replace with a @MockBean)</li>
 *   <li>The real PaymentOrchestratorService (also @MockBean)</li>
 * </ul>
 * Specifying {@code PaymentController.class} limits the slice to only that controller.
 *
 * <h2>Filter Registration in @WebMvcTest</h2>
 * <p>{@code @WebMvcTest} auto-detects {@code @Component} beans in the MVC filter chain.
 * Since {@code IdempotencyFilter} extends {@code OncePerRequestFilter} and is annotated
 * with {@code @Component}, it is automatically wired into the MockMvc request pipeline.
 * We also {@code @Import} it explicitly to be safe.
 *
 * <h2>What these tests validate</h2>
 * <ul>
 *   <li>Missing/blank Idempotency-Key header → HTTP 400 from the filter</li>
 *   <li>In-flight idempotency key → HTTP 409 from the filter</li>
 *   <li>Cache hit in Redis → HTTP 200 returned from filter, controller NEVER reached</li>
 *   <li>New valid request passes through the filter to the controller</li>
 *   <li>GET requests bypass the filter entirely (idempotency is POST-only)</li>
 * </ul>
 */
@WebMvcTest(controllers = org.example.controller.PaymentController.class)
@Import({IdempotencyFilter.class, org.example.exception.GlobalExceptionHandler.class})
@DisplayName("IdempotencyFilter — Web Slice Tests")
class IdempotencyFilterTest {

    /**
     * MockMvc is Spring's in-process HTTP client. It dispatches through the full
     * DispatcherServlet pipeline (including filters) without binding to a real port.
     * This is order-of-magnitude faster than @SpringBootTest with a real server.
     */
    @Autowired
    private MockMvc mockMvc;

    /**
     * @MockBean replaces the real IdempotencyService bean in the Spring test context.
     * Any call to it returns Mockito's default stubs (false for boolean, empty Optional)
     * unless we configure it explicitly with when().thenReturn().
     */
    @MockBean
    private IdempotencyService idempotencyService;

    /**
     * @MockBean for the orchestrator service — we don't want actual DB/Kafka calls
     * in web slice tests. This mock can be configured per-test to return expected responses.
     */
    @MockBean
    private org.example.service.PaymentOrchestratorService paymentOrchestratorService;

    private ObjectMapper objectMapper;

    // Shared test constants
    private static final String PAYMENTS_URL    = "/v1/payments";
    private static final String IDEMPOTENCY_HDR = "Idempotency-Key";
    private static final String PAYMENT_ID      = "550e8400-e29b-41d4-a716-446655440001";
    private static final String IDEMPOTENCY_KEY = "unique-key-for-test-001";

    @BeforeEach
    void setUp() {
        objectMapper = new ObjectMapper().registerModule(new JavaTimeModule());
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Helper: valid create payment request JSON body
    // ─────────────────────────────────────────────────────────────────────────

    private String validCardRequestBody() {
        return """
                {
                  "amount": 150.00,
                  "currency": "USD",
                  "paymentMethod": "CARD"
                }
                """;
    }

    private String validUpiRequestBody() {
        return """
                {
                  "amount": 500.00,
                  "currency": "INR",
                  "paymentMethod": "UPI"
                }
                """;
    }

    /**
     * Builds a pre-canned successful PaymentResponse to return from MockBean stubs.
     */
    private PaymentResponse buildCachedSuccessResponse() {
        return new PaymentResponse(
                PAYMENT_ID,
                IDEMPOTENCY_KEY,
                new BigDecimal("150.00"),
                "USD",
                PaymentMethod.CARD,
                PaymentStatus.SUCCESS,
                "PROVIDER_A",
                "PROVA-CACHEDREF12345",
                0,
                LocalDateTime.now().minusMinutes(2),
                LocalDateTime.now().minusMinutes(2)
        );
    }

    // =========================================================================
    // CATEGORY A — Filter Pass-Through (Good Requests)
    // =========================================================================

    @Nested
    @DisplayName("Category A — Filter pass-through for valid requests")
    class FilterPassThroughTests {

        /**
         * Validates that a valid POST request with a unique Idempotency-Key passes through
         * the filter and reaches the controller (and ultimately the orchestrator).
         *
         * <p>The filter must:
         * <ul>
         *   <li>NOT find the key in-flight (isInFlight → false)</li>
         *   <li>NOT find a cached response (getCachedResponse → empty)</li>
         *   <li>NOT short-circuit — let the request reach the controller</li>
         * </ul>
         * The controller delegates to the orchestrator mock, which we configure to
         * return a SUCCESS response. The test asserts HTTP 201.
         */
        @Test
        @DisplayName("Valid POST with new Idempotency-Key passes filter to controller — HTTP 201")
        void validPostWithNewKeyPassesThroughFilter() throws Exception {
            // ARRANGE
            // Filter checks: key not in-flight, no cached response
            when(idempotencyService.isInFlight(IDEMPOTENCY_KEY)).thenReturn(false);
            when(idempotencyService.getCachedResponse(IDEMPOTENCY_KEY)).thenReturn(Optional.empty());

            // Controller mock returns SUCCESS response
            PaymentResponse orchestratorResponse = buildCachedSuccessResponse();
            when(paymentOrchestratorService.createPayment(any(CreatePaymentRequest.class), anyString()))
                    .thenReturn(orchestratorResponse);

            // ACT & ASSERT
            mockMvc.perform(post(PAYMENTS_URL)
                            .header(IDEMPOTENCY_HDR, IDEMPOTENCY_KEY)
                            .contentType(MediaType.APPLICATION_JSON)
                            .content(validCardRequestBody()))
                    .andExpect(status().isCreated())
                    .andExpect(jsonPath("$.status").value("SUCCESS"))
                    .andExpect(jsonPath("$.id").value(PAYMENT_ID))
                    .andExpect(jsonPath("$.paymentMethod").value("CARD"));

            // Verify the filter called both Redis checks (the filter has no early exit
            // until it has checked both conditions)
            verify(idempotencyService).isInFlight(IDEMPOTENCY_KEY);
            verify(idempotencyService).getCachedResponse(IDEMPOTENCY_KEY);
        }

        /**
         * Validates that GET requests bypass the idempotency filter entirely.
         * GET requests are read-only — idempotency enforcement is unnecessary.
         */
        @Test
        @DisplayName("GET requests bypass the IdempotencyFilter completely")
        void getRequestsBypassesIdempotencyFilter() throws Exception {
            // ARRANGE — configure the orchestrator mock for GET
            PaymentResponse response = buildCachedSuccessResponse();
            when(paymentOrchestratorService.getPayment(PAYMENT_ID)).thenReturn(response);

            // ACT — GET does not include Idempotency-Key header
            mockMvc.perform(get(PAYMENTS_URL + "/" + PAYMENT_ID)
                            .contentType(MediaType.APPLICATION_JSON))
                    .andExpect(status().isOk())
                    .andExpect(jsonPath("$.id").value(PAYMENT_ID));

            // ASSERT — idempotency service must NEVER be called for GET requests
            verify(idempotencyService, never()).isInFlight(anyString());
            verify(idempotencyService, never()).getCachedResponse(anyString());
        }
    }

    // =========================================================================
    // CATEGORY B — Filter Short-Circuits (Bad/Duplicate Requests)
    // =========================================================================

    @Nested
    @DisplayName("Category B — Filter rejection scenarios")
    class FilterRejectionTests {

        /**
         * TEST CASE 7 — Variant A: Missing Idempotency-Key header → HTTP 400.
         *
         * <p>The filter is the FIRST line of defence for missing headers. It checks before
         * the DispatcherServlet even resolves the handler method, so the controller's
         * {@code @RequestHeader} validation is never reached.
         */
        @Test
        @DisplayName("TC-07A: POST without Idempotency-Key header returns HTTP 400")
        void tc07a_missingIdempotencyKeyHeaderReturns400() throws Exception {
            mockMvc.perform(post(PAYMENTS_URL)
                            .contentType(MediaType.APPLICATION_JSON)
                            .content(validCardRequestBody()))
                    // Expecting 400 — the filter rejects immediately
                    .andExpect(status().isBadRequest())
                    .andExpect(jsonPath("$.message")
                            .value(org.hamcrest.Matchers.containsString("Idempotency-Key")));

            // The orchestrator must NEVER be reached when the header is missing
            verify(paymentOrchestratorService, never()).createPayment(any(CreatePaymentRequest.class), anyString());
        }

        /**
         * TEST CASE 7 — Variant B: Blank Idempotency-Key header → HTTP 400.
         *
         * <p>An empty or whitespace-only header value is treated the same as a missing header.
         */
        @Test
        @DisplayName("TC-07B: POST with blank Idempotency-Key header returns HTTP 400")
        void tc07b_blankIdempotencyKeyHeaderReturns400() throws Exception {
            mockMvc.perform(post(PAYMENTS_URL)
                            .header(IDEMPOTENCY_HDR, "   ")  // whitespace-only value
                            .contentType(MediaType.APPLICATION_JSON)
                            .content(validCardRequestBody()))
                    .andExpect(status().isBadRequest());

            verify(paymentOrchestratorService, never()).createPayment(any(CreatePaymentRequest.class), anyString());
        }

        /**
         * TEST CASE 7 — Variant C: Invalid request body (negative amount) → HTTP 400.
         *
         * <p>Bean Validation rejects {@code amount = -50.00} because {@code @DecimalMin("0.01")}
         * requires positive values. This validation fires in the controller layer AFTER the
         * filter passes — confirming the two-layered validation approach.
         */
        @Test
        @DisplayName("TC-07C: POST with negative amount fails Bean Validation — HTTP 400")
        void tc07c_negativeAmountFailsBeanValidation() throws Exception {
            String invalidBody = """
                    {
                      "amount": -50.00,
                      "currency": "USD",
                      "paymentMethod": "CARD"
                    }
                    """;

            mockMvc.perform(post(PAYMENTS_URL)
                            .header(IDEMPOTENCY_HDR, "valid-key-for-bad-body")
                            .contentType(MediaType.APPLICATION_JSON)
                            .content(invalidBody))
                    // Filter passes (valid header), but validation fails at controller level
                    .andExpect(status().isBadRequest())
                    .andExpect(jsonPath("$.fieldErrors").isArray())
                    .andExpect(jsonPath("$.fieldErrors[0].field").value("amount"));

            verify(paymentOrchestratorService, never()).createPayment(any(CreatePaymentRequest.class), anyString());
        }

        /**
         * TEST CASE 7 — Variant D: Invalid paymentMethod enum value → HTTP 400.
         *
         * <p>Jackson's JsonDeserializer cannot map "WIRE" to the {@code PaymentMethod} enum.
         * Spring MVC throws {@code HttpMessageNotReadableException}, handled by
         * {@code GlobalExceptionHandler.handleMalformedBody()} → 400.
         */
        @Test
        @DisplayName("TC-07D: POST with invalid paymentMethod enum returns HTTP 400")
        void tc07d_invalidPaymentMethodEnumReturns400() throws Exception {
            String invalidBody = """
                    {
                      "amount": 100.00,
                      "currency": "USD",
                      "paymentMethod": "WIRE"
                    }
                    """;

            mockMvc.perform(post(PAYMENTS_URL)
                            .header(IDEMPOTENCY_HDR, "valid-key-enum-test")
                            .contentType(MediaType.APPLICATION_JSON)
                            .content(invalidBody))
                    .andExpect(status().isBadRequest())
                    .andExpect(jsonPath("$.error").value("Bad Request"))
                    .andExpect(jsonPath("$.message")
                            .value(org.hamcrest.Matchers.containsString("CARD")));

            verify(paymentOrchestratorService, never()).createPayment(any(CreatePaymentRequest.class), anyString());
        }

        /**
         * TEST CASE 7 — Variant E: Missing required field (currency null) → HTTP 400.
         */
        @Test
        @DisplayName("TC-07E: POST with null currency fails Bean Validation — HTTP 400 with fieldErrors")
        void tc07e_nullCurrencyFailsValidationWithFieldErrors() throws Exception {
            String invalidBody = """
                    {
                      "amount": 100.00,
                      "paymentMethod": "CARD"
                    }
                    """;

            mockMvc.perform(post(PAYMENTS_URL)
                            .header(IDEMPOTENCY_HDR, "valid-key-no-currency")
                            .contentType(MediaType.APPLICATION_JSON)
                            .content(invalidBody))
                    .andExpect(status().isBadRequest())
                    .andExpect(jsonPath("$.fieldErrors").isArray())
                    // At least one field error about 'currency'
                    .andExpect(jsonPath(
                            "$.fieldErrors[?(@.field == 'currency')]").exists());

            verify(paymentOrchestratorService, never()).createPayment(any(CreatePaymentRequest.class), anyString());
        }

        /**
         * TEST CASE 6: In-flight idempotency collision → HTTP 409 Conflict.
         *
         * <p>The filter detects the key is IN_FLIGHT in Redis and short-circuits with 409.
         * The controller and orchestrator are NEVER reached. This prevents duplicate payments
         * from concurrent requests.
         */
        @Test
        @DisplayName("TC-06: In-flight Idempotency-Key returns HTTP 409 Conflict from filter")
        void tc06_inFlightKeyReturns409ConflictFromFilter() throws Exception {
            // ARRANGE — Redis says this key is currently being processed
            when(idempotencyService.isInFlight(IDEMPOTENCY_KEY)).thenReturn(true);

            // ACT & ASSERT
            mockMvc.perform(post(PAYMENTS_URL)
                            .header(IDEMPOTENCY_HDR, IDEMPOTENCY_KEY)
                            .contentType(MediaType.APPLICATION_JSON)
                            .content(validCardRequestBody()))
                    .andExpect(status().isConflict())
                    .andExpect(jsonPath("$.message")
                            .value(org.hamcrest.Matchers.containsString(IDEMPOTENCY_KEY)));

            // Controller MUST NOT be reached — filter short-circuited the chain
            verify(paymentOrchestratorService, never()).createPayment(any(CreatePaymentRequest.class), anyString());

            // getCachedResponse should NEVER be called when isInFlight is true
            // (the filter returns 409 immediately after detecting IN_FLIGHT)
            verify(idempotencyService, never()).getCachedResponse(anyString());
        }

        /**
         * TEST CASE 4: Idempotency cache hit — completed response returned from filter.
         *
         * <p>The filter finds a completed {@code PaymentResponse} in Redis for the given key.
         * It serializes it directly to the HTTP response body (HTTP 200) and returns without
         * invoking the filter chain. The orphan controller is NEVER reached.
         *
         * <p><strong>Why HTTP 200 and not 201?</strong> The original successful request
         * returned 201. The filter returns 200 on replay to distinguish "new creation"
         * from "idempotent replay of existing". Both are correct responses; the status code
         * difference signals idempotent replays.
         */
        @Test
        @DisplayName("TC-04: Cache hit in Redis returns HTTP 200 with cached response; controller bypassed")
        void tc04_cacheHitInRedisReturnsHttp200WithCachedResponseBypassingController() throws Exception {
            // ARRANGE
            // Key is NOT in-flight (it's completed), so isInFlight returns false
            when(idempotencyService.isInFlight(IDEMPOTENCY_KEY)).thenReturn(false);

            // getCachedResponse returns the previously cached PaymentResponse
            PaymentResponse cachedResponse = buildCachedSuccessResponse();
            when(idempotencyService.getCachedResponse(IDEMPOTENCY_KEY))
                    .thenReturn(Optional.of(cachedResponse));

            // ACT — same request body, same idempotency key as the original request
            MvcResult mvcResult = mockMvc.perform(post(PAYMENTS_URL)
                            .header(IDEMPOTENCY_HDR, IDEMPOTENCY_KEY)
                            .contentType(MediaType.APPLICATION_JSON)
                            .content(validCardRequestBody()))
                    // Filter returns 200 for idempotent replay
                    .andExpect(status().isOk())
                    .andReturn();

            // Parse the response body to validate it matches the cached object
            String responseBody = mvcResult.getResponse().getContentAsString();
            PaymentResponse returnedResponse = objectMapper.readValue(responseBody, PaymentResponse.class);

            assertThat(returnedResponse.id())
                    .as("Replayed response must carry the original payment ID")
                    .isEqualTo(PAYMENT_ID);
            assertThat(returnedResponse.status())
                    .as("Replayed response must carry the original SUCCESS status")
                    .isEqualTo(PaymentStatus.SUCCESS);
            assertThat(returnedResponse.providerReferenceId())
                    .as("Replayed response must carry the original provider reference")
                    .isEqualTo("PROVA-CACHEDREF12345");

            // Controller + orchestrator must NEVER be called — pure filter response
            verify(paymentOrchestratorService, never()).createPayment(any(CreatePaymentRequest.class), anyString());
        }
    }

    // =========================================================================
    // CATEGORY B — Error Handling in GlobalExceptionHandler
    // =========================================================================

    @Nested
    @DisplayName("Category B — GlobalExceptionHandler mapping")
    class GlobalExceptionHandlerTests {

        /**
         * TEST CASE 8: GET with non-existent payment ID → HTTP 404.
         *
         * <p>The orchestrator mock throws {@code PaymentNotFoundException}, which the
         * {@code GlobalExceptionHandler.handlePaymentNotFound()} maps to 404 with a
         * structured ErrorResponse body.
         */
        @Test
        @DisplayName("TC-08: GET with unknown payment ID uses GlobalExceptionHandler → HTTP 404")
        void tc08_getUnknownPaymentIdReturns404ViaGlobalHandler() throws Exception {
            // ARRANGE
            String unknownId = "00000000-ffff-0000-ffff-000000000000";
            when(paymentOrchestratorService.getPayment(unknownId))
                    .thenThrow(new org.example.exception.PaymentNotFoundException(unknownId));

            // ACT & ASSERT
            mockMvc.perform(get(PAYMENTS_URL + "/" + unknownId))
                    .andExpect(status().isNotFound())
                    .andExpect(jsonPath("$.status").value(404))
                    .andExpect(jsonPath("$.error").value("Not Found"))
                    .andExpect(jsonPath("$.message")
                            .value(org.hamcrest.Matchers.containsString(unknownId)))
                    .andExpect(jsonPath("$.path")
                            .value(PAYMENTS_URL + "/" + unknownId));
        }

        /**
         * Validates that the GlobalExceptionHandler formats the timestamp field
         * as an ISO-8601 date-time string, not a raw array.
         */
        @Test
        @DisplayName("Error response timestamp is ISO-8601 formatted string")
        void errorResponseTimestampIsIso8601() throws Exception {
            when(paymentOrchestratorService.getPayment(anyString()))
                    .thenThrow(new org.example.exception.PaymentNotFoundException("any-id"));

            mockMvc.perform(get(PAYMENTS_URL + "/any-id"))
                    .andExpect(status().isNotFound())
                    // timestamp should be a string, not an array [2024,1,15,10,30,0,0]
                    .andExpect(jsonPath("$.timestamp").isString());
        }
    }

    // ─── Hamcrest import helper (used in jsonPath matchers above) ────────────
    // Note: Hamcrest is a transitive dependency of spring-boot-starter-test.
    // The static import org.hamcrest.Matchers.containsString is used inline above.
}





