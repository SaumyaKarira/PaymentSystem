package org.example.filter;

import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import lombok.extern.slf4j.Slf4j;
import org.example.dto.PaymentResponse;
import org.example.service.IdempotencyService;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Component;
import org.springframework.web.filter.OncePerRequestFilter;

import java.io.IOException;
import java.util.Optional;

/**
 * IdempotencyFilter — a Servlet filter that enforces idempotency for POST requests
 * to the payments API.
 *
 * <h2>Filter Placement</h2>
 * <p>This filter runs BEFORE the request reaches the DispatcherServlet and any
 * {@code @RestController}.  It is registered automatically by Spring Boot because
 * it is annotated with {@code @Component} and extends {@code OncePerRequestFilter}.
 *
 * <p>{@code OncePerRequestFilter} guarantees the filter executes exactly once per
 * request, even in cases of request-forwarding within the servlet container.
 *
 * <h2>Idempotency Check Flow</h2>
 * <pre>
 *  Incoming POST /v1/payments
 *       │
 *       ▼
 *  Is Idempotency-Key header present?
 *       │ NO  → reject with 400 Bad Request
 *       │ YES
 *       ▼
 *  Is the key currently IN_FLIGHT in Redis?
 *       │ YES → reject with 409 Conflict
 *       │ NO
 *       ▼
 *  Is there a cached completed response?
 *       │ YES → return cached 200 response (no DB hit, no side effect)
 *       │ NO
 *       ▼
 *  Proceed to controller → service layer will acquire the lock before processing
 * </pre>
 *
 * <h2>Why a Filter and not a HandlerInterceptor?</h2>
 * <p>A Servlet Filter runs earlier in the request pipeline — before Spring MVC mapping.
 * This means idempotency checks happen before any argument resolution or databinding,
 * which is the correct behavior: we want to short-circuit at the edge, not after
 * deserializing the full request body.
 */
@Slf4j
@Component
public class IdempotencyFilter extends OncePerRequestFilter {

    /** HTTP header name expected from clients */
    public static final String IDEMPOTENCY_KEY_HEADER = "Idempotency-Key";

    /** Only apply this filter to POST requests on the payments endpoint */
    private static final String PAYMENTS_PATH_PREFIX = "/v1/payments";

    private final IdempotencyService idempotencyService;

    /**
     * Injected from RedisConfig — the same ObjectMapper used by IdempotencyService
     * to serialize PaymentResponse. Using the SAME bean instance guarantees that the
     * format written to Redis (by the service) matches the format read back here (by
     * the filter) — dates as ISO-8601 strings, no @class type metadata.
     */
    private final ObjectMapper objectMapper;

    public IdempotencyFilter(
            IdempotencyService idempotencyService,
            @Qualifier("redisObjectMapper") ObjectMapper objectMapper) {
        this.idempotencyService = idempotencyService;
        this.objectMapper = objectMapper;
    }

    /**
     * Core filter logic.
     *
     * <p>The filter only intercepts {@code POST} requests to {@code /v1/payments}.
     * All other requests (GET, OPTIONS, etc.) pass through immediately without any
     * idempotency check — idempotency is only meaningful for mutating operations.
     *
     * @param request     the incoming HTTP request
     * @param response    the outgoing HTTP response (may be short-circuited here)
     * @param filterChain the remaining filter chain to invoke if we don't short-circuit
     */
    @Override
    protected void doFilterInternal(
            HttpServletRequest request,
            HttpServletResponse response,
            FilterChain filterChain
    ) throws ServletException, IOException {

        // ── SCOPE CHECK: Only intercept POST /v1/payments ─────────────────────
        // Non-POST requests and non-payment paths bypass idempotency logic entirely.
        String method = request.getMethod();
        String path = request.getRequestURI();

        if (!"POST".equalsIgnoreCase(method) || !path.startsWith(PAYMENTS_PATH_PREFIX)) {
            filterChain.doFilter(request, response);
            return;
        }

        // ── HEADER VALIDATION ─────────────────────────────────────────────────
        // Reject requests that do not supply the Idempotency-Key header.
        // This is a client contract violation → 400 Bad Request.
        String idempotencyKey = request.getHeader(IDEMPOTENCY_KEY_HEADER);
        if (idempotencyKey == null || idempotencyKey.isBlank()) {
            log.warn("POST {} rejected: missing or blank Idempotency-Key header", path);
            writeErrorResponse(
                    response,
                    HttpStatus.BAD_REQUEST.value(),
                    "Missing required header: " + IDEMPOTENCY_KEY_HEADER
            );
            return;
        }

        log.debug("Idempotency check for key [{}] on {} {}", idempotencyKey, method, path);

        // ── IN-FLIGHT CHECK ───────────────────────────────────────────────────
        // If the Redis key exists and is set to "IN_FLIGHT", another request is actively
        // processing this idempotency key right now.
        // Return 409 Conflict so the client knows to wait and poll for the outcome.
        if (idempotencyService.isInFlight(idempotencyKey)) {
            log.warn("Idempotency key [{}] is currently IN_FLIGHT — returning 409 Conflict", idempotencyKey);
            writeErrorResponse(
                    response,
                    HttpStatus.CONFLICT.value(),
                    "A payment with idempotency key '" + idempotencyKey + "' is currently being processed. "
                            + "Poll GET /v1/payments/{id} for the latest status."
            );
            return;
        }

        // ── CACHE HIT CHECK ───────────────────────────────────────────────────
        // If a completed PaymentResponse is cached in Redis for this key, return it
        // immediately.  This is the "safe replay" path: the client gets the same
        // response as the original successful request — NO new payment is created.
        Optional<PaymentResponse> cachedResponse = idempotencyService.getCachedResponse(idempotencyKey);
        if (cachedResponse.isPresent()) {
            log.info("Idempotency cache hit for key [{}] — returning cached response", idempotencyKey);
            writeCachedResponse(response, cachedResponse.get());
            return;
        }

        // ── PROCEED ───────────────────────────────────────────────────────────
        // No in-flight lock and no cached response → this is a genuine new request.
        // The service layer (PaymentOrchestratorService) will acquire the Redis lock
        // atomically before processing.
        filterChain.doFilter(request, response);
    }

    /**
     * Writes a plain-text error message as an HTTP response and short-circuits the chain.
     *
     * @param response   the HttpServletResponse to write to
     * @param statusCode the HTTP status code (e.g., 400, 409)
     * @param message    the error message body
     */
    private void writeErrorResponse(HttpServletResponse response, int statusCode, String message)
            throws IOException {
        response.setStatus(statusCode);
        response.setContentType(MediaType.APPLICATION_JSON_VALUE);
        response.setCharacterEncoding("UTF-8");
        // Write a simple JSON error payload compatible with the ErrorResponse record structure
        String body = String.format(
                "{\"status\":%d,\"error\":\"%s\",\"message\":\"%s\"}",
                statusCode,
                statusCode == 400 ? "Bad Request" : "Conflict",
                message.replace("\"", "\\\"")
        );
        response.getWriter().write(body);
    }

    /**
     * Writes a cached {@link PaymentResponse} as the HTTP response body.
     * The status is HTTP 200 OK, mirroring what the original request received.
     *
     * @param response       the HttpServletResponse to write to
     * @param paymentResponse the cached response to serialize
     */
    private void writeCachedResponse(HttpServletResponse response, PaymentResponse paymentResponse)
            throws IOException {
        response.setStatus(HttpStatus.OK.value());
        response.setContentType(MediaType.APPLICATION_JSON_VALUE);
        response.setCharacterEncoding("UTF-8");
        objectMapper.writeValue(response.getWriter(), paymentResponse);
    }
}

