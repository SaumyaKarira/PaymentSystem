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

// Servlet filter that enforces idempotency for POST /v1/payments.
// Flow: check header → check IN_FLIGHT → check cached response → proceed
@Slf4j
@Component
public class IdempotencyFilter extends OncePerRequestFilter {

    // HTTP header name expected from clients
    public static final String IDEMPOTENCY_KEY_HEADER = "Idempotency-Key";

    // Only apply this filter to POST requests on the payments endpoint
    private static final String PAYMENTS_PATH_PREFIX = "/v1/payments";

    private final IdempotencyService idempotencyService;

    // Same ObjectMapper used by IdempotencyService to ensure consistent Redis read/write format
    private final ObjectMapper objectMapper;

    public IdempotencyFilter(
            IdempotencyService idempotencyService,
            @Qualifier("redisObjectMapper") ObjectMapper objectMapper) {
        this.idempotencyService = idempotencyService;
        this.objectMapper = objectMapper;
    }

    @Override
    protected void doFilterInternal(
            HttpServletRequest request,
            HttpServletResponse response,
            FilterChain filterChain
    ) throws ServletException, IOException {

        // Only intercept POST /v1/payments — all other requests pass through
        String method = request.getMethod();
        String path = request.getRequestURI();

        if (!"POST".equalsIgnoreCase(method) || !path.startsWith(PAYMENTS_PATH_PREFIX)) {
            filterChain.doFilter(request, response);
            return;
        }

        // Reject requests missing the Idempotency-Key header → 400
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

        // If another request is actively processing this key → 409
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

        // Return cached completed response if present — no new payment created
        Optional<PaymentResponse> cachedResponse = idempotencyService.getCachedResponse(idempotencyKey);
        if (cachedResponse.isPresent()) {
            log.info("Idempotency cache hit for key [{}] — returning cached response", idempotencyKey);
            writeCachedResponse(response, cachedResponse.get());
            return;
        }

        // No lock, no cache — genuine new request; service will acquire the lock before processing
        filterChain.doFilter(request, response);
    }

    // Writes a JSON error response and short-circuits the filter chain
    private void writeErrorResponse(HttpServletResponse response, int statusCode, String message)
            throws IOException {
        response.setStatus(statusCode);
        response.setContentType(MediaType.APPLICATION_JSON_VALUE);
        response.setCharacterEncoding("UTF-8");
        String body = String.format(
                "{\"status\":%d,\"error\":\"%s\",\"message\":\"%s\"}",
                statusCode,
                statusCode == 400 ? "Bad Request" : "Conflict",
                message.replace("\"", "\\\"")
        );
        response.getWriter().write(body);
    }

    // Writes a cached PaymentResponse as HTTP 200
    private void writeCachedResponse(HttpServletResponse response, PaymentResponse paymentResponse)
            throws IOException {
        response.setStatus(HttpStatus.OK.value());
        response.setContentType(MediaType.APPLICATION_JSON_VALUE);
        response.setCharacterEncoding("UTF-8");
        objectMapper.writeValue(response.getWriter(), paymentResponse);
    }
}
