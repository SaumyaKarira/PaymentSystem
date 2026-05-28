package org.example.exception;

import jakarta.servlet.http.HttpServletRequest;
import lombok.extern.slf4j.Slf4j;
import org.example.dto.ErrorResponse;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.MissingRequestHeaderException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.servlet.resource.NoResourceFoundException;

import java.time.LocalDateTime;
import java.util.List;
import java.util.stream.Collectors;

/**
 * GlobalExceptionHandler — centralized exception handling for all REST API errors.
 *
 * <h2>Design</h2>
 * <p>{@code @RestControllerAdvice} is a composed annotation that combines:
 * <ul>
 *   <li>{@code @ControllerAdvice} — applies to all controllers globally</li>
 *   <li>{@code @ResponseBody}     — all handler return values are serialized to JSON</li>
 * </ul>
 * <p>Every {@code @ExceptionHandler} method:
 * <ol>
 *   <li>Logs the exception at the appropriate severity level</li>
 *   <li>Constructs a structured {@link ErrorResponse} record</li>
 *   <li>Returns a {@code ResponseEntity} with the correct HTTP status code</li>
 * </ol>
 *
 * <h2>Error Response Format</h2>
 * <p>All errors share the same JSON structure (defined in {@link ErrorResponse}):
 * <pre>
 * {
 *   "timestamp": "2024-01-15T10:30:00",
 *   "status": 404,
 *   "error": "Not Found",
 *   "message": "Payment not found with id: abc-123",
 *   "path": "/v1/payments/abc-123",
 *   "fieldErrors": null   // only present for 400 validation errors
 * }
 * </pre>
 */
@Slf4j
@RestControllerAdvice
public class GlobalExceptionHandler {

    // ─────────────────────────────────────────────────────────────────────────
    // 400 BAD REQUEST — Client-side input errors
    // ─────────────────────────────────────────────────────────────────────────

    /**
     * Handles Bean Validation failures from {@code @Valid} on request body DTOs.
     *
     * <p>Spring MVC throws {@code MethodArgumentNotValidException} when a request body
     * fails {@code @NotNull}, {@code @NotBlank}, {@code @DecimalMin}, or other JSR-380
     * constraints.  We extract all field-level errors and include them in the response
     * so clients can fix ALL problems in one round-trip.
     *
     * @param ex      the validation exception containing field error details
     * @param request the HTTP request (used to populate the {@code path} field)
     * @return HTTP 400 with a list of field-level validation errors
     */
    @ExceptionHandler(MethodArgumentNotValidException.class)
    public ResponseEntity<ErrorResponse> handleValidationErrors(
            MethodArgumentNotValidException ex, HttpServletRequest request) {

        List<ErrorResponse.FieldError> fieldErrors = ex.getBindingResult()
                .getFieldErrors()
                .stream()
                .map(fe -> new ErrorResponse.FieldError(fe.getField(), fe.getDefaultMessage()))
                .collect(Collectors.toList());

        log.warn("Validation failed for request to [{}]: {} field error(s)",
                request.getRequestURI(), fieldErrors.size());

        ErrorResponse body = new ErrorResponse(
                LocalDateTime.now(),
                HttpStatus.BAD_REQUEST.value(),
                "Bad Request",
                "Request validation failed. See fieldErrors for details.",
                request.getRequestURI(),
                fieldErrors
        );
        return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(body);
    }

    /**
     * Handles missing required HTTP headers (e.g., {@code Idempotency-Key}).
     *
     * <p>Spring MVC throws this when a {@code @RequestHeader(required=true)} is absent.
     * The filter already catches this for the idempotency header, but this handler
     * is a safety net for any other required headers we may add in the future.
     *
     * @param ex      the exception with header name details
     * @param request the HTTP request
     * @return HTTP 400 with a clear message about the missing header
     */
    @ExceptionHandler(MissingRequestHeaderException.class)
    public ResponseEntity<ErrorResponse> handleMissingHeader(
            MissingRequestHeaderException ex, HttpServletRequest request) {

        log.warn("Missing required header [{}] for request to [{}]",
                ex.getHeaderName(), request.getRequestURI());

        ErrorResponse body = new ErrorResponse(
                LocalDateTime.now(),
                HttpStatus.BAD_REQUEST.value(),
                "Bad Request",
                "Required request header '" + ex.getHeaderName() + "' is missing.",
                request.getRequestURI(),
                null
        );
        return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(body);
    }

    /**
     * Handles malformed JSON request bodies.
     *
     * <p>Thrown when the request body cannot be deserialized to the expected DTO — e.g.,
     * unknown enum value ("paymentMethod": "WIRE"), invalid JSON syntax, or wrong data type.
     *
     * @param ex      the exception describing the parse failure
     * @param request the HTTP request
     * @return HTTP 400 with a user-friendly error message
     */
    @ExceptionHandler(HttpMessageNotReadableException.class)
    public ResponseEntity<ErrorResponse> handleMalformedBody(
            HttpMessageNotReadableException ex, HttpServletRequest request) {

        log.warn("Malformed request body for [{}]: {}", request.getRequestURI(), ex.getMessage());

        ErrorResponse body = new ErrorResponse(
                LocalDateTime.now(),
                HttpStatus.BAD_REQUEST.value(),
                "Bad Request",
                "Request body is malformed or contains invalid values. "
                        + "Ensure enum values match: paymentMethod=[CARD, UPI].",
                request.getRequestURI(),
                null
        );
        return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(body);
    }

    // ─────────────────────────────────────────────────────────────────────────
    // 404 NOT FOUND — Resource does not exist
    // ─────────────────────────────────────────────────────────────────────────

    /**
     * Handles {@link PaymentNotFoundException} — thrown by the service layer when a
     * payment with the requested UUID does not exist in MySQL.
     *
     * @param ex      the exception with the missing payment ID
     * @param request the HTTP request
     * @return HTTP 404 with a descriptive message
     */
    @ExceptionHandler(PaymentNotFoundException.class)
    public ResponseEntity<ErrorResponse> handlePaymentNotFound(
            PaymentNotFoundException ex, HttpServletRequest request) {

        log.warn("Payment not found: {} (requested by {})", ex.getMessage(), request.getRequestURI());

        ErrorResponse body = new ErrorResponse(
                LocalDateTime.now(),
                HttpStatus.NOT_FOUND.value(),
                "Not Found",
                ex.getMessage(),
                request.getRequestURI(),
                null
        );
        return ResponseEntity.status(HttpStatus.NOT_FOUND).body(body);
    }

    // ─────────────────────────────────────────────────────────────────────────
    // 409 CONFLICT — Idempotency key collision
    // ─────────────────────────────────────────────────────────────────────────

    /**
     * Handles {@link IdempotencyConflictException} — thrown when a request's
     * Idempotency-Key maps to a payment that is currently being processed
     * (IN_FLIGHT state in Redis).
     *
     * <p>The client should:
     * <ol>
     *   <li>Wait a few seconds</li>
     *   <li>Poll {@code GET /v1/payments/{id}} to check the outcome</li>
     *   <li>Only retry the POST if the poll confirms no payment exists</li>
     * </ol>
     *
     * @param ex      the conflict exception
     * @param request the HTTP request
     * @return HTTP 409 with instructions for the client
     */
    @ExceptionHandler(IdempotencyConflictException.class)
    public ResponseEntity<ErrorResponse> handleIdempotencyConflict(
            IdempotencyConflictException ex, HttpServletRequest request) {

        log.warn("Idempotency conflict for request to [{}]: {}", request.getRequestURI(), ex.getMessage());

        ErrorResponse body = new ErrorResponse(
                LocalDateTime.now(),
                HttpStatus.CONFLICT.value(),
                "Conflict",
                ex.getMessage(),
                request.getRequestURI(),
                null
        );
        return ResponseEntity.status(HttpStatus.CONFLICT).body(body);
    }

    // ─────────────────────────────────────────────────────────────────────────
    // 409 CONFLICT — Database unique constraint violations
    // ─────────────────────────────────────────────────────────────────────────

    /**
     * Handles {@code DataIntegrityViolationException} — thrown by Spring Data JPA when
     * a DB unique constraint is violated (e.g., duplicate {@code idempotency_key} insert).
     *
     * <p>This is the last line of defence against duplicate payments.  The idempotency
     * filter and Redis lock prevent the common cases; this handler covers the rare scenario
     * where two requests slip through the in-memory guards simultaneously and both reach
     * the DB INSERT.  The DB's {@code UNIQUE} index rejects the second insert, and we
     * surface it as a 409 rather than a 500.
     *
     * @param ex      the data integrity exception
     * @param request the HTTP request
     * @return HTTP 409 with a duplicate payment message
     */
    @ExceptionHandler(DataIntegrityViolationException.class)
    public ResponseEntity<ErrorResponse> handleDataIntegrityViolation(
            DataIntegrityViolationException ex, HttpServletRequest request) {

        log.warn("Data integrity violation at [{}]: {}", request.getRequestURI(), ex.getMessage());

        ErrorResponse body = new ErrorResponse(
                LocalDateTime.now(),
                HttpStatus.CONFLICT.value(),
                "Conflict",
                "A payment with the provided idempotency key already exists.",
                request.getRequestURI(),
                null
        );
        return ResponseEntity.status(HttpStatus.CONFLICT).body(body);
    }

    // ─────────────────────────────────────────────────────────────────────────
    // 500 INTERNAL SERVER ERROR — Unexpected / unhandled errors
    // ─────────────────────────────────────────────────────────────────────────

    /**
     * Handles {@code NoResourceFoundException} — thrown by Spring MVC when a request
     * targets a static resource path that does not exist (e.g., {@code /favicon.ico}
     * automatically requested by browsers).
     *
     * <p>This is intentionally logged at DEBUG level and suppressed from ERROR logs
     * because it is a benign browser-initiated request, not an application error.
     *
     * @param ex      the exception
     * @param request the HTTP request
     * @return HTTP 404 with no body
     */
    @ExceptionHandler(NoResourceFoundException.class)
    public ResponseEntity<ErrorResponse> handleNoResourceFound(
            NoResourceFoundException ex, HttpServletRequest request) {

        log.debug("Static resource not found [{}]: {}", request.getRequestURI(), ex.getMessage());
        ErrorResponse body = new ErrorResponse(
                LocalDateTime.now(),
                HttpStatus.NOT_FOUND.value(),
                "Not Found",
                ex.getMessage(),
                request.getRequestURI(),
                null
        );
        return ResponseEntity.status(HttpStatus.NOT_FOUND).body(body);
    }

    /**
     * Catch-all handler for any exception not explicitly handled above.
     *
     * <p>This ensures the API never exposes raw Java stack traces in the response body.
     * The full stack trace is logged at ERROR level for investigation, but the response
     * body only contains a generic message — preventing information leakage.
     *
     * <p>Examples of exceptions caught here:
     * <ul>
     *   <li>Redis connectivity failures (LettuceConnectionException)</li>
     *   <li>Kafka send failures (KafkaProducerException)</li>
     *   <li>Unexpected NullPointerException in the service layer</li>
     * </ul>
     *
     * @param ex      the unexpected exception
     * @param request the HTTP request
     * @return HTTP 500 with a generic error message (no stack trace in body)
     */
    @ExceptionHandler(Exception.class)
    public ResponseEntity<ErrorResponse> handleGenericException(
            Exception ex, HttpServletRequest request) {

        // Log the full stack trace at ERROR level for investigation
        log.error("Unhandled exception for request to [{}]: {}",
                request.getRequestURI(), ex.getMessage(), ex);

        ErrorResponse body = new ErrorResponse(
                LocalDateTime.now(),
                HttpStatus.INTERNAL_SERVER_ERROR.value(),
                "Internal Server Error",
                "An unexpected error occurred. Please try again later or contact support.",
                request.getRequestURI(),
                null
        );
        return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(body);
    }
}

