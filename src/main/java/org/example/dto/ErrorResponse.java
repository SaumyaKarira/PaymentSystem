package org.example.dto;

import com.fasterxml.jackson.annotation.JsonInclude;

import java.time.LocalDateTime;
import java.util.List;

/**
 * ErrorResponse — a standardized error payload returned by {@code GlobalExceptionHandler}.
 *
 * <p>All error responses from this API share the same JSON structure, making it easier
 * for clients to parse errors programmatically regardless of which exception was thrown.
 *
 * <p>The {@code fieldErrors} list is populated only for validation failures (HTTP 400),
 * and suppressed via {@code @JsonInclude(NON_NULL)} for all other error types.
 *
 * @param timestamp   the exact moment the error occurred (UTC)
 * @param status      HTTP status code (e.g., 400, 404, 409, 500)
 * @param error       short human-readable error category (e.g., "Not Found")
 * @param message     detailed description of the error
 * @param path        the request URI that triggered the error
 * @param fieldErrors list of per-field validation errors; null for non-validation errors
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public record ErrorResponse(
        LocalDateTime timestamp,
        int status,
        String error,
        String message,
        String path,
        List<FieldError> fieldErrors
) {

    /**
     * FieldError — a single field-level validation error.
     *
     * @param field   the name of the request field that failed validation
     * @param message the constraint violation message
     */
    public record FieldError(String field, String message) {}
}

