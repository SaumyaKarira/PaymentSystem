package org.example.dto;

import com.fasterxml.jackson.annotation.JsonInclude;

import java.time.LocalDateTime;
import java.util.List;

// Standardized error payload returned by GlobalExceptionHandler for all API errors.
// fieldErrors is populated only for validation failures (400); null for all other error types.
@JsonInclude(JsonInclude.Include.NON_NULL)
public record ErrorResponse(
        LocalDateTime timestamp,
        int status,
        String error,
        String message,
        String path,
        List<FieldError> fieldErrors
) {

    // Single field-level validation error (field name + constraint violation message)
    public record FieldError(String field, String message) {}
}
