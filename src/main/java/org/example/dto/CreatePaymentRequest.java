package org.example.dto;

import jakarta.validation.constraints.DecimalMin;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Size;
import org.example.entity.PaymentMethod;

import java.math.BigDecimal;

/**
 * CreatePaymentRequest — incoming DTO for the {@code POST /v1/payments} endpoint.
 *
 * <p>Bean Validation annotations enforce contract correctness before the request
 * reaches the service layer. Violations are caught by {@code GlobalExceptionHandler}
 * and returned as structured 400 Bad Request responses.
 *
 * <p>Uses a Java record (Java 16+) for conciseness — records are immutable by design,
 * which is exactly what we want for an inbound request DTO.
 *
 * @param amount        monetary value; must be greater than zero
 * @param currency      ISO 4217 currency code (2–10 characters)
 * @param paymentMethod the payment instrument (CARD or UPI)
 */
public record CreatePaymentRequest(

        @NotNull(message = "amount must not be null")
        @DecimalMin(value = "0.01", message = "amount must be greater than 0.00")
        BigDecimal amount,

        @NotBlank(message = "currency must not be blank")
        @Size(min = 2, max = 10, message = "currency must be between 2 and 10 characters")
        String currency,

        @NotNull(message = "paymentMethod must not be null")
        PaymentMethod paymentMethod

) {}

