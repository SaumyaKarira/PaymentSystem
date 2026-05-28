package org.example.dto;

import jakarta.validation.constraints.DecimalMin;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Size;
import org.example.entity.PaymentMethod;

import java.math.BigDecimal;

// Incoming DTO for POST /v1/payments.
// Bean Validation annotations enforce contract correctness; violations return 400 via GlobalExceptionHandler.
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
