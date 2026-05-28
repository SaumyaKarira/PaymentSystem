package org.example.dto;

import com.fasterxml.jackson.annotation.JsonInclude;
import org.example.entity.Payment;
import org.example.entity.PaymentMethod;
import org.example.entity.PaymentStatus;

import java.math.BigDecimal;
import java.time.LocalDateTime;

// Outbound DTO returned by both POST and GET payment endpoints.
// @JsonInclude(NON_NULL) suppresses null fields (e.g., providerReferenceId before a provider succeeds).
// from() maps a Payment entity to this DTO, keeping entity-to-DTO logic in one place.
@JsonInclude(JsonInclude.Include.NON_NULL)
public record PaymentResponse(
        String id,
        String idempotencyKey,
        BigDecimal amount,
        String currency,
        PaymentMethod paymentMethod,
        PaymentStatus status,
        String providerId,
        String providerReferenceId,
        Integer retryCount,
        LocalDateTime createdAt,
        LocalDateTime updatedAt
) {

    // Maps a Payment entity to a PaymentResponse DTO
    public static PaymentResponse from(Payment payment) {
        return new PaymentResponse(
                payment.getId(),
                payment.getIdempotencyKey(),
                payment.getAmount(),
                payment.getCurrency(),
                payment.getPaymentMethod(),
                payment.getStatus(),
                payment.getProviderId(),
                payment.getProviderReferenceId(),
                payment.getRetryCount(),
                payment.getCreatedAt(),
                payment.getUpdatedAt()
        );
    }
}
