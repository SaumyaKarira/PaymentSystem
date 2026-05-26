package org.example.dto;

import com.fasterxml.jackson.annotation.JsonInclude;
import org.example.entity.Payment;
import org.example.entity.PaymentMethod;
import org.example.entity.PaymentStatus;

import java.math.BigDecimal;
import java.time.LocalDateTime;

/**
 * PaymentResponse — outbound DTO returned by both the POST and GET endpoints.
 *
 * <p>{@code @JsonInclude(NON_NULL)} suppresses null fields from the JSON response body,
 * keeping the response clean when optional fields (e.g., {@code providerReferenceId})
 * have not yet been populated.
 *
 * <p>This is a Java record — immutable, compact, and avoids Lombok on the DTO layer.
 * The static factory method {@link #from(Payment)} encapsulates entity-to-DTO mapping,
 * keeping the service layer free of Jackson or DTO concerns.
 *
 * @param id                    payment UUID
 * @param idempotencyKey        the key used to deduplicate this request
 * @param amount                monetary amount
 * @param currency              ISO 4217 currency code
 * @param paymentMethod         CARD or UPI
 * @param status                current lifecycle status
 * @param providerId            which provider connector handled this payment
 * @param providerReferenceId   provider's own transaction reference ID
 * @param retryCount            number of retry attempts made via Kafka
 * @param createdAt             record creation timestamp
 * @param updatedAt             last update timestamp
 */
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

    /**
     * Maps a {@link Payment} JPA entity to a {@link PaymentResponse} DTO.
     *
     * <p>Keeping the mapping logic here (on the DTO) follows the principle of
     * co-locating the DTO's construction logic with the DTO itself, rather than
     * scattering it across service classes.
     *
     * @param payment the entity to convert
     * @return a fully populated response DTO
     */
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

