package org.example.dto;

import org.example.entity.PaymentMethod;

import java.math.BigDecimal;

// Kafka message payload published to payment-main-topic when a synchronous provider call fails.
// Lightweight event with only the fields needed for retry processing.
// version is intentionally excluded — consumer always re-fetches from DB to get the latest version.
public record PaymentEvent(
        String paymentId,
        BigDecimal amount,
        String currency,
        PaymentMethod paymentMethod,
        int retryCount
) {}
