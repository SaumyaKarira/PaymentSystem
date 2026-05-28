package org.example.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.example.dto.PaymentResponse;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

// Manages idempotency key lifecycle in Redis.
// While processing: key → "IN_FLIGHT"
// After completion: key → serialized PaymentResponse JSON string
@Slf4j
@Service
public class IdempotencyService {

    private static final String KEY_PREFIX      = "idempotency:";
    private static final String IN_FLIGHT_VALUE = "IN_FLIGHT";

    @Value("${payment.idempotency.ttl-seconds:86400}")
    private long ttlSeconds;

    // RedisTemplate with String keys and String values
    private final RedisTemplate<String, String> redisTemplate;

    // ObjectMapper shared with the filter for consistent JSON serialization of PaymentResponse
    private final ObjectMapper objectMapper;

    public IdempotencyService(
            RedisTemplate<String, String> redisTemplate,
            @Qualifier("redisObjectMapper") ObjectMapper objectMapper) {
        this.redisTemplate = redisTemplate;
        this.objectMapper = objectMapper;
    }

    // Acquires an in-flight lock via Redis SET NX. Returns true if the lock was acquired.
    public boolean acquireInFlightLock(String idempotencyKey) {
        String redisKey = KEY_PREFIX + idempotencyKey;
        Boolean acquired = redisTemplate.opsForValue()
                .setIfAbsent(redisKey, IN_FLIGHT_VALUE, Duration.ofSeconds(ttlSeconds));
        boolean result = Boolean.TRUE.equals(acquired);
        log.debug("Idempotency lock for key [{}]: {}", idempotencyKey, result ? "ACQUIRED" : "FAILED");
        return result;
    }

    // Returns true if the key exists in Redis with value "IN_FLIGHT".
    public boolean isInFlight(String idempotencyKey) {
        String value = redisTemplate.opsForValue().get(KEY_PREFIX + idempotencyKey);
        return IN_FLIGHT_VALUE.equals(value);
    }

    // Serializes the completed PaymentResponse to JSON and stores it in Redis, replacing IN_FLIGHT.
    public void storeCompletedResponse(String idempotencyKey, PaymentResponse paymentResponse) {
        try {
            String json = objectMapper.writeValueAsString(paymentResponse);
            redisTemplate.opsForValue().set(KEY_PREFIX + idempotencyKey, json, ttlSeconds, TimeUnit.SECONDS);
            log.debug("Stored completed response in Redis for key [{}]", idempotencyKey);
        } catch (JsonProcessingException e) {
            // Shouldn't happen for a well-formed PaymentResponse — log and continue.
            log.error("Failed to serialize PaymentResponse to JSON for key [{}]: {}",
                    idempotencyKey, e.getMessage());
        }
    }

    // Reads cached JSON from Redis and deserializes it. Returns empty if absent or still IN_FLIGHT.
    public Optional<PaymentResponse> getCachedResponse(String idempotencyKey) {
        String value = redisTemplate.opsForValue().get(KEY_PREFIX + idempotencyKey);

        if (value == null || IN_FLIGHT_VALUE.equals(value)) {
            return Optional.empty();
        }

        try {
            PaymentResponse response = objectMapper.readValue(value, PaymentResponse.class);
            log.debug("Cache hit for idempotency key [{}]", idempotencyKey);
            return Optional.of(response);
        } catch (JsonProcessingException e) {
            // Corrupted value in Redis — treat as cache miss.
            log.error("Failed to deserialize cached PaymentResponse for key [{}]: {}. Treating as cache miss.",
                    idempotencyKey, e.getMessage());
            return Optional.empty();
        }
    }

    // Deletes the idempotency key from Redis so the client can safely retry.
    public void releaseKey(String idempotencyKey) {
        redisTemplate.delete(KEY_PREFIX + idempotencyKey);
        log.debug("Released idempotency key [{}] from Redis", idempotencyKey);
    }

    // Returns true if any entry (IN_FLIGHT or completed) exists for the key.
    public boolean exists(String idempotencyKey) {
        return Boolean.TRUE.equals(redisTemplate.hasKey(KEY_PREFIX + idempotencyKey));
    }
}
