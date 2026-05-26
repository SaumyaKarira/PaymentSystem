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

/**
 * IdempotencyService — manages idempotency key lifecycle in local Redis.
 *
 * <h2>Storage Strategy</h2>
 * <p>All values in Redis are stored as plain UTF-8 JSON strings:
 * <ul>
 *   <li>While processing:  key → {@code "IN_FLIGHT"}</li>
 *   <li>After completion:  key → {@code "{\"id\":\"...\",\"status\":\"SUCCESS\",...}"}</li>
 * </ul>
 *
 * <p>Using plain strings avoids all Jackson polymorphic type-embedding issues.
 * {@code PaymentResponse} is a Java {@code record} (implicitly {@code final}), which
 * was excluded from type metadata when using {@code NON_FINAL} typing — causing the
 * {@code SerializationException: missing type id property '@class'} on deserialization.
 * Storing as a raw JSON string and deserializing with an explicit target type completely
 * sidesteps this problem.
 */
@Slf4j
@Service
public class IdempotencyService {

    private static final String KEY_PREFIX      = "idempotency:";
    private static final String IN_FLIGHT_VALUE = "IN_FLIGHT";

    @Value("${payment.idempotency.ttl-seconds:86400}")
    private long ttlSeconds;

    /**
     * RedisTemplate typed to {@code <String, String>} — both keys and values are
     * plain strings. Injected from {@code RedisConfig.redisTemplate()}.
     */
    private final RedisTemplate<String, String> redisTemplate;

    /**
     * Shared ObjectMapper for serializing {@code PaymentResponse} to JSON string
     * before storing in Redis, and deserializing back on read.
     * Uses the {@code redisObjectMapper} bean from {@code RedisConfig} which has
     * {@code JavaTimeModule} configured so dates are ISO-8601 strings, not arrays.
     */
    private final ObjectMapper objectMapper;

    public IdempotencyService(
            RedisTemplate<String, String> redisTemplate,
            @Qualifier("redisObjectMapper") ObjectMapper objectMapper) {
        this.redisTemplate = redisTemplate;
        this.objectMapper = objectMapper;
    }

    /**
     * Attempts to acquire an in-flight lock for the given idempotency key.
     *
     * <p>Uses Redis {@code SET key value NX PX ttl} (atomic — only sets if key absent).
     * Only one concurrent request can win this lock for a given key. All others get false.
     *
     * @param idempotencyKey the raw idempotency key from the request header
     * @return {@code true} if the lock was acquired; {@code false} if already held
     */
    public boolean acquireInFlightLock(String idempotencyKey) {
        String redisKey = KEY_PREFIX + idempotencyKey;
        Boolean acquired = redisTemplate.opsForValue()
                .setIfAbsent(redisKey, IN_FLIGHT_VALUE, Duration.ofSeconds(ttlSeconds));
        boolean result = Boolean.TRUE.equals(acquired);
        log.debug("Idempotency lock for key [{}]: {}", idempotencyKey, result ? "ACQUIRED" : "FAILED");
        return result;
    }

    /**
     * Returns {@code true} if the key exists in Redis with value {@code "IN_FLIGHT"}.
     * Used by the filter to short-circuit concurrent duplicate requests with 409.
     *
     * @param idempotencyKey the raw idempotency key
     * @return true if currently in-flight
     */
    public boolean isInFlight(String idempotencyKey) {
        String value = redisTemplate.opsForValue().get(KEY_PREFIX + idempotencyKey);
        return IN_FLIGHT_VALUE.equals(value);
    }

    /**
     * Serializes the completed {@link PaymentResponse} to a JSON string and stores it
     * in Redis, overwriting the {@code IN_FLIGHT} sentinel.
     *
     * <p>After this call, any duplicate request arriving at the filter will find this
     * JSON string, deserialize it back to {@code PaymentResponse}, and return it to
     * the client — with zero DB, provider, or Kafka involvement.
     *
     * @param idempotencyKey  the raw idempotency key
     * @param paymentResponse the completed payment response to cache
     */
    public void storeCompletedResponse(String idempotencyKey, PaymentResponse paymentResponse) {
        try {
            // Serialize PaymentResponse → plain JSON string.
            // Example: {"id":"abc","status":"SUCCESS","amount":150.00,...}
            String json = objectMapper.writeValueAsString(paymentResponse);
            redisTemplate.opsForValue().set(KEY_PREFIX + idempotencyKey, json, ttlSeconds, TimeUnit.SECONDS);
            log.debug("Stored completed response in Redis for key [{}]", idempotencyKey);
        } catch (JsonProcessingException e) {
            // This should never happen for a well-formed PaymentResponse record.
            // Log and continue — worst case the next duplicate request re-processes normally.
            log.error("Failed to serialize PaymentResponse to JSON for key [{}]: {}",
                    idempotencyKey, e.getMessage());
        }
    }

    /**
     * Reads the cached JSON string from Redis and deserializes it back to
     * {@link PaymentResponse}. Returns empty if the key is absent or still IN_FLIGHT.
     *
     * @param idempotencyKey the raw idempotency key
     * @return Optional containing the cached response, or empty
     */
    public Optional<PaymentResponse> getCachedResponse(String idempotencyKey) {
        String value = redisTemplate.opsForValue().get(KEY_PREFIX + idempotencyKey);

        // Key absent or still IN_FLIGHT — not a completed response
        if (value == null || IN_FLIGHT_VALUE.equals(value)) {
            return Optional.empty();
        }

        try {
            // Deserialize the JSON string back to PaymentResponse using the same ObjectMapper
            // that was used to serialize it — guaranteed format consistency.
            PaymentResponse response = objectMapper.readValue(value, PaymentResponse.class);
            log.debug("Cache hit for idempotency key [{}]", idempotencyKey);
            return Optional.of(response);
        } catch (JsonProcessingException e) {
            // Corrupted or unexpected value in Redis — treat as cache miss and reprocess.
            log.error("Failed to deserialize cached PaymentResponse for key [{}]: {}. "
                    + "Treating as cache miss.", idempotencyKey, e.getMessage());
            return Optional.empty();
        }
    }

    /**
     * Deletes the idempotency key from Redis entirely.
     * Called when an unexpected error occurs during payment creation so the client
     * can safely retry after the error is resolved (key won't stay as IN_FLIGHT).
     *
     * @param idempotencyKey the raw idempotency key to remove
     */
    public void releaseKey(String idempotencyKey) {
        redisTemplate.delete(KEY_PREFIX + idempotencyKey);
        log.debug("Released idempotency key [{}] from Redis", idempotencyKey);
    }

    /**
     * Returns {@code true} if any entry (IN_FLIGHT or completed) exists for the key.
     *
     * @param idempotencyKey the raw idempotency key
     * @return true if the key exists in Redis
     */
    public boolean exists(String idempotencyKey) {
        return Boolean.TRUE.equals(redisTemplate.hasKey(KEY_PREFIX + idempotencyKey));
    }
}
