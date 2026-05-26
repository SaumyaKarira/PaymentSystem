package org.example.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.example.dto.PaymentResponse;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

/**
 * IdempotencyService — manages idempotency key lifecycle in local Redis.
 *
 * <h2>Purpose</h2>
 * <p>Idempotency prevents duplicate side effects when a client retransmits a request
 * (e.g., due to a network timeout).  This service provides a Redis-backed store for:
 * <ol>
 *   <li><strong>In-flight lock</strong>: A sentinel value marking that a request with
 *       a given key is currently being processed.  Prevents concurrent duplicate requests.</li>
 *   <li><strong>Completed response cache</strong>: The full {@code PaymentResponse} stored
 *       after a payment is finalized (SUCCESS or immediate state).  Returned directly to
 *       clients on duplicate requests.</li>
 * </ol>
 *
 * <h2>Key Naming Convention</h2>
 * <p>All keys are stored under the prefix {@code idempotency:} to namespace them in Redis:
 * <pre>
 *   idempotency:{client-supplied-key}  →  "IN_FLIGHT"  (during processing)
 *   idempotency:{client-supplied-key}  →  {PaymentResponse JSON}  (after completion)
 * </pre>
 *
 * <h2>TTL Strategy</h2>
 * <p>All keys are set with a 24-hour TTL (configurable via {@code payment.idempotency.ttl-seconds}).
 * After 24 hours, a duplicate request is treated as a new payment.  This matches common
 * industry practice (Stripe, Adyen use 24-hour idempotency windows).
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class IdempotencyService {

    /**
     * The Redis key prefix used to namespace all idempotency keys.
     * Prevents collision with other Redis key spaces in the same database.
     */
    private static final String KEY_PREFIX = "idempotency:";

    /**
     * Sentinel value stored during in-flight processing.
     * A string is intentionally used (not a complex object) so that the check
     * {@code "IN_FLIGHT".equals(value)} is a simple equality test.
     */
    private static final String IN_FLIGHT_VALUE = "IN_FLIGHT";

    /**
     * Injected from {@code application.yml: payment.idempotency.ttl-seconds}.
     * Default is 86400 (24 hours).
     */
    @Value("${payment.idempotency.ttl-seconds:86400}")
    private long ttlSeconds;

    /**
     * The custom RedisTemplate configured in {@code RedisConfig}.
     * Uses String keys and JSON-serialized values.
     */
    private final RedisTemplate<String, Object> redisTemplate;

    /**
     * Attempts to acquire an in-flight lock for the given idempotency key.
     *
     * <h3>Lock Acquisition Logic (Thread-Safety Explanation)</h3>
     * <p>We use Redis's {@code SET key value NX PX ttl} command (exposed as
     * {@code setIfAbsent} in Spring Data Redis).  This is an <strong>atomic</strong>
     * Redis operation:
     * <ul>
     *   <li>{@code NX} — only set if the key does NOT already exist</li>
     *   <li>{@code PX} — set the expiry in milliseconds atomically with the set</li>
     * </ul>
     * <p>Atomicity is crucial: if we used a separate GET + SET, two concurrent requests
     * could both see the key absent and both proceed (classic TOCTOU race condition).
     * With {@code SET NX PX}, only ONE request will get a {@code true} return value —
     * that request "wins" the lock and proceeds to create the payment.
     * All other concurrent requests for the same key will receive {@code false}.
     *
     * @param idempotencyKey the client-supplied idempotency key (raw, without prefix)
     * @return {@code true} if the lock was acquired (this request should proceed),
     *         {@code false} if another request is already in-flight for this key
     */
    public boolean acquireInFlightLock(String idempotencyKey) {
        String redisKey = KEY_PREFIX + idempotencyKey;

        // setIfAbsent = Redis SET key value NX (set only if Not eXists).
        // We also attach the TTL atomically to prevent orphaned locks if the JVM crashes
        // before the lock is explicitly released or replaced with the final response.
        Boolean acquired = redisTemplate.opsForValue()
                .setIfAbsent(redisKey, IN_FLIGHT_VALUE, Duration.ofSeconds(ttlSeconds));

        // Redis returns null in certain edge cases (e.g., connection issue, wrong config).
        // Treat null as "failed to acquire" to be safe and avoid proceeding without a lock.
        boolean result = Boolean.TRUE.equals(acquired);
        log.debug("Idempotency lock acquisition for key [{}]: {}", idempotencyKey, result ? "ACQUIRED" : "FAILED");
        return result;
    }

    /**
     * Checks whether a request for the given idempotency key is currently in-flight.
     *
     * <p>Returns {@code true} only if the key exists AND its value is the
     * {@link #IN_FLIGHT_VALUE} sentinel.  This distinguishes between:
     * <ul>
     *   <li>Key absent → no previous request (or TTL expired)</li>
     *   <li>Key = IN_FLIGHT → request in progress right now</li>
     *   <li>Key = PaymentResponse → request already completed</li>
     * </ul>
     *
     * @param idempotencyKey the raw idempotency key
     * @return {@code true} if the key is in IN_FLIGHT state
     */
    public boolean isInFlight(String idempotencyKey) {
        Object value = redisTemplate.opsForValue().get(KEY_PREFIX + idempotencyKey);
        return IN_FLIGHT_VALUE.equals(value);
    }

    /**
     * Stores the completed {@link PaymentResponse} in Redis, replacing the IN_FLIGHT sentinel.
     *
     * <p>After this call, any subsequent duplicate request will find the stored response
     * and return it directly without creating a new payment.
     * The TTL is refreshed to 24 hours from the current moment.
     *
     * @param idempotencyKey  the raw idempotency key
     * @param paymentResponse the completed payment response to cache
     */
    public void storeCompletedResponse(String idempotencyKey, PaymentResponse paymentResponse) {
        String redisKey = KEY_PREFIX + idempotencyKey;
        // Overwrite the IN_FLIGHT sentinel with the actual response object.
        // SET with expiry: the value will automatically expire after ttlSeconds.
        redisTemplate.opsForValue().set(redisKey, paymentResponse, ttlSeconds, TimeUnit.SECONDS);
        log.debug("Stored completed response in Redis for idempotency key [{}]", idempotencyKey);
    }

    /**
     * Retrieves the cached {@link PaymentResponse} for a completed payment, if available.
     *
     * <p>Returns an empty Optional if:
     * <ul>
     *   <li>The key does not exist (TTL expired or never set)</li>
     *   <li>The key is in IN_FLIGHT state (not yet completed)</li>
     *   <li>The stored value is not a {@code PaymentResponse} (unexpected type)</li>
     * </ul>
     *
     * @param idempotencyKey the raw idempotency key
     * @return an Optional containing the cached PaymentResponse, or empty
     */
    public Optional<PaymentResponse> getCachedResponse(String idempotencyKey) {
        Object value = redisTemplate.opsForValue().get(KEY_PREFIX + idempotencyKey);
        if (value instanceof PaymentResponse response) {
            log.debug("Cache hit for idempotency key [{}]", idempotencyKey);
            return Optional.of(response);
        }
        return Optional.empty();
    }

    /**
     * Removes the idempotency key from Redis entirely.
     *
     * <p>This is used as a cleanup mechanism if the payment creation fails with an
     * unrecoverable error (e.g., DB constraint violation that is NOT a duplicate key).
     * Without cleanup, the key would remain as IN_FLIGHT for 24 hours, blocking retries.
     *
     * @param idempotencyKey the raw idempotency key to remove
     */
    public void releaseKey(String idempotencyKey) {
        redisTemplate.delete(KEY_PREFIX + idempotencyKey);
        log.debug("Released idempotency key [{}] from Redis", idempotencyKey);
    }

    /**
     * Checks whether any entry (IN_FLIGHT or completed) exists for the given key.
     *
     * @param idempotencyKey the raw idempotency key
     * @return {@code true} if the key exists in Redis
     */
    public boolean exists(String idempotencyKey) {
        return Boolean.TRUE.equals(redisTemplate.hasKey(KEY_PREFIX + idempotencyKey));
    }
}

