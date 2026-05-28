package org.example.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.example.dto.PaymentResponse;
import org.example.entity.PaymentMethod;
import org.example.entity.PaymentStatus;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.ValueOperations;
import org.springframework.test.util.ReflectionTestUtils;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * IdempotencyServiceTest — unit tests for {@link IdempotencyService}.
 *
 * <p>All Redis interactions are mocked using Mockito, so no real Redis connection
 * is required. The ObjectMapper is a real instance configured identically to how
 * {@code RedisConfig.redisObjectMapper()} creates it in production.
 */
@ExtendWith(MockitoExtension.class)
@DisplayName("IdempotencyService — Unit Tests")
class IdempotencyServiceTest {

    @Mock
    private RedisTemplate<String, String> redisTemplate;

    @Mock
    private ValueOperations<String, String> valueOps;

    private IdempotencyService idempotencyService;

    private static final String IDEM_KEY   = "test-key-abc";
    private static final String REDIS_KEY  = "idempotency:test-key-abc";
    private static final long   TTL        = 60L;

    private ObjectMapper objectMapper;

    @BeforeEach
    void setUp() {
        objectMapper = new ObjectMapper()
                .registerModule(new JavaTimeModule())
                .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);

        idempotencyService = new IdempotencyService(redisTemplate, objectMapper);
        ReflectionTestUtils.setField(idempotencyService, "ttlSeconds", TTL);
        // NOTE: opsForValue() stub is NOT set here globally because releaseKey() and
        // exists() never call opsForValue(), which would cause UnnecessaryStubbingException
        // in Mockito strict mode. Each nested class that needs it sets up the stub locally.
    }

    // ── Helper ────────────────────────────────────────────────────────────────

    private PaymentResponse sampleResponse() {
        return new PaymentResponse(
                "pay-001", IDEM_KEY, new BigDecimal("150.00"), "USD",
                PaymentMethod.CARD, PaymentStatus.SUCCESS,
                "PROVIDER_A", "PROVA-REF123", 0,
                LocalDateTime.of(2026, 5, 28, 10, 0),
                LocalDateTime.of(2026, 5, 28, 10, 1));
    }

    // =========================================================================
    // acquireInFlightLock
    // =========================================================================

    @Nested
    @DisplayName("acquireInFlightLock")
    class AcquireInFlightLockTests {

        @BeforeEach
        void wireValueOps() {
            when(redisTemplate.opsForValue()).thenReturn(valueOps);
        }
        @Test
        @DisplayName("Returns true when Redis SET NX succeeds (key was absent)")
        void returnsTrue_whenSetNxSucceeds() {
            when(valueOps.setIfAbsent(eq(REDIS_KEY), eq("IN_FLIGHT"), any(Duration.class)))
                    .thenReturn(Boolean.TRUE);

            boolean result = idempotencyService.acquireInFlightLock(IDEM_KEY);

            assertThat(result).isTrue();
            verify(valueOps).setIfAbsent(eq(REDIS_KEY), eq("IN_FLIGHT"), any(Duration.class));
        }

        @Test
        @DisplayName("Returns false when Redis SET NX fails (key already exists)")
        void returnsFalse_whenSetNxFails() {
            when(valueOps.setIfAbsent(eq(REDIS_KEY), eq("IN_FLIGHT"), any(Duration.class)))
                    .thenReturn(Boolean.FALSE);

            boolean result = idempotencyService.acquireInFlightLock(IDEM_KEY);

            assertThat(result).isFalse();
        }

        @Test
        @DisplayName("Returns false when Redis returns null (network issue treated as failure)")
        void returnsFalse_whenRedisReturnsNull() {
            when(valueOps.setIfAbsent(eq(REDIS_KEY), eq("IN_FLIGHT"), any(Duration.class)))
                    .thenReturn(null);

            boolean result = idempotencyService.acquireInFlightLock(IDEM_KEY);

            assertThat(result).isFalse();
        }
    }

    // =========================================================================
    // isInFlight
    // =========================================================================

    @Nested
    @DisplayName("isInFlight")
    class IsInFlightTests {

        @BeforeEach
        void wireValueOps() {
            when(redisTemplate.opsForValue()).thenReturn(valueOps);
        }
        @Test
        @DisplayName("Returns true when Redis value is IN_FLIGHT")
        void returnsTrue_whenValueIsInFlight() {
            when(valueOps.get(REDIS_KEY)).thenReturn("IN_FLIGHT");

            assertThat(idempotencyService.isInFlight(IDEM_KEY)).isTrue();
        }

        @Test
        @DisplayName("Returns false when Redis value is a JSON response (already completed)")
        void returnsFalse_whenValueIsJsonResponse() {
            when(valueOps.get(REDIS_KEY)).thenReturn("{\"id\":\"pay-001\",\"status\":\"SUCCESS\"}");

            assertThat(idempotencyService.isInFlight(IDEM_KEY)).isFalse();
        }

        @Test
        @DisplayName("Returns false when key does not exist in Redis")
        void returnsFalse_whenKeyAbsent() {
            when(valueOps.get(REDIS_KEY)).thenReturn(null);

            assertThat(idempotencyService.isInFlight(IDEM_KEY)).isFalse();
        }
    }

    // =========================================================================
    // storeCompletedResponse
    // =========================================================================

    @Nested
    @DisplayName("storeCompletedResponse")
    class StoreCompletedResponseTests {

        @BeforeEach
        void wireValueOps() {
            when(redisTemplate.opsForValue()).thenReturn(valueOps);
        }
        @Test
        @DisplayName("Serializes PaymentResponse to JSON and stores it in Redis with TTL")
        void storesJsonStringWithCorrectTtl() {
            idempotencyService.storeCompletedResponse(IDEM_KEY, sampleResponse());

            verify(valueOps).set(
                    eq(REDIS_KEY),
                    anyString(),    // the JSON string — content verified in getCachedResponse tests
                    eq(TTL),
                    eq(TimeUnit.SECONDS)
            );
        }

        @Test
        @DisplayName("Stored JSON contains expected payment fields")
        void storedJson_containsPaymentFields() {
            // Capture the actual JSON string that gets stored
            var jsonCaptor = org.mockito.ArgumentCaptor.forClass(String.class);

            idempotencyService.storeCompletedResponse(IDEM_KEY, sampleResponse());

            verify(valueOps).set(eq(REDIS_KEY), jsonCaptor.capture(), eq(TTL), eq(TimeUnit.SECONDS));

            String json = jsonCaptor.getValue();
            assertThat(json).contains("pay-001");
            assertThat(json).contains("SUCCESS");
            assertThat(json).contains("150");
            assertThat(json).contains("PROVIDER_A");
            // Date must be an ISO-8601 string, not a numeric array
            assertThat(json).contains("2026-05-28");
        }
    }

    // =========================================================================
    // getCachedResponse
    // =========================================================================

    @Nested
    @DisplayName("getCachedResponse")
    class GetCachedResponseTests {

        @BeforeEach
        void wireValueOps() {
            when(redisTemplate.opsForValue()).thenReturn(valueOps);
        }
        @Test
        @DisplayName("Returns empty Optional when key is absent in Redis")
        void returnsEmpty_whenKeyAbsent() {
            when(valueOps.get(REDIS_KEY)).thenReturn(null);

            Optional<PaymentResponse> result = idempotencyService.getCachedResponse(IDEM_KEY);

            assertThat(result).isEmpty();
        }

        @Test
        @DisplayName("Returns empty Optional when value is IN_FLIGHT")
        void returnsEmpty_whenValueIsInFlight() {
            when(valueOps.get(REDIS_KEY)).thenReturn("IN_FLIGHT");

            Optional<PaymentResponse> result = idempotencyService.getCachedResponse(IDEM_KEY);

            assertThat(result).isEmpty();
        }

        @Test
        @DisplayName("Deserializes stored JSON back to PaymentResponse correctly")
        void deserializesJson_toPaymentResponse() throws Exception {
            // Pre-serialize a response to simulate what storeCompletedResponse would put in Redis
            String json = objectMapper.writeValueAsString(sampleResponse());
            when(valueOps.get(REDIS_KEY)).thenReturn(json);

            Optional<PaymentResponse> result = idempotencyService.getCachedResponse(IDEM_KEY);

            assertThat(result).isPresent();
            PaymentResponse response = result.get();
            assertThat(response.id()).isEqualTo("pay-001");
            assertThat(response.status()).isEqualTo(PaymentStatus.SUCCESS);
            assertThat(response.paymentMethod()).isEqualTo(PaymentMethod.CARD);
            assertThat(response.amount()).isEqualByComparingTo(new BigDecimal("150.00"));
            assertThat(response.providerId()).isEqualTo("PROVIDER_A");
        }

        @Test
        @DisplayName("Returns empty Optional when Redis contains malformed JSON")
        void returnsEmpty_whenJsonMalformed() {
            when(valueOps.get(REDIS_KEY)).thenReturn("not-valid-json{{{");

            Optional<PaymentResponse> result = idempotencyService.getCachedResponse(IDEM_KEY);

            // Should not throw — graceful degradation
            assertThat(result).isEmpty();
        }
    }

    // =========================================================================
    // releaseKey
    // =========================================================================

    @Nested
    @DisplayName("releaseKey")
    class ReleaseKeyTests {

        @Test
        @DisplayName("Calls redisTemplate.delete with the namespaced key")
        void deletesNamespacedKey() {
            idempotencyService.releaseKey(IDEM_KEY);

            verify(redisTemplate).delete(REDIS_KEY);
        }

        @Test
        @DisplayName("Does not interact with valueOps when releasing the key")
        void doesNotUseValueOps_onRelease() {
            idempotencyService.releaseKey(IDEM_KEY);

            verify(redisTemplate, never()).opsForValue();
        }
    }

    // =========================================================================
    // exists
    // =========================================================================

    @Nested
    @DisplayName("exists")
    class ExistsTests {

        @Test
        @DisplayName("Returns true when key is present in Redis")
        void returnsTrue_whenKeyPresent() {
            when(redisTemplate.hasKey(REDIS_KEY)).thenReturn(Boolean.TRUE);

            assertThat(idempotencyService.exists(IDEM_KEY)).isTrue();
        }

        @Test
        @DisplayName("Returns false when key is absent in Redis")
        void returnsFalse_whenKeyAbsent() {
            when(redisTemplate.hasKey(REDIS_KEY)).thenReturn(Boolean.FALSE);

            assertThat(idempotencyService.exists(IDEM_KEY)).isFalse();
        }

        @Test
        @DisplayName("Returns false when Redis returns null for hasKey")
        void returnsFalse_whenHasKeyReturnsNull() {
            when(redisTemplate.hasKey(REDIS_KEY)).thenReturn(null);

            assertThat(idempotencyService.exists(IDEM_KEY)).isFalse();
        }
    }
}






