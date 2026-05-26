package org.example.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.connection.RedisStandaloneConfiguration;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.serializer.StringRedisSerializer;

/**
 * RedisConfig — configures the Spring Data Redis infrastructure for local development.
 *
 * <h2>Serialization Strategy — Why Plain String Values</h2>
 * <p>The previous implementation used {@code GenericJackson2JsonRedisSerializer} with
 * {@code ObjectMapper.DefaultTyping.NON_FINAL}. This caused a deserialization failure
 * because {@code PaymentResponse} is a Java {@code record}, which is implicitly
 * {@code final}. The {@code NON_FINAL} typing policy explicitly EXCLUDES final classes
 * from having the {@code @class} type metadata embedded in the JSON. So the stored
 * JSON had no top-level {@code @class} field, but the deserializer expected one — crash.
 *
 * <p>The fix: store ALL values as plain UTF-8 {@code String}s in Redis.
 * {@code IdempotencyService} serializes {@code PaymentResponse} objects to JSON strings
 * using a dedicated {@code ObjectMapper} bean before storing, and deserializes them on
 * read using the same mapper. No polymorphic type embedding is needed at all — we always
 * know exactly what type we stored for each key.
 *
 * <h2>Result in redis-cli</h2>
 * <pre>
 *   GET "idempotency:my-key"
 *   → "IN_FLIGHT"          (while processing)
 *   → "{\"id\":\"...\", ...}" (after completion — clean JSON string, no @class noise)
 * </pre>
 */
@Configuration
public class RedisConfig {

    /**
     * Lettuce connection factory pointing to localhost Redis on port 6379.
     */
    @Bean
    public LettuceConnectionFactory redisConnectionFactory() {
        RedisStandaloneConfiguration config = new RedisStandaloneConfiguration();
        config.setHostName("localhost");
        config.setPort(6379);
        config.setDatabase(0);
        return new LettuceConnectionFactory(config);
    }

    /**
     * RedisTemplate using String serializer for BOTH keys and values.
     *
     * <p>Both keys and values are stored as human-readable UTF-8 strings.
     * Values are pre-serialized to JSON strings by the service layer before
     * being passed to this template, so no Jackson serializer is needed here.
     * This avoids all polymorphic type-embedding problems entirely.
     *
     * @param connectionFactory the Lettuce connection factory
     * @return a fully configured RedisTemplate with String key and String value serializers
     */
    @Bean
    public RedisTemplate<String, String> redisTemplate(RedisConnectionFactory connectionFactory) {
        RedisTemplate<String, String> template = new RedisTemplate<>();
        template.setConnectionFactory(connectionFactory);

        // Use StringRedisSerializer for both keys and values.
        // Keys:   "idempotency:my-key-value"
        // Values: "IN_FLIGHT" | "{\"id\":\"...\", \"status\":\"SUCCESS\", ...}"
        StringRedisSerializer stringSerializer = new StringRedisSerializer();
        template.setKeySerializer(stringSerializer);
        template.setValueSerializer(stringSerializer);
        template.setHashKeySerializer(stringSerializer);
        template.setHashValueSerializer(stringSerializer);
        template.setDefaultSerializer(stringSerializer);

        template.afterPropertiesSet();
        return template;
    }

    /**
     * Shared ObjectMapper bean for serializing/deserializing {@code PaymentResponse}
     * to/from JSON strings stored in Redis.
     *
     * <p>Configured with:
     * <ul>
     *   <li>{@code JavaTimeModule} — serializes {@code LocalDateTime} as ISO-8601 strings
     *       (e.g. "2026-05-27T10:30:00") instead of a raw int array [2026,5,27,...]</li>
     *   <li>{@code WRITE_DATES_AS_TIMESTAMPS = false} — ensures dates are strings not arrays</li>
     * </ul>
     *
     * <p>This bean is injected into {@code IdempotencyService} and {@code IdempotencyFilter}
     * so that serialization and deserialization use the SAME mapper configuration —
     * eliminating any write/read format mismatch.
     *
     * @return a configured ObjectMapper
     */
    @Bean
    public ObjectMapper redisObjectMapper() {
        return new ObjectMapper()
                .registerModule(new JavaTimeModule())
                .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
    }
}
