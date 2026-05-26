package org.example.config;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.jsontype.impl.LaissezFaireSubTypeValidator;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.connection.RedisStandaloneConfiguration;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.serializer.GenericJackson2JsonRedisSerializer;
import org.springframework.data.redis.serializer.StringRedisSerializer;

/**
 * RedisConfig — configures the Spring Data Redis infrastructure for local development.
 *
 * <h2>Connection</h2>
 * <p>Targets a Redis instance running on {@code localhost:6379} with no password
 * (standard local Redis default settings).  The Lettuce client is used (Spring Boot's
 * default Redis client) because it is non-blocking, thread-safe, and shares a single
 * connection across multiple threads — ideal for our idempotency use case where many
 * HTTP threads access Redis concurrently.
 *
 * <h2>Serialization</h2>
 * <p>We configure the {@code RedisTemplate} to use:
 * <ul>
 *   <li>{@code StringRedisSerializer} for keys — keeps Redis keys human-readable
 *       (important for debugging via {@code redis-cli})</li>
 *   <li>{@code GenericJackson2JsonRedisSerializer} for values — serializes Java objects
 *       to JSON with embedded type information so they can be deserialized back correctly
 *       even if the value is a complex object like {@code PaymentResponse}</li>
 * </ul>
 *
 * <h2>Why not use Spring Boot's auto-configured RedisTemplate?</h2>
 * <p>Spring Boot auto-configures a {@code RedisTemplate<Object, Object>} which uses
 * Java serialization by default.  Java-serialized values are opaque binary blobs that
 * are unreadable in {@code redis-cli}.  Our custom template uses JSON serialization,
 * making it easy to inspect idempotency keys with: {@code redis-cli GET "idempotency:KEY"}
 */
@Configuration
public class RedisConfig {

    /**
     * Lettuce connection factory pointing to localhost Redis.
     *
     * <p>Lettuce uses a single shared connection per thread group (Netty EventLoop).
     * There is no connection pool in the traditional sense for standalone mode,
     * but we configure a pool in {@code application.yml} for connection reuse safety.
     *
     * @return a configured {@code LettuceConnectionFactory}
     */
    @Bean
    public LettuceConnectionFactory redisConnectionFactory() {
        // RedisStandaloneConfiguration holds host/port/database/password for a single node.
        // For a Redis Cluster, use RedisClusterConfiguration instead.
        RedisStandaloneConfiguration config = new RedisStandaloneConfiguration();
        config.setHostName("localhost");
        config.setPort(6379);
        // No password for local Redis; leave as-is or call config.setPassword("...") if needed
        config.setDatabase(0);
        return new LettuceConnectionFactory(config);
    }

    /**
     * Custom {@code RedisTemplate} with String keys and JSON-serialized values.
     *
     * <p>This bean is injected into {@code IdempotencyService} as
     * {@code RedisTemplate<String, Object>}.
     *
     * @param connectionFactory the Lettuce connection factory defined above
     * @return a fully configured {@code RedisTemplate}
     */
    @Bean
    public RedisTemplate<String, Object> redisTemplate(RedisConnectionFactory connectionFactory) {
        RedisTemplate<String, Object> template = new RedisTemplate<>();
        template.setConnectionFactory(connectionFactory);

        // ── KEY SERIALIZER ────────────────────────────────────────────────────
        // StringRedisSerializer encodes/decodes keys as plain UTF-8 strings.
        // This makes Redis keys human-readable: "idempotency:some-key-value"
        StringRedisSerializer stringSerializer = new StringRedisSerializer();
        template.setKeySerializer(stringSerializer);
        template.setHashKeySerializer(stringSerializer);

        // ── VALUE SERIALIZER ──────────────────────────────────────────────────
        // Build a custom ObjectMapper with:
        //   1. JavaTimeModule — so LocalDateTime is serialized as an ISO-8601 string
        //      rather than a raw timestamp array.
        //   2. ActivateDefaultTyping — embeds the Java class name in the JSON so that
        //      the deserializer knows which class to instantiate (required for polymorphism).
        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.registerModule(new JavaTimeModule());
        // Embed type info as a property named "@class" in the JSON.
        // LaissezFaireSubTypeValidator allows all types (safe for local dev).
        // In production, use a more restrictive validator to prevent deserialization attacks.
        objectMapper.activateDefaultTyping(
                LaissezFaireSubTypeValidator.instance,
                ObjectMapper.DefaultTyping.NON_FINAL,
                JsonTypeInfo.As.PROPERTY
        );
        GenericJackson2JsonRedisSerializer jsonSerializer =
                new GenericJackson2JsonRedisSerializer(objectMapper);

        template.setValueSerializer(jsonSerializer);
        template.setHashValueSerializer(jsonSerializer);

        // Initialize the template (sets up the serializers internally)
        template.afterPropertiesSet();
        return template;
    }
}

