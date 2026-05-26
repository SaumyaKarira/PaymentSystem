package org.example.config;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.example.dto.PaymentEvent;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.support.serializer.JsonDeserializer;

import java.util.HashMap;
import java.util.Map;

/**
 * KafkaConsumerConfig — configures the Kafka consumer infrastructure for processing
 * {@link PaymentEvent} messages from the retry topic pipeline.
 *
 * <h2>Non-Blocking Retry Architecture (Spring Kafka @RetryableTopic)</h2>
 * <p>Spring Kafka's {@code @RetryableTopic} implements non-blocking retries by
 * generating <strong>intermediate retry topics</strong> automatically:
 *
 * <pre>
 *   payment-main-topic
 *       │  (attempt 1 fails)
 *       ▼
 *   payment-main-topic-retry-0    ← retry attempt 2 (after 2s delay)
 *       │  (attempt 2 fails)
 *       ▼
 *   payment-main-topic-retry-1    ← retry attempt 3 (after 4s delay — exponential)
 *       │  (attempt 3 fails)
 *       ▼
 *   payment-main-topic-dlt        ← Dead Letter Topic
 * </pre>
 *
 * <p>The naming convention is {@code {original-topic}-retry-{n}} for each retry attempt
 * and {@code {original-topic}-dlt} for the dead letter topic.  Spring Kafka creates these
 * topics automatically via the {@code RetryTopicConfigurationSupport} infrastructure.
 *
 * <h2>Why Non-Blocking?</h2>
 * <p>"Non-blocking" means the consumer does NOT block/sleep while waiting for the retry
 * backoff delay.  Instead:
 * <ol>
 *   <li>The failed message is immediately published to the next retry topic</li>
 *   <li>A header ({@code kafka_backoff_timestamp}) records when the message should be
 *       re-processed</li>
 *   <li>The retry consumer pauses its partition until that timestamp, then resumes</li>
 * </ol>
 * <p>This keeps the main consumer thread free to process other messages instead of
 * sleeping, dramatically improving throughput under partial failure conditions.
 *
 * <h2>AckMode.MANUAL_IMMEDIATE</h2>
 * <p>We use {@code MANUAL_IMMEDIATE} acknowledgment mode so that Spring Kafka's retry
 * infrastructure can control exactly when a message's offset is committed.  If a message
 * fails and needs to be forwarded to a retry topic, the offset must NOT be committed
 * until after the forwarding succeeds.  Auto-commit would commit the offset before the
 * retry delivery is confirmed, potentially losing the message.
 */
@Configuration
public class KafkaConsumerConfig {

    /**
     * Kafka consumer properties for the payment retry group.
     *
     * @return configuration map for the consumer
     */
    private Map<String, Object> consumerProperties() {
        Map<String, Object> props = new HashMap<>();

        // Connect to the local Kafka broker
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

        // Consumer group ID: all instances of this service share this group.
        // Kafka distributes partitions across all instances in the group for horizontal scaling.
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "payment-retry-group");

        // Start from the earliest available offset when no committed offset exists.
        // Critical for retry topics — we never want to skip a retry message after restart.
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // Deserialize message keys as plain strings
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

        // Deserialize message values from JSON back to PaymentEvent
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);

        // Trust our application package so JsonDeserializer can instantiate PaymentEvent.
        // Without this, JsonDeserializer throws IllegalArgumentException for unknown types.
        props.put(JsonDeserializer.TRUSTED_PACKAGES, "org.example.*");

        // Disable Kafka's auto-commit of consumer offsets.
        // With auto-commit disabled, the container manages offset commits via AckMode,
        // ensuring a message is only committed after it's been processed or forwarded
        // to a retry/DLT topic by the @RetryableTopic infrastructure.
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

        // Maximum number of records returned in a single poll.
        // Kept low for local development to make message processing observable.
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 10);

        return props;
    }

    /**
     * Consumer factory typed to {@link PaymentEvent}.
     *
     * <p>The factory creates new Kafka consumer instances (one per listener container
     * thread) using the configuration map above.
     *
     * @return a typed consumer factory for PaymentEvent messages
     */
    @Bean
    public ConsumerFactory<String, PaymentEvent> paymentEventConsumerFactory() {
        return new DefaultKafkaConsumerFactory<>(
                consumerProperties(),
                new StringDeserializer(),
                // JsonDeserializer configured to deserialize directly to PaymentEvent.class.
                // The second boolean parameter (false) means: do NOT use the type header from
                // the Kafka message; always deserialize to the specified type class.
                // We set it to true here to use Spring's type mapping headers for flexibility.
                new JsonDeserializer<>(PaymentEvent.class, false)
        );
    }

    /**
     * Listener container factory that wires the consumer factory with the correct AckMode.
     *
     * <p>The {@code ConcurrentKafkaListenerContainerFactory} creates
     * {@code ConcurrentMessageListenerContainer} instances — each of which can run
     * multiple consumer threads concurrently (controlled by {@code setConcurrency}).
     * For local development we use concurrency=1 (single-threaded) to simplify log output.
     *
     * <p><strong>AckMode.MANUAL_IMMEDIATE</strong>: The listener must explicitly call
     * {@code Acknowledgment.acknowledge()} to commit the offset.  Spring Kafka's
     * {@code @RetryableTopic} infrastructure does this automatically after successful
     * processing or after forwarding to a retry/DLT topic.  We do NOT need to call
     * {@code ack.acknowledge()} manually in our {@code @KafkaListener} methods.
     *
     * @param consumerFactory the typed consumer factory
     * @return a fully configured listener container factory
     */
    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, PaymentEvent>
    kafkaListenerContainerFactory(ConsumerFactory<String, PaymentEvent> consumerFactory) {

        ConcurrentKafkaListenerContainerFactory<String, PaymentEvent> factory =
                new ConcurrentKafkaListenerContainerFactory<>();

        factory.setConsumerFactory(consumerFactory);

        // Single consumer thread per partition for local dev observability.
        // Increase to the number of partitions for production throughput.
        factory.setConcurrency(1);

        // MANUAL_IMMEDIATE: offset is committed immediately when acknowledge() is called.
        // @RetryableTopic handles the acknowledge() call internally.
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);

        return factory;
    }
}

