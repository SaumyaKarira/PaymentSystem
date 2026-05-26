package org.example.config;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.example.dto.PaymentEvent;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.serializer.JsonSerializer;

import java.util.HashMap;
import java.util.Map;

/**
 * KafkaProducerConfig — configures the Kafka producer infrastructure for publishing
 * {@link PaymentEvent} messages to the local Kafka broker.
 *
 * <h2>Design Decisions</h2>
 * <ul>
 *   <li>We define an explicit {@code ProducerFactory<String, PaymentEvent>} typed to our
 *       event DTO. This prevents accidental type mismatches at compile time.</li>
 *   <li>{@code enable.idempotence=true} ensures that a producer retry (due to a transient
 *       network hiccup) does not result in duplicate messages in the Kafka topic.
 *       This is Kafka's producer-side exactly-once guarantee.</li>
 *   <li>{@code acks=all} ensures the broker leader AND all in-sync replicas acknowledge
 *       the message before the producer considers it sent. Combined with idempotence,
 *       this provides the strongest durability guarantee available.</li>
 * </ul>
 *
 * <h2>Local Kafka Assumptions</h2>
 * <p>The broker is expected to be running on {@code localhost:9092}.
 * Start Kafka locally with:
 * <pre>
 *   # Start ZooKeeper (if not using KRaft):
 *   bin/zookeeper-server-start.sh config/zookeeper.properties
 *
 *   # Start Kafka broker:
 *   bin/kafka-server-start.sh config/server.properties
 * </pre>
 */
@Configuration
public class KafkaProducerConfig {

    /**
     * Kafka producer properties pointing to the local broker.
     *
     * @return a map of Kafka producer configuration entries
     */
    private Map<String, Object> producerProperties() {
        Map<String, Object> props = new HashMap<>();

        // Local Kafka broker address
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

        // Serialize message keys as plain strings (the payment UUID will be the key)
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        // Serialize message values as JSON using Spring Kafka's JsonSerializer.
        // The JsonSerializer writes the Java class name as a Kafka header
        // (spring_json_header_types) so the consumer can deserialize back to PaymentEvent.
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);

        // ack=all: Strongest durability — wait for all in-sync replicas to acknowledge.
        // On a single-node local broker, this effectively means just the leader.
        props.put(ProducerConfig.ACKS_CONFIG, "all");

        // Enable producer idempotence: assigns each producer a unique ID (PID) and
        // attaches a sequence number to each message.  If the broker receives a duplicate
        // (due to a producer retry), it deduplicates using (PID, PartitionID, SequenceNo).
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);

        // Retry up to 3 times on transient send failures before surfacing the exception
        props.put(ProducerConfig.RETRIES_CONFIG, 3);

        // Allow Kafka to batch messages that arrive within 5ms of each other.
        // Improves throughput without meaningfully impacting latency for our use case.
        props.put(ProducerConfig.LINGER_MS_CONFIG, 5);

        return props;
    }

    /**
     * Producer factory for {@code PaymentEvent} messages.
     *
     * <p>{@code DefaultKafkaProducerFactory} creates a pool of Kafka producers
     * backed by the specified configuration. Spring Kafka manages the lifecycle
     * of these producers.
     *
     * @return a typed producer factory
     */
    @Bean
    public ProducerFactory<String, PaymentEvent> paymentEventProducerFactory() {
        return new DefaultKafkaProducerFactory<>(producerProperties());
    }

    /**
     * KafkaTemplate for publishing {@link PaymentEvent} messages.
     *
     * <p>{@code KafkaTemplate} is the Spring abstraction over the Kafka producer API.
     * It is thread-safe and should be injected as a singleton wherever Kafka publishing
     * is needed.
     *
     * <p>Usage in service layer:
     * <pre>
     *   kafkaTemplate.send("payment-main-topic", paymentId, paymentEvent);
     * </pre>
     * The message key ({@code paymentId}) ensures that all events for the same payment
     * are routed to the same Kafka partition, preserving order for that payment.
     *
     * @param producerFactory the typed producer factory defined above
     * @return a ready-to-use KafkaTemplate
     */
    @Bean
    public KafkaTemplate<String, PaymentEvent> kafkaTemplate(
            ProducerFactory<String, PaymentEvent> producerFactory) {
        return new KafkaTemplate<>(producerFactory);
    }
}

