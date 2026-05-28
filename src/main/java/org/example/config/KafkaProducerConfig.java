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

// Kafka producer configuration for publishing PaymentEvent messages.
// Producer idempotence + acks=all provides strongest durability guarantee.
@Configuration
public class KafkaProducerConfig {

    private Map<String, Object> producerProperties() {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        // Keys are payment UUIDs (String); values are PaymentEvent serialized to JSON
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
        // Wait for all in-sync replicas to acknowledge
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        // Producer idempotence prevents duplicate messages on retry (PID + sequence dedup)
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        props.put(ProducerConfig.RETRIES_CONFIG, 3);
        // Batch messages arriving within 5ms for better throughput
        props.put(ProducerConfig.LINGER_MS_CONFIG, 5);
        return props;
    }

    // Producer factory for PaymentEvent messages
    @Bean
    public ProducerFactory<String, PaymentEvent> paymentEventProducerFactory() {
        return new DefaultKafkaProducerFactory<>(producerProperties());
    }

    // Thread-safe KafkaTemplate for publishing PaymentEvent messages.
    // Using payment ID as message key routes all events for the same payment to the same partition.
    @Bean
    public KafkaTemplate<String, PaymentEvent> kafkaTemplate(
            ProducerFactory<String, PaymentEvent> producerFactory) {
        return new KafkaTemplate<>(producerFactory);
    }
}
