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

// Kafka consumer configuration for processing PaymentEvent messages.
// Uses MANUAL_IMMEDIATE ack mode so @RetryableTopic controls offset commits.
// Non-blocking retries: failed messages are forwarded to retry topics instead of blocking the consumer.
@Configuration
public class KafkaConsumerConfig {

    private Map<String, Object> consumerProperties() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "payment-retry-group");
        // Start from earliest so retry messages are never skipped after a restart
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);
        // Trust our package so JsonDeserializer can instantiate PaymentEvent
        props.put(JsonDeserializer.TRUSTED_PACKAGES, "org.example.*");
        // Auto-commit disabled — container manages commits via AckMode
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 10);
        return props;
    }

    // Consumer factory typed to PaymentEvent
    @Bean
    public ConsumerFactory<String, PaymentEvent> paymentEventConsumerFactory() {
        return new DefaultKafkaConsumerFactory<>(
                consumerProperties(),
                new StringDeserializer(),
                // false = don't use type headers; always deserialize to PaymentEvent.class
                new JsonDeserializer<>(PaymentEvent.class, false)
        );
    }

    // Container factory wired with MANUAL_IMMEDIATE ack mode.
    // @RetryableTopic handles acknowledge() internally — no need to call it in listener code.
    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, PaymentEvent>
    kafkaListenerContainerFactory(ConsumerFactory<String, PaymentEvent> consumerFactory) {

        ConcurrentKafkaListenerContainerFactory<String, PaymentEvent> factory =
                new ConcurrentKafkaListenerContainerFactory<>();

        factory.setConsumerFactory(consumerFactory);
        // Single thread for local dev; increase to match partition count in production
        factory.setConcurrency(1);
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);

        return factory;
    }
}
