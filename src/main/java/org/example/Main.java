package org.example;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * PaymentOrchestrationApplication — Spring Boot 3.x entry point.
 *
 * <p>{@code @SpringBootApplication} is a convenience annotation that combines:
 * <ul>
 *   <li>{@code @Configuration}       — marks this class as a source of bean definitions</li>
 *   <li>{@code @EnableAutoConfiguration} — lets Spring Boot auto-configure beans based on
 *       classpath dependencies (e.g., DataSource, RedisTemplate, KafkaTemplate)</li>
 *   <li>{@code @ComponentScan}        — scans the {@code org.example} package and all
 *       sub-packages for Spring-managed components</li>
 * </ul>
 */
@SpringBootApplication
public class Main {

    public static void main(String[] args) {
        SpringApplication.run(Main.class, args);
    }
}