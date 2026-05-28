package org.example;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

// Spring Boot entry point — bootstraps the application context and component scan.
@SpringBootApplication
public class PaymentOrchestrationApplication {

    public static void main(String[] args) {
        SpringApplication.run(PaymentOrchestrationApplication.class, args);
    }
}

