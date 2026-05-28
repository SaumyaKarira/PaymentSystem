package org.example.exception;

import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.ResponseStatus;

// Thrown when a payment with the requested UUID does not exist in MySQL.
// @ResponseStatus serves as documentation; GlobalExceptionHandler maps this to HTTP 404.
@ResponseStatus(HttpStatus.NOT_FOUND)
public class PaymentNotFoundException extends RuntimeException {

    public PaymentNotFoundException(String paymentId) {
        super("Payment not found with id: " + paymentId);
    }
}
