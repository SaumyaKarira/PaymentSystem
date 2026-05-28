package org.example.exception;

import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.ResponseStatus;

// Thrown when an Idempotency-Key maps to a payment that is currently in-flight (PROCESSING).
// Returns HTTP 409 — tells the client to wait and poll GET /v1/payments/{id} for the outcome.
@ResponseStatus(HttpStatus.CONFLICT)
public class IdempotencyConflictException extends RuntimeException {

    public IdempotencyConflictException(String idempotencyKey) {
        super("A payment with idempotency key '" + idempotencyKey
                + "' is currently being processed. "
                + "Poll GET /v1/payments/{id} for the latest status.");
    }
}
