package org.example.exception;

import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.ResponseStatus;

/**
 * IdempotencyConflictException — thrown when a request arrives with an
 * {@code Idempotency-Key} that maps to a payment currently in the PROCESSING state
 * (i.e., the original request is still in-flight or being retried via Kafka).
 *
 * <p>Returning HTTP 409 Conflict tells the client: "Your request was received and is
 * being worked on — do not submit it again yet."  The client should poll
 * {@code GET /v1/payments/{id}} to check the outcome.
 *
 * <p>This is distinct from a duplicate key for a COMPLETED payment, which returns the
 * cached response directly (handled in {@code IdempotencyFilter}).
 */
@ResponseStatus(HttpStatus.CONFLICT)
public class IdempotencyConflictException extends RuntimeException {

    /**
     * @param idempotencyKey the key that caused the conflict
     */
    public IdempotencyConflictException(String idempotencyKey) {
        super("A payment with idempotency key '" + idempotencyKey
                + "' is currently being processed. "
                + "Poll GET /v1/payments/{id} for the latest status.");
    }
}

