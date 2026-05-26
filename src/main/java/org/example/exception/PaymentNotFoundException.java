package org.example.exception;

import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.ResponseStatus;

/**
 * PaymentNotFoundException — thrown when a payment with the requested ID does not
 * exist in the local MySQL database.
 *
 * <p>{@code @ResponseStatus(HttpStatus.NOT_FOUND)} instructs Spring MVC to map this
 * exception to an HTTP 404 response when it is not caught by the
 * {@code GlobalExceptionHandler}.  However, since we do have a global handler,
 * the annotation serves primarily as documentation.
 */
@ResponseStatus(HttpStatus.NOT_FOUND)
public class PaymentNotFoundException extends RuntimeException {

    /**
     * Constructs the exception with a descriptive message embedding the payment ID.
     *
     * @param paymentId the UUID that was not found
     */
    public PaymentNotFoundException(String paymentId) {
        super("Payment not found with id: " + paymentId);
    }
}

