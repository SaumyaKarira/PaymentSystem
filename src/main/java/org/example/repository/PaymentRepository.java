package org.example.repository;

import org.example.entity.Payment;
import org.example.entity.PaymentStatus;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.util.Optional;

/**
 * PaymentRepository — Spring Data JPA repository for {@link Payment} entities.
 *
 * <p>Spring Data JPA auto-generates the implementation at startup by scanning all
 * interfaces that extend {@code JpaRepository}.  The generated proxy handles:
 * <ul>
 *   <li>Basic CRUD: {@code save()}, {@code findById()}, {@code delete()}, etc.</li>
 *   <li>Derived query methods like {@link #findByIdempotencyKey(String)}</li>
 *   <li>Custom JPQL queries annotated with {@code @Query}</li>
 * </ul>
 *
 * <h2>Optimistic Lock Collision Handling</h2>
 * <p>All save operations that modify the {@code version} field may throw
 * {@code ObjectOptimisticLockingFailureException} if another transaction concurrently
 * updated the same row.  Callers (service layer) must catch and handle this exception.
 */
@Repository
public interface PaymentRepository extends JpaRepository<Payment, String> {

    /**
     * Find a payment by its idempotency key.
     *
     * <p>Used by the idempotency filter and the orchestrator to check whether a payment
     * for a given request has already been initiated.  The underlying DB column has a
     * {@code UNIQUE} index so this lookup is O(log n).
     *
     * @param idempotencyKey the client-supplied idempotency key
     * @return an {@code Optional} containing the payment if found, empty otherwise
     */
    Optional<Payment> findByIdempotencyKey(String idempotencyKey);

    /**
     * Atomically update the status and retry count of a payment identified by its ID,
     * only if the current version matches the expected version.
     *
     * <p>This custom JPQL update is used by the Kafka consumer when it needs to update
     * the payment state WITHOUT first loading the full entity (avoiding an extra SELECT).
     * The {@code WHERE version = :version} clause is the application-level equivalent of
     * the Hibernate @Version check — it prevents overwriting a concurrent update.
     *
     * <p>Returns the number of rows updated:
     * <ul>
     *   <li>1 — the update was applied successfully</li>
     *   <li>0 — version mismatch (concurrent modification); caller should reload and retry</li>
     * </ul>
     *
     * @param id          the payment UUID
     * @param status      the new status to set
     * @param retryCount  the new retry count to set
     * @param version     the version the caller observed when it loaded the entity
     * @return number of rows affected (1 = success, 0 = optimistic lock conflict)
     */
    @Modifying
    @Query("""
            UPDATE Payment p
            SET p.status = :status,
                p.retryCount = :retryCount,
                p.version = p.version + 1
            WHERE p.id = :id
              AND p.version = :version
            """)
    int updateStatusWithVersionCheck(
            @Param("id") String id,
            @Param("status") PaymentStatus status,
            @Param("retryCount") int retryCount,
            @Param("version") long version
    );

    /**
     * Atomically update the status, provider details, and retry count for a payment,
     * subject to the same optimistic version check.
     *
     * <p>Called on the SUCCESS path to record which provider handled the payment and
     * the provider's own reference/transaction ID.
     *
     * @param id                    the payment UUID
     * @param status                new status (expected: SUCCESS)
     * @param providerId            the provider connector that succeeded
     * @param providerReferenceId   the transaction ID returned by the provider
     * @param retryCount            the retry count at the time of success
     * @param version               optimistic lock version guard
     * @return number of rows affected
     */
    @Modifying
    @Query("""
            UPDATE Payment p
            SET p.status = :status,
                p.providerId = :providerId,
                p.providerReferenceId = :providerReferenceId,
                p.retryCount = :retryCount,
                p.version = p.version + 1
            WHERE p.id = :id
              AND p.version = :version
            """)
    int updateOnSuccess(
            @Param("id") String id,
            @Param("status") PaymentStatus status,
            @Param("providerId") String providerId,
            @Param("providerReferenceId") String providerReferenceId,
            @Param("retryCount") int retryCount,
            @Param("version") long version
    );
}

