package org.example.repository;

import org.example.entity.Payment;
import org.example.entity.PaymentStatus;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

import java.util.Optional;

/**
 * PaymentRepository — Spring Data JPA repository for {@link Payment} entities.
 *
 * <h2>Why @Transactional is required on @Modifying queries</h2>
 * <p>Spring Data JPA's {@code @Modifying} annotation marks a JPQL query as an
 * UPDATE or DELETE statement. JPA requires an active transaction to execute any
 * write operation. If no transaction is already active when the method is called,
 * the JPA provider throws:
 * <pre>
 *   javax.persistence.TransactionRequiredException:
 *       Executing an update/delete query
 * </pre>
 *
 * <p>Adding {@code @Transactional} directly on these repository methods guarantees
 * that a transaction is always started (or joined if one already exists) before the
 * JPQL UPDATE executes — regardless of whether the caller has its own transaction.
 *
 * <h2>Transaction propagation</h2>
 * <p>The default propagation is {@code REQUIRED}: if the caller already has an active
 * transaction (e.g., a service method annotated with {@code @Transactional}), these
 * methods participate in it. If no transaction exists, a new one is started and
 * committed when the method returns. This is safe in both cases.
 *
 * <h2>clearAutomatically on @Modifying</h2>
 * <p>{@code @Modifying(clearAutomatically = true)} clears the JPA first-level cache
 * (EntityManager) after the JPQL UPDATE executes. This is critical because a JPQL
 * bulk UPDATE bypasses the EntityManager cache — without clearing it, any subsequent
 * {@code findById()} in the same transaction would return the stale cached entity
 * (with the old status) rather than the updated row from the database.
 */
@Repository
public interface PaymentRepository extends JpaRepository<Payment, String> {

    /**
     * Find a payment by its idempotency key.
     *
     * <p>Used by the idempotency filter and the orchestrator to check whether a payment
     * for a given request has already been initiated. The underlying DB column has a
     * {@code UNIQUE} index so this lookup is O(log n).
     *
     * @param idempotencyKey the client-supplied idempotency key
     * @return an {@code Optional} containing the payment if found, empty otherwise
     */
    Optional<Payment> findByIdempotencyKey(String idempotencyKey);

    /**
     * Atomically update the status and retry count of a payment, version-guarded.
     *
     * <h3>Why @Transactional here</h3>
     * <p>JPQL UPDATE/DELETE statements require an active JPA transaction.
     * {@code @Transactional} on this method ensures a transaction always exists,
     * even when called from a non-transactional context. If the caller already
     * has a transaction open (REQUIRED propagation), this method joins it.
     *
     * <h3>Why clearAutomatically = true</h3>
     * <p>JPQL bulk updates go directly to the database and bypass the JPA
     * EntityManager first-level cache. Without clearing the cache after this
     * UPDATE, any subsequent {@code findById()} in the same persistence context
     * would return the stale pre-update entity from the cache. Setting
     * {@code clearAutomatically = true} forces the EntityManager to clear its
     * cache after the UPDATE, so the next read always fetches fresh data from DB.
     *
     * <h3>Optimistic lock guard: WHERE version = :version</h3>
     * <p>Returns 1 if updated, 0 if another thread already changed the version.
     * Callers check the return value to detect concurrent modification.
     *
     * @param id          the payment UUID
     * @param status      the new status to set
     * @param retryCount  the new retry count to set
     * @param version     the version the caller observed when it last read the entity
     * @return number of rows affected (1 = success, 0 = optimistic lock conflict)
     */
    @Modifying(clearAutomatically = true)
    @Transactional
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
     * Atomically update payment to SUCCESS state with provider details, version-guarded.
     *
     * <h3>Why @Transactional here</h3>
     * <p>Same reason as {@link #updateStatusWithVersionCheck}: JPQL UPDATE requires
     * an active transaction. {@code @Transactional} guarantees this at the repository
     * method boundary as the innermost safety net.
     *
     * <h3>Why clearAutomatically = true</h3>
     * <p>After this JPQL UPDATE commits, the EntityManager cache still holds the old
     * Payment entity (with status=INITIATED or PROCESSING). Clearing it ensures the
     * subsequent {@code findById()} in {@code PaymentOrchestratorService} reads the
     * updated row (status=SUCCESS, providerReferenceId populated) from the database.
     *
     * @param id                    the payment UUID
     * @param status                new status (SUCCESS)
     * @param providerId            the connector that succeeded
     * @param providerReferenceId   the provider's transaction reference
     * @param retryCount            retry count at time of success
     * @param version               optimistic lock version guard
     * @return number of rows affected (1 = success, 0 = version conflict)
     */
    @Modifying(clearAutomatically = true)
    @Transactional
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
