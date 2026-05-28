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

// Spring Data JPA repository for Payment entities.
// @Transactional on @Modifying methods ensures a transaction exists for JPQL UPDATE/DELETE.
// clearAutomatically=true clears the EntityManager cache after bulk updates so subsequent reads are fresh.
@Repository
public interface PaymentRepository extends JpaRepository<Payment, String> {

    // Finds a payment by its idempotency key (UNIQUE indexed column, O(log n) lookup)
    Optional<Payment> findByIdempotencyKey(String idempotencyKey);

    // Atomically updates status and retryCount, version-guarded (optimistic lock).
    // Returns 1 if updated, 0 if another thread already changed the version.
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

    // Atomically updates payment to SUCCESS with provider details, version-guarded.
    // Returns 1 if updated, 0 on version conflict.
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
