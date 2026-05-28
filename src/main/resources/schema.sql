-- =============================================================================
-- schema.sql — Payment Orchestration System
-- Target Database : payment_orchestrator (local MySQL)
-- Run once manually OR let Spring Boot execute it via sql.init.mode=always
-- =============================================================================

-- Create the database if it doesn't already exist (safe for re-runs)
CREATE DATABASE IF NOT EXISTS payment_orchestrator;

USE payment_orchestrator;

-- =============================================================================
-- TABLE: payments
-- Core entity storing every payment lifecycle record.
-- =============================================================================
CREATE TABLE IF NOT EXISTS payments
(
    -- Primary key: UUID stored as CHAR(36) for readability and portability.
    -- Using CHAR(36) is intentional here; in ultra-high-throughput systems a
    -- BINARY(16) representation is faster, but CHAR(36) aids local debugging.
    id                    CHAR(36)       NOT NULL,

    -- idempotency_key: supplied by the API caller via the Idempotency-Key header.
    -- UNIQUE constraint enforces that no two payments share the same key at the
    -- DB level (Redis is the fast-path guard; DB unique index is the safety net).
    idempotency_key       VARCHAR(255)   NOT NULL,

    -- Monetary amount. DECIMAL(19,4) is the standard for financial amounts:
    -- 19 total digits, 4 decimal places; avoids floating-point representation errors.
    amount                DECIMAL(19, 4) NOT NULL,

    -- ISO 4217 currency code (e.g., USD, INR, EUR)
    currency              VARCHAR(10)    NOT NULL,

    -- payment_method: ENUM restricted at DB level to prevent invalid values
    payment_method        ENUM ('CARD', 'UPI') NOT NULL,

    -- status: lifecycle state machine value
    -- INITIATED  → payment record created, synchronous processing about to begin
    -- PROCESSING → primary provider call failed; pushed to Kafka retry pipeline
    -- SUCCESS    → a provider returned a successful response
    -- FAILED     → all retry attempts exhausted; DLT handler marked this FAILED
    status                ENUM ('INITIATED', 'PROCESSING', 'SUCCESS', 'FAILED') NOT NULL DEFAULT 'INITIATED',

    -- provider_id: identifies which connector handled or is handling this payment
    -- (e.g., "PROVIDER_A", "PROVIDER_B").  Nullable until routing occurs.
    provider_id           VARCHAR(100)   NULL,

    -- provider_reference_id: the transaction ID returned by the external provider.
    -- Stored for reconciliation and customer support queries.
    provider_reference_id VARCHAR(255)   NULL,

    -- retry_count: incremented each time the Kafka retry consumer re-attempts the payment.
    -- Starts at 0; max value mirrors @RetryableTopic(attempts = 3).
    retry_count           INT            NOT NULL DEFAULT 0,

    -- version: used by Hibernate @Version for Optimistic Locking.
    -- Every UPDATE must include WHERE version = <current> and increments this by 1.
    -- If two concurrent transactions try to update the same row simultaneously,
    -- the second one will find version has changed and throw
    -- ObjectOptimisticLockingFailureException, preventing a dirty write.
    version               BIGINT         NOT NULL DEFAULT 0,

    -- Audit timestamps: set at INSERT time; updated_at refreshed on every UPDATE.
    created_at            DATETIME(6)    NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    updated_at            DATETIME(6)    NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),

    -- -------------------------------------------------------------------------
    -- CONSTRAINTS
    -- -------------------------------------------------------------------------
    PRIMARY KEY (id),

    -- Unique index on idempotency_key: prevents duplicate payment records for the
    -- same API request. Redis is the first line of defence; this is the DB safety net.
    UNIQUE INDEX uq_idempotency_key (idempotency_key),

    -- Composite index on (status, created_at): accelerates queries that filter
    -- payments by status for monitoring dashboards or batch reconciliation jobs.
    INDEX idx_status_created_at (status, created_at),

    -- Index on payment_method for routing analytics queries
    INDEX idx_payment_method (payment_method)

) ENGINE = InnoDB
  DEFAULT CHARSET = utf8mb4
  COLLATE = utf8mb4_unicode_ci
  COMMENT = 'Stores all payment lifecycle records for the Payment Orchestration System';

