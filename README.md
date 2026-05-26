# Payment Orchestration System

A production-grade, event-driven **Payment Orchestration System** built with **Java 21** and **Spring Boot 3.x**. It demonstrates a clean layered architecture with synchronous REST APIs, idempotency via Redis, smart provider routing, and a fully automated non-blocking Kafka retry pipeline with Dead Letter Queue (DLQ) handling.

---

## Table of Contents

- [What This Project Does](#what-this-project-does)
- [Architecture Overview](#architecture-overview)
- [Technology Stack](#technology-stack)
- [Project Structure](#project-structure)
- [Prerequisites](#prerequisites)
- [Initial Setup](#initial-setup)
  - [1. MySQL Setup](#1-mysql-setup)
  - [2. Redis Setup](#2-redis-setup)
  - [3. Apache Kafka Setup](#3-apache-kafka-setup)
- [Running the Application](#running-the-application)
- [API Reference](#api-reference)
  - [Create Payment](#create-payment)
  - [Get Payment](#get-payment)
- [Payment Lifecycle & State Machine](#payment-lifecycle--state-machine)
- [Idempotency Behaviour](#idempotency-behaviour)
- [Kafka Retry & DLQ Architecture](#kafka-retry--dlq-architecture)
- [Provider Failover Strategy](#provider-failover-strategy)
- [Concurrency & Optimistic Locking](#concurrency--optimistic-locking)
- [Running Tests](#running-tests)
- [Sample cURL Commands](#sample-curl-commands)
- [Monitoring & Debugging](#monitoring--debugging)

---

## What This Project Does

The Payment Orchestration System acts as a **payment gateway router and retry engine**. When a client submits a payment:

1. **Idempotency** is enforced — duplicate requests within 24 hours are safely detected via Redis and return the original response without creating a duplicate charge.
2. The payment is **persisted** to MySQL in an `INITIATED` state.
3. A **Smart Routing Engine** selects the correct payment provider based on the payment method:
   - `CARD` payments → **Provider A**
   - `UPI` payments → **Provider B**
4. If the provider **succeeds** → payment is marked `SUCCESS` and the client gets a `201 Created` response.
5. If the provider **fails** (simulated 20% failure rate — 504 Gateway Timeout or 500 Internal Error):
   - Payment is transitioned to `PROCESSING`
   - A `PaymentEvent` is published to **Apache Kafka** (`payment-main-topic`)
   - The client immediately receives a `201` with `PROCESSING` status (non-blocking)
6. The **Kafka Retry Consumer** (`@RetryableTopic`) retries with **exponential backoff** (2s → 4s → 8s) across automatically generated retry topics.
7. On retries, a **provider failover** occurs — if Provider A failed for CARD, retries use Provider B.
8. If all retries are exhausted, the message lands on the **Dead Letter Topic** (`payment-main-topic-dlt`) and the payment is marked `FAILED`.
9. Throughout all of this, **Optimistic Locking** (`@Version` on the JPA entity) prevents race conditions and dirty writes between concurrent HTTP threads and Kafka consumer threads.

---

## Architecture Overview

```
Client
  │
  ▼
┌─────────────────────────────────────────────────────────┐
│  Spring Boot Application (localhost:8080)               │
│                                                         │
│  IdempotencyFilter  (Servlet Filter)                    │
│       │  checks Redis before any controller logic       │
│       ▼                                                 │
│  PaymentController  (REST Layer)                        │
│       │  POST /v1/payments  │  GET /v1/payments/{id}    │
│       ▼                                                 │
│  PaymentOrchestratorService  (Core Business Logic)      │
│       │                                                 │
│       ├── IdempotencyService  ──► Redis (localhost:6379)│
│       ├── PaymentRepository   ──► MySQL (localhost:3306)│
│       ├── RoutingEngine                                 │
│       │       ├── ProviderAConnector (CARD primary)     │
│       │       └── ProviderBConnector (UPI primary)      │
│       └── KafkaTemplate ──► Kafka (localhost:9092)      │
│                               │                         │
│                               ▼                         │
│  PaymentRetryConsumer  (@RetryableTopic)                │
│       ├── payment-main-topic        (attempt 1)         │
│       ├── payment-main-topic-retry-0 (attempt 2, +2s)  │
│       ├── payment-main-topic-retry-1 (attempt 3, +4s)  │
│       ├── payment-main-topic-retry-2 (attempt 4, +8s)  │
│       └── payment-main-topic-dlt    (mark FAILED)       │
└─────────────────────────────────────────────────────────┘
```

---

## Technology Stack

| Component        | Technology                        | Version    |
|------------------|-----------------------------------|------------|
| Language         | Java                              | 21         |
| Framework        | Spring Boot                       | 3.2.5      |
| Persistence      | Spring Data JPA + Hibernate       | 3.2.x      |
| Database         | MySQL                             | 8.x        |
| Cache / Lock     | Redis (via Lettuce client)        | 7.x        |
| Messaging        | Apache Kafka                      | 3.x        |
| Build Tool       | Maven                             | 3.8+       |
| Boilerplate      | Lombok                            | Latest     |
| Testing          | JUnit 5, Mockito, AssertJ, Awaitility, EmbeddedKafka | — |

---

## Project Structure

```
PaymentSystem/
├── pom.xml
├── README.md
└── src/
    ├── main/
    │   ├── java/org/example/
    │   │   ├── Main.java                         # Spring Boot entry point
    │   │   ├── config/
    │   │   │   ├── RedisConfig.java              # Lettuce Redis configuration
    │   │   │   ├── KafkaProducerConfig.java      # Kafka producer beans
    │   │   │   └── KafkaConsumerConfig.java      # Kafka consumer + @EnableRetryableTopic
    │   │   ├── controller/
    │   │   │   └── PaymentController.java        # REST endpoints
    │   │   ├── dto/
    │   │   │   ├── CreatePaymentRequest.java     # Inbound request DTO (record)
    │   │   │   ├── PaymentResponse.java          # Outbound response DTO (record)
    │   │   │   ├── PaymentEvent.java             # Kafka message payload (record)
    │   │   │   └── ErrorResponse.java            # Standardised error payload (record)
    │   │   ├── entity/
    │   │   │   ├── Payment.java                  # JPA entity with @Version
    │   │   │   ├── PaymentMethod.java            # Enum: CARD, UPI
    │   │   │   └── PaymentStatus.java            # Enum: INITIATED, PROCESSING, SUCCESS, FAILED
    │   │   ├── exception/
    │   │   │   ├── GlobalExceptionHandler.java   # @RestControllerAdvice
    │   │   │   ├── PaymentNotFoundException.java
    │   │   │   ├── IdempotencyConflictException.java
    │   │   │   └── ProviderException.java
    │   │   ├── filter/
    │   │   │   └── IdempotencyFilter.java        # OncePerRequestFilter for idempotency
    │   │   ├── kafka/
    │   │   │   └── PaymentRetryConsumer.java     # @RetryableTopic + @DltHandler
    │   │   ├── provider/
    │   │   │   ├── PaymentProviderConnector.java # Interface
    │   │   │   ├── ProviderAConnector.java       # CARD primary (20% simulated failure)
    │   │   │   └── ProviderBConnector.java       # UPI primary / CARD failover
    │   │   ├── repository/
    │   │   │   └── PaymentRepository.java        # Spring Data JPA + custom JPQL
    │   │   ├── routing/
    │   │   │   └── RoutingEngine.java            # Payment method → provider selection
    │   │   └── service/
    │   │       ├── IdempotencyService.java       # Redis lock + cache logic
    │   │       └── PaymentOrchestratorService.java # Core orchestration
    │   └── resources/
    │       ├── application.yml                   # All config (MySQL, Redis, Kafka)
    │       └── schema.sql                        # MySQL DDL (auto-run on startup)
    └── test/
        └── java/org/example/
            ├── service/
            │   └── PaymentOrchestratorServiceTest.java  # Mockito unit tests
            ├── filter/
            │   └── IdempotencyFilterTest.java           # @WebMvcTest slice tests
            └── kafka/
                └── PaymentConsumerIntegrationTest.java  # @EmbeddedKafka integration tests
```

---

## Prerequisites

Make sure the following are installed on your local machine before running the application.

| Tool        | Install Command (macOS/Homebrew)          | Verify                  |
|-------------|-------------------------------------------|-------------------------|
| Java 21     | `brew install openjdk@21`                 | `java -version`         |
| Maven 3.8+  | `brew install maven`                      | `mvn -version`          |
| MySQL 8.x   | `brew install mysql`                      | `mysql --version`       |
| Redis 7.x   | `brew install redis`                      | `redis-server --version`|
| Apache Kafka| `brew install kafka`                      | `kafka-topics.sh --version` |

> **Windows / Linux users:** Use the official download pages:
> - Java 21: https://adoptium.net
> - MySQL: https://dev.mysql.com/downloads/
> - Redis: https://redis.io/download
> - Kafka: https://kafka.apache.org/downloads

---

## Initial Setup

### 1. MySQL Setup

#### Start MySQL
```bash
# macOS (Homebrew)
brew services start mysql

# Linux (systemd)
sudo systemctl start mysql
```

#### Create the database and user
```bash
# Connect as root
mysql -u root -p
```

Inside the MySQL shell:
```sql
-- Create the database
CREATE DATABASE IF NOT EXISTS payment_orchestrator
    CHARACTER SET utf8mb4
    COLLATE utf8mb4_unicode_ci;

-- If your root user has no password set locally, run:
ALTER USER 'root'@'localhost' IDENTIFIED BY 'password';
FLUSH PRIVILEGES;

EXIT;
```

> The application is pre-configured with:
> - **Host:** `localhost:3306`
> - **Database:** `payment_orchestrator`
> - **Username:** `root`
> - **Password:** `password`
>
> To change these, edit `src/main/resources/application.yml` under `spring.datasource`.

> **Schema creation is automatic.** On first startup, Spring Boot runs `src/main/resources/schema.sql` which creates the `payments` table with all indexes. You do **not** need to run it manually.

---

### 2. Redis Setup

#### Start Redis
```bash
# macOS (Homebrew)
brew services start redis

# Linux (systemd)
sudo systemctl start redis

# Or start manually in the foreground:
redis-server
```

#### Verify Redis is running
```bash
redis-cli ping
# Expected output: PONG
```

> The application connects to Redis at `localhost:6379` with no password (standard local default).
> No additional Redis configuration is required.

---

### 3. Apache Kafka Setup

#### Option A — Homebrew (macOS, easiest)
```bash
# Install
brew install kafka

# Start ZooKeeper first
brew services start zookeeper

# Then start Kafka broker
brew services start kafka

# Verify both are running
brew services list | grep -E "kafka|zookeeper"
```

#### Option B — Manual (all platforms)
```bash
# Navigate to your Kafka installation directory
cd /path/to/kafka

# Terminal 1 — Start ZooKeeper
bin/zookeeper-server-start.sh config/zookeeper.properties

# Terminal 2 — Start Kafka broker
bin/kafka-server-start.sh config/server.properties
```

#### Verify Kafka is running
```bash
# List topics (should return empty or existing topics)
kafka-topics --list --bootstrap-server localhost:9092
```

> The application uses Kafka at `localhost:9092`. Retry topics and the DLT are **auto-created** by `@RetryableTopic` on first message publish — no manual topic creation needed.

---

## Running the Application

### Step 1 — Clone / open the project
```bash
cd /path/to/PaymentSystem
```

### Step 2 — Build the project
```bash
mvn clean install -DskipTests
```

### Step 3 — Start the application
```bash
mvn spring-boot:run
```

Or run the fat JAR directly:
```bash
java -jar target/PaymentSystem-1.0-SNAPSHOT.jar
```

### Step 4 — Confirm the application started
Look for this line in the logs:
```
Started Main in X.XXX seconds (process running for X.XXX)
```

The application is now live at:
```
http://localhost:8080
```

### Startup Checklist

Before starting, confirm all three backing services are up:

| Service   | Check Command                          | Expected        |
|-----------|----------------------------------------|-----------------|
| MySQL     | `mysql -u root -ppassword -e "SELECT 1"`| `1`            |
| Redis     | `redis-cli ping`                        | `PONG`         |
| Kafka     | `kafka-topics --list --bootstrap-server localhost:9092` | (no error) |

---

## API Reference

### Base URL
```
http://localhost:8080
```

---

### Create Payment

**`POST /v1/payments`**

Creates a new payment. This endpoint is **idempotent** — identical requests with the same `Idempotency-Key` are safe to retry.

#### Request Headers

| Header            | Required | Description                                        |
|-------------------|----------|----------------------------------------------------|
| `Content-Type`    | Yes      | Must be `application/json`                         |
| `Idempotency-Key` | Yes      | A unique string (UUID recommended) per payment intent. Requests sharing the same key within 24 hours return the original response. |

#### Request Body

```json
{
  "amount": 150.00,
  "currency": "USD",
  "paymentMethod": "CARD"
}
```

| Field           | Type       | Required | Constraints                          |
|-----------------|------------|----------|--------------------------------------|
| `amount`        | BigDecimal | Yes      | Must be greater than `0.00`          |
| `currency`      | String     | Yes      | 2–10 characters (ISO 4217 code)      |
| `paymentMethod` | Enum       | Yes      | `CARD` or `UPI`                      |

#### Response — `201 Created`

```json
{
  "id": "550e8400-e29b-41d4-a716-446655440000",
  "idempotencyKey": "my-unique-key-001",
  "amount": 150.00,
  "currency": "USD",
  "paymentMethod": "CARD",
  "status": "SUCCESS",
  "providerId": "PROVIDER_A",
  "providerReferenceId": "PROVA-A1B2C3D4E5F6G7H8",
  "retryCount": 0,
  "createdAt": "2026-05-27T10:30:00",
  "updatedAt": "2026-05-27T10:30:00.123"
}
```

> If the provider fails (20% chance), `status` will be `PROCESSING` and `providerReferenceId` will be `null`. Poll `GET /v1/payments/{id}` to track the outcome.

#### Error Responses

| HTTP Status | Scenario |
|-------------|----------|
| `400 Bad Request` | Missing `Idempotency-Key` header, invalid body, negative amount, unknown enum value |
| `409 Conflict` | A request with this `Idempotency-Key` is currently in-flight (concurrent duplicate) |
| `500 Internal Server Error` | Unexpected server error (Redis/MySQL/Kafka connectivity issue) |

---

### Get Payment

**`GET /v1/payments/{id}`**

Fetches the real-time status of a payment directly from MySQL. Always returns the freshest state — useful for polling during the async Kafka retry phase.

#### Path Parameter

| Parameter | Description                        |
|-----------|------------------------------------|
| `id`      | The UUID of the payment to fetch   |

#### Response — `200 OK`

```json
{
  "id": "550e8400-e29b-41d4-a716-446655440000",
  "idempotencyKey": "my-unique-key-001",
  "amount": 150.00,
  "currency": "USD",
  "paymentMethod": "CARD",
  "status": "PROCESSING",
  "providerId": "PROVIDER_A",
  "providerReferenceId": null,
  "retryCount": 2,
  "createdAt": "2026-05-27T10:30:00",
  "updatedAt": "2026-05-27T10:30:08"
}
```

#### Error Responses

| HTTP Status | Scenario |
|-------------|----------|
| `404 Not Found` | No payment with the given UUID exists in the database |

---

## Payment Lifecycle & State Machine

```
                    ┌─────────────┐
  POST /v1/payments │   INITIATED │
                    └──────┬──────┘
                           │
              ┌────────────┴─────────────┐
        Provider Success           Provider Failure
              │                          │
              ▼                          ▼
       ┌─────────────┐          ┌──────────────────┐
       │   SUCCESS   │          │    PROCESSING    │──── Kafka retries running
       └─────────────┘          └────────┬─────────┘
                                         │
                            ┌────────────┴──────────────┐
                      Retry succeeds             All retries fail
                            │                          │
                            ▼                          ▼
                     ┌─────────────┐           ┌─────────────┐
                     │   SUCCESS   │           │   FAILED    │
                     └─────────────┘           └─────────────┘
```

| Status       | Meaning                                                      |
|--------------|--------------------------------------------------------------|
| `INITIATED`  | Payment record created; synchronous provider call about to fire |
| `PROCESSING` | Primary provider failed; Kafka retry pipeline is active      |
| `SUCCESS`    | A provider returned a successful transaction reference       |
| `FAILED`     | All retry attempts exhausted; DLT handler set this terminal state |

---

## Idempotency Behaviour

Every `POST /v1/payments` request **must** include a unique `Idempotency-Key` header.

| Scenario | What Happens |
|----------|-------------|
| **New key** | Request proceeds normally through the full processing pipeline |
| **Duplicate key, in-flight** | Returns `409 Conflict` immediately — the original request is still being processed |
| **Duplicate key, completed** | Returns `200 OK` with the **exact same response** as the original — without touching the database or any provider |

**How it works internally:**
1. The `IdempotencyFilter` runs before the controller
2. It checks Redis for the key using an **atomic `SET NX`** (set-if-not-exists) operation
3. If the key exists as `IN_FLIGHT` → return 409
4. If the key maps to a completed `PaymentResponse` → return it directly (cache hit)
5. If the key is new → the service acquires the Redis lock and proceeds
6. On completion (success or processing), the response is stored in Redis with a **24-hour TTL**

---

## Kafka Retry & DLQ Architecture

When a provider fails, the payment is handed off to Kafka for non-blocking retry:

```
payment-main-topic          → Attempt 1 (immediate)
payment-main-topic-retry-0  → Attempt 2 (after 2 second backoff)
payment-main-topic-retry-1  → Attempt 3 (after 4 second backoff)
payment-main-topic-retry-2  → Attempt 4 (after 8 second backoff)
payment-main-topic-dlt      → Dead Letter Topic (all attempts failed → FAILED)
```

**Why non-blocking?**
Unlike blocking retries (`Thread.sleep`), Spring Kafka's `@RetryableTopic` writes a timestamp header (`kafka_backoff_timestamp`) and moves the message to the next retry topic immediately. The consumer pauses only the relevant partition until the backoff expires — keeping all other partitions and consumer threads free.

**Verifying Kafka topics:**
```bash
kafka-topics --list --bootstrap-server localhost:9092
```

**Consuming DLT messages for inspection:**
```bash
kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic payment-main-topic-dlt \
  --from-beginning
```

---

## Provider Failover Strategy

| Attempt | Payment Method | Provider Used     |
|---------|---------------|-------------------|
| 1       | CARD          | Provider A (primary) |
| 2+      | CARD          | Provider B (failover) |
| 1       | UPI           | Provider B (primary) |
| 2+      | UPI           | Provider A (failover) |

Both providers have a simulated **20% failure rate** (random 504 / 500 errors) to exercise the retry and failover paths. This rate is controlled by `FAILURE_RATE = 0.20` in `ProviderAConnector` and `ProviderBConnector`.

---

## Concurrency & Optimistic Locking

The `Payment` entity has a `@Version` field (mapped to the `version` column in MySQL). Every database UPDATE includes:

```sql
UPDATE payments
SET status = ?, version = version + 1, ...
WHERE id = ? AND version = <expected>
```

If two threads (e.g., a REST request and a Kafka consumer) both read version `N` and both try to update:
- **Thread A** commits → version becomes `N+1`
- **Thread B** tries with `WHERE version = N` → 0 rows affected → Hibernate throws `ObjectOptimisticLockingFailureException`
- The service catches this and logs a warning — the concurrent update wins, preventing any silent overwrite

---

## Running Tests

```bash
# Run all tests
mvn test

# Run only the unit tests (fast — no infrastructure needed)
mvn test -Dtest=PaymentOrchestratorServiceTest

# Run only the web layer slice tests (fast — no infrastructure needed)
mvn test -Dtest=IdempotencyFilterTest

# Run only the Kafka integration tests (uses embedded Kafka — takes ~45–90 seconds)
mvn test -Dtest=PaymentConsumerIntegrationTest

# Run all tests with verbose output
mvn test -Dsurefire.useFile=false
```

### Test Coverage Summary

| Test File | Type | Tests | What It Validates |
|-----------|------|-------|-------------------|
| `PaymentOrchestratorServiceTest` | Mockito Unit | 11 | Routing logic, sync/async state transitions, Kafka hand-off, DLT handler, optimistic lock warnings |
| `IdempotencyFilterTest` | `@WebMvcTest` Slice | 10 | Filter HTTP short-circuits (400/409), cache hits (200), Bean Validation, GlobalExceptionHandler (404) |
| `PaymentConsumerIntegrationTest` | `@EmbeddedKafka` Integration | 5 | Full Kafka retry lifecycle, failover routing, DLT terminal state, idempotent skip of terminal payments |

---

## Sample cURL Commands

### Create a CARD payment
```bash
curl -X POST http://localhost:8080/v1/payments \
  -H "Content-Type: application/json" \
  -H "Idempotency-Key: $(uuidgen)" \
  -d '{"amount": 150.00, "currency": "USD", "paymentMethod": "CARD"}'
```

### Create a UPI payment
```bash
curl -X POST http://localhost:8080/v1/payments \
  -H "Content-Type: application/json" \
  -H "Idempotency-Key: $(uuidgen)" \
  -d '{"amount": 500.00, "currency": "INR", "paymentMethod": "UPI"}'
```

### Fetch payment status
```bash
curl http://localhost:8080/v1/payments/{replace-with-payment-id}
```

### Test idempotency replay (run the same key twice)
```bash
KEY="my-fixed-key-$(date +%s)"

# First call — creates the payment
curl -X POST http://localhost:8080/v1/payments \
  -H "Content-Type: application/json" \
  -H "Idempotency-Key: $KEY" \
  -d '{"amount": 99.99, "currency": "USD", "paymentMethod": "CARD"}'

# Second call — returns the cached response (HTTP 200, no new payment)
curl -X POST http://localhost:8080/v1/payments \
  -H "Content-Type: application/json" \
  -H "Idempotency-Key: $KEY" \
  -d '{"amount": 99.99, "currency": "USD", "paymentMethod": "CARD"}'
```

### Test 400 — missing Idempotency-Key header
```bash
curl -X POST http://localhost:8080/v1/payments \
  -H "Content-Type: application/json" \
  -d '{"amount": 100.00, "currency": "USD", "paymentMethod": "CARD"}'
```

### Test 404 — unknown payment ID
```bash
curl http://localhost:8080/v1/payments/00000000-0000-0000-0000-000000000000
```

---

## Monitoring & Debugging

### Check Redis Idempotency Keys
```bash
# Open Redis CLI
redis-cli

# List all idempotency keys
KEYS idempotency:*

# Inspect a key's value
GET "idempotency:your-key-here"

# Check remaining TTL (in seconds)
TTL "idempotency:your-key-here"
```

### Inspect MySQL Records
```bash
mysql -u root -ppassword payment_orchestrator

SELECT id, left(idempotency_key, 20) AS idem_key, amount, currency,
       payment_method, status, provider_id,
       left(provider_reference_id, 20) AS prov_ref,
       retry_count, version, created_at
FROM payments
ORDER BY created_at DESC
LIMIT 20;
```

### Watch Application Logs
The application logs at `DEBUG` level for `org.example` classes. Key log patterns to watch:

| Log Pattern | Meaning |
|-------------|---------|
| `Payment [xxx] created in INITIATED state` | New payment successfully persisted |
| `Simulated 504 Gateway Timeout` | Provider failure triggered (retry will follow) |
| `Payment [xxx] published to Kafka topic` | Async retry pipeline activated |
| `Kafka retry consumer: processing payment ... attempt #2` | First Kafka retry in progress |
| `Payment [xxx] succeeded on retry attempt` | Kafka retry resolved the payment |
| `DLT HANDLER: Payment [xxx] has exhausted all retry attempts` | All retries failed, payment marked FAILED |
| `Optimistic lock conflict` | Concurrent thread detected — safe, handled gracefully |

### Kafka Topic Inspection
```bash
# Watch the main topic in real time
kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic payment-main-topic \
  --from-beginning

# Watch the DLT for failed payments
kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic payment-main-topic-dlt \
  --from-beginning

# Check consumer group lag
kafka-consumer-groups \
  --bootstrap-server localhost:9092 \
  --describe \
  --group payment-retry-group
```

---

## Configuration Reference

All configuration is in `src/main/resources/application.yml`. Key properties:

| Property | Default | Description |
|----------|---------|-------------|
| `spring.datasource.url` | `jdbc:mysql://localhost:3306/payment_orchestrator` | MySQL connection URL |
| `spring.datasource.username` | `root` | MySQL username |
| `spring.datasource.password` | `password` | MySQL password |
| `spring.data.redis.host` | `localhost` | Redis host |
| `spring.data.redis.port` | `6379` | Redis port |
| `spring.kafka.bootstrap-servers` | `localhost:9092` | Kafka broker address |
| `payment.idempotency.ttl-seconds` | `86400` | Redis key TTL (24 hours) |
| `payment.kafka.main-topic` | `payment-main-topic` | Main Kafka topic name |
| `server.port` | `8080` | HTTP server port |

