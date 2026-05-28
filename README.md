# Payment Orchestration System

A event-driven **Payment Orchestration System** built with **Java 21** and **Spring Boot 3.x**. It demonstrates a clean layered architecture with synchronous REST APIs, idempotency via Redis, smart provider routing, and a fully automated non-blocking Kafka retry pipeline with Dead Letter Queue (DLQ) handling.

---

## Table of Contents

- [What This Project Does](#what-this-project-does)
- [System Architecture Blueprint](#system-architecture-blueprint)
- [Technology Stack](#technology-stack)
- [Infrastructure Setup (Localhost)](#infrastructure-setup-localhost)
- [Execution & Quick-Start Guide](#execution--quick-start-guide)
- [Testing Idempotency & Payment Routing](#testing-idempotency--payment-routing)
- [Create Payment](#create-payment)
- [Get Payment](#get-payment)
- [Payment Lifecycle & State Machine](#payment-lifecycle--state-machine)
- [Idempotency Behaviour](#idempotency-behaviour)
- [Kafka Retry & DLQ Architecture](#kafka-retry--dlq-architecture)
- [Provider Routing & Retry Strategy](#provider-routing--retry-strategy)

---

## What This Project Does

The Payment Orchestration System acts as a **payment gateway router and retry engine**. When a client submits a payment:

1. **Idempotency** is enforced — duplicate requests within 24 hours are safely detected via Redis and return the original response without creating a duplicate charge.
2. The payment is **persisted** to MySQL in an `INITIATED` state.
3. A **Smart Routing Engine** selects the correct payment provider based on the payment method:
   - `CARD` payments → **Provider A** (primary)
   - `UPI` payments → **Provider B** (primary)
4. If the provider **succeeds** → payment is marked `SUCCESS` and the client gets a `201 Created` response.
5. If the provider **fails** (simulated 20% failure rate — 504 Gateway Timeout or 500 Internal Error):
   - Payment is transitioned to `PROCESSING`
   - A `PaymentEvent` is published to **Apache Kafka** (`payment-main-topic`)
   - The client immediately receives a `201` with `PROCESSING` status (non-blocking)
6. The **Kafka Retry Consumer** (`@RetryableTopic`) retries with **exponential backoff** (2s → 4s → 8s) across automatically generated retry topics.
7. All retry attempts use the **same primary provider** that was selected initially (no provider failover — this is by design).
8. If all retries are exhausted, the message lands on the **Dead Letter Topic** (`payment-main-topic-dlt`) and the payment is marked `FAILED`.
9. Throughout all of this, **Optimistic Locking** (`@Version` on the JPA entity) prevents race conditions and dirty writes between concurrent HTTP threads and Kafka consumer threads.

---

## System Architecture Blueprint

The Payment Orchestration System uses a clean **layered architecture** with two distinct payment processing paths: **Synchronous REST** (happy path) and **Asynchronous Kafka Retry** (resilience path).

### Layered Architecture Overview

```
┌─────────────────────────────────────────────────────────────────┐
│  Presentation Layer (HTTP)                                      │
│  • IdempotencyFilter (Servlet filter)                           │
│  • PaymentController (REST endpoints: POST /v1/payments, GET /{id})
└─────────────────────────────────────────────────────────────────┘
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│  Application/Service Layer (Business Logic)                     │
│  • PaymentOrchestratorService (orchestration)                   │
│  • IdempotencyService (Redis lock & cache)                      │
│  • RoutingEngine (provider selection)                           │
└─────────────────────────────────────────────────────────────────┘
         │                            │                    │
         ▼                            ▼                    ▼
    ┌────────┐          ┌──────────────────────┐     ┌──────────┐
    │  MySQL │          │  Redis Cache         │     │  Kafka   │
    │        │          │  (Idempotency TTL)   │     │  Topics  │
    │payments│          │  localhost:6379      │     │:9092     │
    │table   │          │  (24-hour TTL)       │     │          │
    │:3306   │          └──────────────────────┘     └──────────┘
    └────────┘                                              │
                                                           ▼
                                      ┌─────────────────────────────┐
                                      │  PaymentRetryConsumer       │
                                      │  • @RetryableTopic          │
                                      │  • Non-blocking retry       │
                                      │  • Exponential backoff      │
                                      │  • DLT handler              │
                                      └─────────────────────────────┘
                                              │
                                              ▼
                        ┌─────────────────────────────────┐
                        │  Provider Connectors            │
                        │  • ProviderAConnector (CARD)    │
                        │  • ProviderBConnector (UPI)     │
                        └─────────────────────────────────┘
```

### Synchronous REST Path (Happy Path, ~80% of payments)

When a client calls `POST /v1/payments` and the primary provider succeeds:

```
1. Client request
   ↓
2. IdempotencyFilter checks Redis for duplicate/in-flight key
   ↓
3. PaymentController validates header & body (@Valid)
   ↓
4. PaymentOrchestratorService:
   a. Acquires Redis in-flight lock
   b. Inserts payment row in MySQL 
   c. Calls primary provider connector (CARD→A, UPI→B)
   ↓
5. Provider returns transaction reference (success)
   ↓
6. Service updates MySQL: status=SUCCESS, provider_id, provider_reference_id
   ↓
7. Service caches response in Redis (24-hour TTL)
   ↓
8. Client receives HTTP 201 with PaymentResponse (status=SUCCESS)
```

**Total latency:** ~100-500ms (depends on provider HTTP call; no Kafka involved).

### Asynchronous Kafka Retry Path (Resilience Path, ~20% of payments)

When a client calls `POST /v1/payments` and the primary provider fails:

```
HTTP Request Phase:
─────────────────
1. Client request
   ↓
2-3. Idempotency + validation (same as happy path)
   ↓
4-5. Service acquires lock, inserts payment (status=INITIATED)
   ↓
6. Service calls provider → THROWS ProviderException (504/500 simulated failure)
   ↓
7. Service catches exception, updates MySQL (status=PROCESSING, retryCount=0)
   ↓
8. Service publishes PaymentEvent to Kafka topic (non-blocking)
   ↓
9. Service caches PROCESSING response in Redis
   ↓
10. Client receives HTTP 201 with PaymentResponse (status=PROCESSING) — request thread released

Async Kafka Retry Phase (separate thread):
───────────────────────────────────────────
11. PaymentRetryConsumer receives message on payment-main-topic
    ↓
12. Service routes to primary provider (CARD→A, UPI→B)
    ↓
13. Provider call:
    • SUCCESS → update MySQL (status=SUCCESS), commit Kafka offset, done
    • FAILURE → update MySQL (retryCount++), throw exception
             → Spring Kafka moves message to payment-main-topic-retry-0
             → Partition pauses for 2 seconds (non-blocking backoff)
    ↓
14. Exponential backoff: 2s → 4s → 8s between retry attempts
    ↓
15. After 4 total attempts (1 initial + 3 retries):
    • If any succeeds → status=SUCCESS, done
    • If all fail → message goes to payment-main-topic-dlt (Dead Letter Topic)
    ↓
16. DLT Handler fires:
    • Updates MySQL (status=FAILED)
    • Logs error for operational investigation
```

**Why two paths?**
- **Sync path** delivers feedback immediately for happy cases (fast, simple).
- **Async path** provides resilience: provider downtime doesn't block the HTTP response; retries happen in the background over minutes, not milliseconds.
- Client can poll `GET /v1/payments/{id}` anytime to check the current status.

### Key Infrastructure & Data Flow

| Component | Type | Role | Config |
|-----------|------|------|--------|
| **MySQL** | Persistent store | Single source of truth for payment state | `localhost:3306/payment_orchestrator` |
| **Redis** | Cache & Lock | Idempotency key storage + in-flight lock | `localhost:6379` (24-hour TTL) |
| **Kafka** | Message queue | Async retry pipeline | `localhost:9092` |
| **Primary Provider** | External service | CARD→ProviderA, UPI→ProviderB | Simulated with 20% failure rate |

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

## Infrastructure Setup (Localhost)

This Payment Orchestration System requires four backing services running locally: **Java 21**, **MySQL 8.x**, **Redis 7.x**, and **Apache Kafka 3.x**. Install and start them using Homebrew (macOS).

### Step 1: Install All Services via Homebrew

```bash
# Connect as root
mysql -u root
```

Inside the MySQL shell:
```sql
-- Create the database
CREATE DATABASE IF NOT EXISTS payment_orchestrator;

-- If your root user has no password set locally, run:
ALTER USER 'root'@'localhost' IDENTIFIED BY 'password';
FLUSH PRIVILEGES;

EXIT;
```
After setting pasword use cmd to connect to mysql and enter password.
```sql
mysql -u root -p
````

### Step 2: Start Each Service

Open four separate terminal windows or use terminal multiplexing (tmux/screen). Start each service:

```bash
# Terminal 1 — Start MySQL
brew services start mysql

# Terminal 2 — Start Redis
brew services start redis

# Terminal 3 — Start ZooKeeper (required by Kafka)
brew services start zookeeper

# Terminal 4 — Start Kafka broker
brew services start kafka
```

### Step 3: Verify All Services Are Running

In a new terminal, run these diagnostic checks:

```bash
# Check Java 21
java -version
# Expected: openjdk version "21.x.x" ...

# Check MySQL
mysql -u root -e "SELECT 1;"
# Expected: 1 (with no output errors)

# Check Redis
redis-cli ping
# Expected: PONG

# Check Kafka
kafka-topics --list --bootstrap-server localhost:9092
# Expected: (no error; topics list will be empty on first run)
```

If all four commands succeed, all backing services are alive and ready.

---

## Execution & Quick-Start Guide

### Build the Project

Navigate to the project root and compile using Maven:

```bash
cd /path/to/PaymentSystem

# Clean build with tests
mvn clean install -U

# Or skip tests for a faster build (tests require running infrastructure)
mvn clean install -U -DskipTests
```


To change credentials, edit `src/main/resources/application.yml` under `spring.datasource`.

### Start the Application

Choose one method:

**Method 1: Using Maven**
```bash
mvn spring-boot:run
```

You should see:
```
Started Main in X.XXX seconds (process running for X.XXX)
Application ready to accept requests
```

## Testing Idempotency & Payment Routing

### Test 1: Create a Fresh Payment (New Idempotency-Key)

```bash
curl -X POST http://localhost:8080/v1/payments \
  -H "Content-Type: application/json" \
  -H "Idempotency-Key: test-key-$(date +%s)" \
  -d '{
    "amount": 100.00,
    "currency": "USD",
    "paymentMethod": "CARD"
  }'
```

**Expected response (HTTP 201):**
```json
{
   "id": "550e8400-e29b-41d4-a716-446655440000",
   "idempotencyKey": "test-key-1234567890",
   "amount": 100.00,
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

**Note:** The `status` will be `SUCCESS` ~80% of the time (synchronous success), or `PROCESSING` ~20% of the time (triggering Kafka retry pipeline).

### Test 2: Idempotency Cache Hit (Same Key, Second Request)

Use the same `Idempotency-Key` from Test 1:

```bash
curl -X POST http://localhost:8080/v1/payments \
  -H "Content-Type: application/json" \
  -H "Idempotency-Key: test-key-1234567890" \
  -d '{
    "amount": 100.00,
    "currency": "USD",
    "paymentMethod": "CARD"
  }'
```

**Expected response (HTTP 201):**
- Same exact response as Test 1
- **No new payment created in MySQL**
- No provider call executed
- Response served from Redis cache (instant)

### Test 3: Verify Database Routing Entries

Connect to MySQL and inspect the payment records:

```bash
mysql -u root -ppassword payment_orchestrator

SELECT 
  id, 
  LEFT(idempotency_key, 20) AS idem_key, 
  amount, 
  currency,
  payment_method, 
  status, 
  provider_id,
  LEFT(provider_reference_id, 20) AS prov_ref,
  retry_count, 
  version, 
  created_at
FROM payments
ORDER BY created_at DESC
LIMIT 20;
```

**Key columns to verify:**
- **payment_method:** `CARD` or `UPI`
- **provider_id:** `PROVIDER_A` (primary for CARD) or `PROVIDER_B` (primary for UPI)
- **status:** `SUCCESS`, `PROCESSING`, or `FAILED`
- **retry_count:** Number of Kafka delivery attempts (0 if synchronous success, 1+ if retried)
- **version:** Incremented on each DB write (optimistic locking counter)


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

#### Response — `201 OK`

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
| **Duplicate key, completed** | Returns `201 OK` with the **exact same response** as the original — without touching the database or any provider |

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

## Provider Routing & Retry Strategy

### Primary Provider Selection

The `RoutingEngine` selects the primary provider based solely on payment method:

| Payment Method | Primary Provider | Details |
|---|---|---|
| `CARD` | Provider A | Used for all CARD payment attempts (sync + all Kafka retries) |
| `UPI` | Provider B | Used for all UPI payment attempts (sync + all Kafka retries) |


### Simulated Failure Rate

Both providers intentionally simulate a **20% failure rate** to exercise the retry and idempotency paths during local development. Failures are random.
This is controlled by `FAILURE_RATE = 0.20` in `AbstractSimulatedProviderConnector` (parent class of both providers).

### Retry Mechanism

When a provider fails:

1. **Sync attempt fails** (HTTP 504/500 thrown during `POST /v1/payments`)
   → Payment transitioned to `PROCESSING` status
   → Event published to Kafka
   → Client receives `HTTP 201 PROCESSING` immediately

2. **Kafka retry attempts** (up to 4 total deliveries)
   → Same primary provider is called again
   → Exponential backoff: 2s, 4s, 8s between attempts
   → Non-blocking: partition pauses, other messages proceed
   → If SUCCESS on any attempt: payment marked `SUCCESS`
   → If all 4 attempts fail: message lands on DLT, payment marked `FAILED`

**Timeline example for a failing CARD payment:**
```
T=0ms     — POST /v1/payments (sync attempt) → Provider A fails
T=0ms     — Kafka event published, HTTP 201 PROCESSING returned
T=2s      — Kafka attempt #2 on retry-0 topic → Provider A fails again
T=6s      — Kafka attempt #3 on retry-1 topic → Provider A fails again
T=14s     — Kafka attempt #4 on retry-2 topic → Provider A fails again
T=14s     — DLT handler fires → payment marked FAILED
```

All attempts target the same provider (ProviderA for CARD, ProviderB for UPI).

---

