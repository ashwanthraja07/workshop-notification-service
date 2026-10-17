# Workshop Registration & Notifications

A resilient backend service demonstrating the **Transactional Outbox Pattern** with:
- **FastAPI** + **asyncpg** + **PostgreSQL 16**
- **At-least-once notification delivery** via a background dispatcher
- **Exponential backoff with jitter** & lease-based crash recovery
- A **Mock HTTP Notifier** with runtime-configurable failure and timeout modes
- Full non-happy-path isolation, idempotency key deduplication, and full-run verification

See [docs/DESIGN.md](docs/DESIGN.md) for architecture deep-dive and trade-off analysis.

---

## Prerequisites

| Tool | Version | Purpose |
|------|---------|---------|
| Docker | ≥ 24.0 | Containerized execution |
| Docker Compose | ≥ 2.20 | Multi-container orchestration |
| Python | ≥ 3.12 | Optional host runtime (for running demo & tests locally) |

---

## Quick Start (How to Run Everything)

### 1. Start All Services with Docker Compose
Run the entire stack (PostgreSQL 16, FastAPI backend with transactional outbox dispatcher, and Mock Notification service) in detached mode:

```bash
docker-compose up --build -d
```

### 2. Verify Services Are Healthy
Check container statuses and health:
```bash
docker-compose ps
```
All 3 containers should be running:
- `backend` (FastAPI + AsyncPG): [http://localhost:8000](http://localhost:8000) (Swagger Docs at [http://localhost:8000/docs](http://localhost:8000/docs))
- `mock_notifier` (Mock HTTP Service): [http://localhost:8001](http://localhost:8001)
- `postgres` (PostgreSQL 16): `localhost:5432`

### 3. View Service Logs
```bash
# Tail all logs
docker-compose logs -f

# Or tail specific services
docker-compose logs -f backend
docker-compose logs -f mock_notifier
```

---

## Running the End-to-End Demo

The demo script runs comprehensive scenarios demonstrating transactional integrity, retries, idempotency, timeout handling, crash recovery, and auditing:

```bash
# Install httpx (if not already installed)
pip install httpx

# Run the end-to-end demo
python demo/demo.py
```

### What the Demo Validates:
1. **Scenario 1 — Happy Path Registration & Delivery**: Creates a workshop, registers 3 members, verifies transactional outbox rows transition from `pending` to `sent`, and confirms delivery in the mock service.
2. **Scenario 2 — 100% Failure & Exponential Backoff Recovery**: Sets mock notifier failure rate to 100%, asserts outbox rows remain pending with `failed_attempts > 0`, restores notifier, and verifies automatic recovery.
3. **Scenario (Timeout) — Client Timeout & Idempotency Deduplication**: Simulates mock timeout (sleep 6s > backend 5s client timeout). Demonstrates backend retry, mock deduplicating by `Idempotency-Key`, and recording exactly 1 delivery.
4. **Scenario 3 — Cancellation Fan-Out & Idempotency**: Cancels a workshop with 5 registered members, emits 5 cancellation outbox rows, and proves re-cancelling is strictly idempotent.
5. **Scenario 4 — Real Crash Recovery (`docker-compose kill / up`)**: Registers members during outage, issues `docker-compose kill backend`, restarts the container, and verifies all pending rows are recovered and delivered.
6. **Scenario 5 — Full Run Audit & Non-Happy-Path Isolation**: Tests invalid workshop ID (404), cancelled workshop registration (409), and duplicate registration (409). Explains and verifies outbox row delta (+1 setup row only), and audits all sent outbox rows against mock deliveries.

---

## Running the Test Suite

### Option A: Run inside Docker (Recommended - Zero host dependencies)
```bash
docker-compose run --rm -e PYTHONPATH=/app -v "${PWD}/tests:/tests" backend pytest /tests -v
```

### Option B: Run locally on host
```bash
cd backend
pip install -r requirements.txt -r requirements-dev.txt
TEST_DATABASE_URL=postgresql://workshop:workshop@localhost:5432/workshop_db pytest ../tests/ -v
cd ..
```

---

## Stopping & Resetting the Environment

```bash
# Stop containers
docker-compose down

# Stop and wipe database volume for a clean slate
docker-compose down -v
```

---

## curl Examples

### Create a Workshop
```bash
curl -s -X POST http://localhost:8000/workshops \
  -H "Content-Type: application/json" \
  -d '{"title":"Intro to FastAPI","starts_at":"2030-01-15T10:00:00Z"}' | jq .
```

### Get a Workshop
```bash
curl -s http://localhost:8000/workshops/<workshop_id> | jq .
```

### Register a Member
```bash
curl -s -X POST http://localhost:8000/workshops/<workshop_id>/register \
  -H "Content-Type: application/json" \
  -d '{"member_id":"alice@example.com"}' | jq .
```

### Register Duplicate (409)
```bash
# Run the register command twice with the same member_id → second returns 409
```

### Cancel a Workshop
```bash
curl -s -X POST http://localhost:8000/workshops/<workshop_id>/cancel | jq .
```

### View Dead-Letter Outbox Rows
```bash
curl -s http://localhost:8000/admin/outbox/dead | jq .
```

### Retry a Dead Row
```bash
curl -s -X POST http://localhost:8000/admin/outbox/<outbox_id>/retry | jq .
```

### View All Outbox Rows
```bash
curl -s http://localhost:8000/admin/outbox/all | jq .
```

### Mock Notifier — Get Stats
```bash
curl -s http://localhost:8001/stats | jq .
```

### Mock Notifier — Set Failure Rate (100%)
```bash
curl -s -X POST http://localhost:8001/admin/config \
  -H "Content-Type: application/json" \
  -d '{"failure_rate": 1.0}' | jq .
```

### Mock Notifier — Take Down Fully
```bash
curl -s -X POST http://localhost:8001/admin/config \
  -H "Content-Type: application/json" \
  -d '{"fully_down": true}' | jq .
```

### Mock Notifier — Restore
```bash
curl -s -X POST http://localhost:8001/admin/config \
  -H "Content-Type: application/json" \
  -d '{"fully_down": false, "failure_rate": 0.0}' | jq .
```

### Mock Notifier — View Delivered Keys
```bash
curl -s http://localhost:8001/delivered | jq .
```

### Mock Notifier — Reset State
```bash
curl -s -X POST http://localhost:8001/admin/reset | jq .
```

---

## Environment Variables

### Backend

| Variable | Default | Description |
|----------|---------|-------------|
| `DATABASE_URL` | `postgresql://workshop:workshop@postgres:5432/workshop_db` | asyncpg DSN |
| `NOTIFIER_URL` | `http://mock_notifier:8001` | Base URL of notification service |
| `NOTIFIER_TIMEOUT` | `5.0` | HTTP timeout in seconds |
| `DISPATCHER_POLL_INTERVAL` | `2.0` | Seconds between dispatcher polls |
| `DISPATCHER_LEASE_SECONDS` | `30` | Lock lease duration (crash recovery window) |
| `DISPATCHER_CONCURRENCY` | `10` | Max concurrent HTTP sends per cycle |
| `DISPATCHER_BACKOFF_BASE` | `2.0` | Base delay for exponential backoff |
| `DISPATCHER_BACKOFF_MAX` | `300.0` | Maximum backoff delay |
| `DISPATCHER_JITTER_FACTOR` | `0.25` | Jitter fraction (±25% of computed delay) |

### Mock Notifier

| Variable | Default | Description |
|----------|---------|-------------|
| `FAILURE_RATE` | `0.0` | Fraction of requests returning 500 (0.0–1.0) |
| `TIMEOUT_RATE` | `0.0` | Fraction of requests that sleep past client timeout |
| `TIMEOUT_SLEEP` | `10.0` | Seconds to sleep on timeout simulation |
| `FULLY_DOWN` | `false` | Return 503 for all requests |

---

## Project Structure

```
.
├── docker-compose.yml
├── README.md
├── backend/
│   ├── Dockerfile
│   ├── requirements.txt
│   ├── requirements-dev.txt
│   ├── pytest.ini
│   └── app/
│       ├── main.py              # FastAPI app factory + lifespan
│       ├── config.py            # Pydantic Settings
│       ├── db/
│       │   ├── engine.py        # asyncpg pool factory
│       │   └── init.sql         # DDL (idempotent)
│       ├── models/
│       │   └── schemas.py       # Pydantic v2 schemas
│       ├── api/
│       │   ├── workshops.py     # Workshop & registration routes
│       │   └── admin.py         # Dead-letter admin routes
│       └── services/
│           ├── workshop_service.py     # Create, get, cancel
│           ├── registration_service.py # Register + outbox write
│           └── dispatcher.py           # Outbox background worker
├── mock_notifier/
│   ├── Dockerfile
│   ├── requirements.txt
│   └── app/
│       └── main.py              # Mock notifier FastAPI app
├── tests/
│   ├── conftest.py              # Fixtures
│   ├── test_registration.py     # Registration tests
│   ├── test_cancellation.py     # Cancellation tests
│   ├── test_dispatcher.py       # Dispatcher tests
│   └── test_workshops.py        # HTTP layer tests
├── demo/
│   └── demo.py                  # End-to-end demo script
└── docs/
    └── DESIGN.md                # Architecture & trade-offs
```