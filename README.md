# Durable Job Processor

A fault-tolerant background job processing system built with Go and PostgreSQL.

**Phase 2 is implemented:** bounded workers now claim queued jobs atomically,
execute typed handlers and record fenced lifecycle transitions. Phase 1 remains
the durable submission and retrieval layer. Heartbeats, expired-lease recovery,
richer retry classification and observability are planned for later phases.

## What works in Phase 2

- A separate `cmd/worker` binary shares the PostgreSQL store with the API.
- A handler registry maps each allow-listed job type to an executable handler;
  the API and worker therefore agree on what can be submitted and run.
- Atomic job claiming with `FOR UPDATE SKIP LOCKED`, priority/run-at ordering and
  a per-claim lease token.
- Lifecycle transitions from `queued`/`retry_wait` to `running`, then to
  `succeeded`, `retry_wait` or terminal `dead`.
- Bounded concurrency: `WORKER_CONCURRENCY` independent claim loops, each
  processing one job at a time.
- Per-handler timeouts, panic recovery and fenced completion/failure writes.
- Graceful shutdown stops claiming immediately, drains in-flight handlers for
  `SHUTDOWN_TIMEOUT`, then cancels their contexts.
- Built-in handlers for `report.generate`, `email.send`, `data.import` and a real
  HTTP `webhook.deliver`.
- Handler failures retry with `RETRY_BASE_DELAY` until `max_attempts` is
  exhausted, then dead-letter. Phase 5 will add failure classification and
  jittered exponential backoff.

## What works in Phase 1

- `POST /v1/jobs` — validate and persist a job transactionally, returning `202`.
- Idempotent submission via the `Idempotency-Key` header. A replay returns the
  existing job with `200` and `Idempotent-Replayed: true`.
- `GET /v1/jobs/{id}` — tenant-scoped lookup that survives process restarts.
- JSON-object payloads, allow-listed job types, priority bounds, per-job maximum
  attempts and payload-size limits.
- Versioned, embedded SQL migrations with an advisory lock and `schema_migrations`.
- PostgreSQL connection pooling, startup ping and graceful shutdown.
- `GET /health/live` and `GET /health/ready`.
- Structured request logging and panic recovery.

Job payloads are deliberately omitted from API responses to avoid exposing
sensitive data. Lease fields and idempotency keys are internal.

## Requirements

- Go 1.26+
- PostgreSQL 14+ (16 recommended)
- Docker/Podman Compose, or an existing PostgreSQL instance

## Quick start

```bash
# 1. Start PostgreSQL
docker compose up -d postgres

# 2. Load environment variables
cp .env.example .env
set -a; source .env; set +a

# 3. Apply migrations
go run ./cmd/migrate

# 4. Run the API
go run ./cmd/api
```

The API listens on `:8080` by default. In a second terminal with the same
environment loaded, start the worker:

```bash
go run ./cmd/worker
```

Alternatively, set `AUTO_MIGRATE=true` to apply migrations when the API or
worker starts. A separate migrate command is recommended for controlled
deployments.

## API examples

Submit a job:

```bash
TENANT_ID="$(uuidgen)"

curl -i -X POST http://localhost:8080/v1/jobs \
  -H "Content-Type: application/json" \
  -H "X-Tenant-ID: $TENANT_ID" \
  -H "Idempotency-Key: report-2026-11-15-001" \
  -d '{
    "job_type": "report.generate",
    "payload": {"report_id": "r-1", "format": "pdf"},
    "priority": 0,
    "max_attempts": 5
  }'
```

Expected response: `202 Accepted` with a `Location` header and a body such as:

```json
{
  "id": "4c0f7d6a-0f5f-4e2f-9a0d-0b26a8f1d4a8",
  "tenant_id": "4d6e1a0b-9b4f-4f04-af3c-0f95edc4e3f1",
  "job_type": "report.generate",
  "status": "queued",
  "priority": 0,
  "run_at": "2026-11-15T09:00:00Z",
  "attempts": 0,
  "max_attempts": 5,
  "created_at": "2026-11-15T09:00:00Z",
  "updated_at": "2026-11-15T09:00:00Z"
}
```

Retrieve it:

```bash
curl http://localhost:8080/v1/jobs/<job-id> \
  -H "X-Tenant-ID: $TENANT_ID"
```

Supported job types and their built-in handlers:

| Job type | Handler behaviour |
|---|---|
| `report.generate` | Simulates report generation and returns `report_id`, `format` and status |
| `email.send` | Simulates email delivery and validates a recipient address |
| `webhook.deliver` | POSTs the supplied JSON body to `payload.url` and requires a 2xx response |
| `data.import` | Simulates an import from `payload.source` |

The API rejects a job type that has no registered handler, so the allow-list and
the worker dispatch table cannot drift.

> **Identity note:** `X-Tenant-ID` is a Phase 1 development boundary. The
> architecture requires deriving tenancy from verified authentication claims.
> Replace this header with authentication in Phase 6; never trust tenant IDs from
> request payloads.

## Worker execution

A worker repeatedly claims one due job per claim loop:

```sql
SELECT id
FROM jobs
WHERE status IN ('queued', 'retry_wait')
  AND run_at <= now()
  AND attempts < max_attempts
ORDER BY priority DESC, run_at ASC, created_at ASC
FOR UPDATE SKIP LOCKED
LIMIT 1;
```

The claim increments `attempts`, records `locked_by`, `lease_token`,
`locked_until` and `started_at`, and returns the job while committing the claim.
No row lock is held while the handler runs. A successful handler writes
`succeeded` plus `result`; a failed handler writes `retry_wait` and a future
`run_at`, or `dead` once attempts are exhausted. Both writes require the current
lease token, so a worker that lost its lease cannot overwrite a newer attempt.

Observe a job after submitting it:

```bash
curl http://localhost:8080/v1/jobs/<job-id> \
  -H "X-Tenant-ID: $TENANT_ID"
```

The response moves from `queued` to `running` and then to `succeeded` or
`retry_wait`. Inspect the durable row directly for internal fields:

```bash
docker compose exec postgres psql -U jobs -d jobs \
  -c "SELECT id, status, attempts, locked_by, last_error FROM jobs ORDER BY created_at DESC LIMIT 10;"
```

Stop the worker with `Ctrl-C` or `SIGTERM`. It stops claiming immediately, lets
in-flight handlers finish for `SHUTDOWN_TIMEOUT`, then cancels their contexts and
records a retry for work that could not finish.

## Configuration

| Variable | Default | Purpose |
|---|---|---|
| `APP_ENV` | `development` | `development`/`local` uses text logs; others use JSON |
| `LOG_LEVEL` | `info` | `debug`, `info`, `warn`, `error` |
| `HTTP_ADDR` | `:8080` | Listen address; overrides `PORT` |
| `PORT` | `8080` | Used when `HTTP_ADDR` is empty |
| `DATABASE_URL` | required | PostgreSQL connection string |
| `DB_MAX_CONNS` | `10` | Maximum pool connections |
| `DB_MIN_CONNS` | `0` | Minimum pool connections |
| `DB_CONNECT_TIMEOUT` | `5s` | Connection/ping timeout |
| `AUTO_MIGRATE` | `false` | Apply migrations on API startup |
| `SHUTDOWN_TIMEOUT` | `15s` | Graceful shutdown budget for API and worker |
| `WORKER_CONCURRENCY` | `4` | Maximum jobs a worker executes simultaneously |
| `POLL_INTERVAL` | `1s` | Delay between claim attempts when no job is ready |
| `LEASE_DURATION` | `60s` | Claim lease recorded in `locked_until` |
| `HEARTBEAT_INTERVAL` | `15s` | Reserved for Phase 4 lease heartbeats |
| `HANDLER_TIMEOUT` | `30s` | Per-handler execution deadline |
| `RETRY_BASE_DELAY` | `5s` | Delay before a failed job becomes eligible again |
| `MAX_ATTEMPTS` | `5` | Default maximum attempts per job |
| `MAX_PAYLOAD_BYTES` | `262144` | Maximum JSON payload size |

`LEASE_DURATION` must be greater than `HEARTBEAT_INTERVAL`, and `HANDLER_TIMEOUT`
must not exceed `LEASE_DURATION`.

## Database migrations

Migrations live in `migrations/` and are embedded into both the API and migrate
binary. Files use `<version>_<description>.sql` naming. Applied versions are
tracked in `schema_migrations`, and an advisory lock prevents concurrent deploys
from racing.

```bash
go run ./cmd/migrate
```

`migrations/0001_create_jobs.sql` creates the documented `job_status` enum, the
`jobs` table, validation constraints and the ready/lease/tenant/idempotency
indexes.

## Tests

```bash
# Unit tests (no database required)
go test ./...

# Race detector
go test -race ./...

# PostgreSQL integration tests
TEST_DATABASE_URL='postgres://jobs:jobs@localhost:5432/jobs?sslmode=disable' \
  go test -v ./tests/integration
```

The integration suite verifies:

- jobs persist across a full API and connection-pool restart;
- concurrent-safe idempotency returns one durable row;
- tenant isolation returns `404` for another tenant;
- missing identity returns `401`;
- invalid payloads are rejected before persistence;
- migrations are idempotent;
- workers claim jobs atomically across concurrent claimers;
- priority/run-at ordering and skip rules are honoured;
- completions and failures are fenced by the lease token;
- handlers move jobs through success, retry and dead-letter states;
- worker concurrency stays bounded and graceful shutdown drains in flight work.

Phase 2 claim tests run in throwaway PostgreSQL schemas, so they do not depend on
or disturb development data. Integration tests are skipped when no
`TEST_DATABASE_URL` or `DATABASE_URL` is set.

## Project layout

```
cmd/api/                 API entrypoint and graceful shutdown
cmd/migrate/             Migration entrypoint
cmd/worker/              Worker entrypoint and graceful shutdown
internal/config/         Environment configuration and validation
internal/domain/         Job model, statuses, transitions, errors
internal/jobs/           Repository interface, validator and service
internal/postgres/       Pool, migrations, repositories, transactions
internal/handlers/       Handler registry and built-in job handlers
internal/queue/          Queue store contract used by the worker
internal/worker/         Claim loops, dispatch, timeout and shutdown runtime
internal/transport/http/ Router, handlers, DTOs and middleware
migrations/              Embedded versioned SQL migrations
tests/integration/       PostgreSQL-backed API and worker tests
docs/architecture/       Architecture blueprint and diagrams
```

## Roadmap

| Phase | Status |
|---|---|
| 1 — Job submission and persistence | Implemented |
| 2 — First worker | Implemented |
| 3 — Concurrency and bounded pool | Planned |
| 4 — Leases, heartbeat and recovery | Planned |
| 5 — Retry and dead-letter | Planned |
| 6 — Authentication and tenant hardening | Planned |
| 7 — Observability | Planned |
| 8 — Containers and CI | Planned |
| 9 — AWS and Terraform | Planned |
| 10 — Kubernetes lab | Planned |

The authoritative design is in
`docs/architecture/Durable_Job_Processor_Architecture_Design.docx`.
