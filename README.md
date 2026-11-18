# Durable Job Processor

A fault-tolerant background job processing system built with Go and PostgreSQL.

**Phases 1–3 and Phase 6 tenant hardening are implemented:** durable submission
and retrieval, a worker that executes typed handlers with bounded concurrency,
the reliability layer (expiring leases, heartbeats, lease-token fencing,
expired-job recovery, jittered retries, dead-lettering and audited manual
replay), hashed API keys that derive tenant and role from verified identity,
owner-or-operator authorization for job inspection and cancellation,
submission-time payload schema validation, cursor-paginated job listing,
cooperative cancellation, per-tenant outstanding quotas and independent
in-memory budgets for general traffic and job submissions. Observability and the
delivery pipeline remain for later phases.

## What works in Phases 2–3

- A separate `cmd/worker` binary shares the PostgreSQL store with the API.
- A handler registry maps each allow-listed job type to an executable handler;
  the API and worker therefore agree on what can be submitted and run.
- Atomic job claiming with `FOR UPDATE SKIP LOCKED`, priority/run-at ordering and
  a per-claim lease token.
- Lifecycle transitions from `queued`/`retry_wait` to `running`, then to
  `succeeded`, `retry_wait` or terminal `dead`.
- Bounded concurrency: `WORKER_CONCURRENCY` independent claim loops, each
  processing one job at a time, so multiple workers and multiple processes can
  share the queue without duplicate active claims.
- Per-handler timeouts, panic recovery and fenced completion/failure writes.
- Graceful shutdown stops claiming immediately, drains in-flight handlers for
  `SHUTDOWN_TIMEOUT`, then cancels their contexts.
- Built-in handlers for `report.generate`, `email.send`, `data.import` and a real
  HTTP `webhook.deliver`.
- Expiring leases renewed by a per-job heartbeat. A worker that loses its lease
  is cancelled and cannot write completion or failure state.
- A background recovery loop requeues running jobs whose lease expired, or
  dead-letters them once attempts are exhausted.
- Failure classification through `retry.Transient`, `retry.Permanent` and
  `retry.Cancelled`. Unknown failures retry conservatively; webhook 4xx
  responses are permanent while 5xx, network and timeout failures are transient.
- Full-jitter exponential backoff capped by `RETRY_MAX_DELAY`, with
  `MAX_ATTEMPTS` as the hard retry limit.
- Dead-letter rows retain attempt count, timestamps and a sanitized error.
- `POST /v1/jobs/{id}/replay` returns an eligible dead job to `retry_wait`,
  resets attempts and records `replay_count`, `replayed_by` and `replayed_at`.

## What works in Phase 1

- `POST /v1/jobs` — validate and persist a job transactionally, returning `202`.
- Idempotent submission via the `Idempotency-Key` header. A replay returns the
  existing job with `200` and `Idempotent-Replayed: true`.
- `GET /v1/jobs/{id}` — tenant-scoped lookup that survives process restarts.
- JSON-object payloads validated against each job type's schema, allow-listed job
  types, priority bounds, per-job maximum attempts and payload-size limits.
- Versioned, embedded SQL migrations with an advisory lock and `schema_migrations`.
- PostgreSQL connection pooling, startup ping and graceful shutdown.
- `GET /health/live` and `GET /health/ready`.
- Structured request logging and panic recovery.

## What works in Phase 6 (tenant hardening)

- Bearer API keys stored only as SHA-256 digests in the `api_keys` table; the
  plaintext key is printed once at creation.
- The request tenant comes from the authenticated key, never from a header or
  payload. `X-Tenant-ID` is no longer accepted.
- Two roles: `client` submits jobs and inspects/cancels only the jobs it created;
  `operator` can inspect, cancel and replay any job in its tenant, including
  dead-letter jobs.
- Immediate revocation: a revoked key stops authenticating on the next request.
- `cmd/apikey` provisions, lists and revokes keys without exposing digests.
- Missing, malformed, unknown and revoked keys return `401`; a valid key without
  the operator role receives `403` on replay.
- Submission validates the payload against the registered handler's schema, so
  malformed jobs return `400 invalid_job` instead of dead-lettering later at
  execution time.
- Each job records the API key that created it. A different `client` key's
  read/cancel request returns `404`; jobs without a recorded creator (created
  before attribution or inserted directly) remain tenant-visible.
- `GET /v1/jobs` lists the authenticated tenant's jobs (only those created by the
  client key, all of them for an operator) with opaque keyset pagination ordered
  by `(created_at DESC, id DESC)` and filters for `status`, `job_type`,
  `created_after` and `created_before`.
- `POST /v1/jobs/{id}/cancel` cancels queued and retry-wait jobs immediately. A
  running job receives a durable cancellation request that its worker observes
  on the next heartbeat, cancels the handler context and fences the attempt as
  `cancelled`. Terminal jobs return `409`.
- A per-tenant outstanding-job quota counts `queued`, `running` and `retry_wait`
  rows. Submission is serialized per tenant in PostgreSQL, so concurrent
  requests cannot overshoot the quota; an idempotent replay is not a new job and
  does not consume quota.
- A per-tenant fixed-window request rate limit runs after authentication and
  returns `429` with `Retry-After` when a tenant exceeds its budget. Job
  submissions also carry a separate, tighter per-tenant budget so submission
  bursts cannot spend the general request allowance.

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

# 4. Provision an API key for a tenant (prints the key once)
TENANT_ID="$(uuidgen)"
go run ./cmd/apikey create -tenant "$TENANT_ID" -name local-dev -role operator
export API_KEY="djp_..."   # copy the key printed above

# 5. Run the API
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

## API authentication

Every `/v1` request must send the key as a bearer token:

```http
Authorization: Bearer djp_...
```

Keys are generated with 256 bits of entropy and stored as SHA-256 digests, so the
database cannot be used to recover a usable credential. Each key belongs to one
tenant and one role:

| Role | Permissions |
|---|---|
| `client` | Submit jobs, read jobs in its tenant |
| `operator` | Everything a client can do, plus replay dead jobs |

The tenant is derived from the authenticated key. A caller cannot select a
different tenant with a header or request field; cross-tenant reads and replays
return `404`.

Provision and manage keys with `DATABASE_URL` set:

```bash
# Create a client key. The plaintext key is shown once and cannot be retrieved.
go run ./cmd/apikey create -tenant "$TENANT_ID" -name reporting-client -role client

# Create an operator key for dead-letter replay.
go run ./cmd/apikey create -tenant "$TENANT_ID" -name on-call-operator -role operator

# List keys for a tenant. Digests are never printed.
go run ./cmd/apikey list -tenant "$TENANT_ID"

# Revoke a key immediately. Revoking an already revoked key is a no-op.
go run ./cmd/apikey revoke -tenant "$TENANT_ID" -name reporting-client
```

Key names are unique within a tenant. If a key is lost, revoke it and create a
replacement; there is no recovery path by design. Both the general and submission
rate limits are applied per tenant by the API instance, while outstanding-job
quotas are enforced durably in PostgreSQL and shared by every API replica.

## API examples

Submit a job:

```bash
curl -i -X POST http://localhost:8080/v1/jobs \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $API_KEY" \
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
  "replay_count": 0,
  "created_at": "2026-11-15T09:00:00Z",
  "updated_at": "2026-11-15T09:00:00Z"
}
```

Payloads that fail the registered handler's schema return `400` with error code
`invalid_job`, and no job is persisted.

Retrieve it:

```bash
curl http://localhost:8080/v1/jobs/<job-id> \
  -H "Authorization: Bearer $API_KEY"
```

List the tenant's jobs newest first. `limit` defaults to 50 and is capped at 100;
`next_cursor` is opaque and is passed back unchanged:

```bash
curl "http://localhost:8080/v1/jobs?status=dead&job_type=report.generate&limit=25" \
  -H "Authorization: Bearer $API_KEY"

# Follow the cursor returned by the previous page.
curl "http://localhost:8080/v1/jobs?limit=25&cursor=$NEXT_CURSOR" \
  -H "Authorization: Bearer $API_KEY"
```

Filters are optional: `status`, `job_type`, `created_after` and `created_before`
(RFC 3339 timestamps). The response is `{"jobs": [...], "next_cursor": "..."}`;
`next_cursor` is absent on the final page.

Cancel a job:

```bash
curl -i -X POST http://localhost:8080/v1/jobs/<job-id>/cancel \
  -H "Authorization: Bearer $API_KEY"
```

A queued or retry-wait job becomes `cancelled` immediately and the response is
`200`. A running job receives a durable cancellation request: the response is
`202 Accepted` with `Cancel-Requested: true`, the handler's context is cancelled
on the worker's next heartbeat, and the attempt is fenced to `cancelled`. A job
that already reached `succeeded`, `dead` or `cancelled` is not cancellable and
returns `409`. Cancelling an already cancelled job is idempotent.

Read and cancel requests follow an owner-or-operator policy: a `client` key can
act only on jobs it submitted, while an `operator` key can act on any job in its
tenant. Other client keys receive `404` so ownership cannot be probed.

When a tenant is at `TENANT_MAX_OUTSTANDING_JOBS`, submission returns `429` with
error code `quota_exceeded`. Completing, cancelling or dead-lettering a job frees
quota. When the general request budget or the tighter submission budget is
exhausted, the API returns `429` with error code `rate_limited` and a
`Retry-After` header.

Supported job types and their built-in handlers:

| Job type | Handler behaviour |
|---|---|
| `report.generate` | Simulates report generation and returns `report_id`, `format` and status |
| `email.send` | Simulates email delivery and validates a recipient address |
| `webhook.deliver` | POSTs the supplied JSON body to `payload.url` and requires a 2xx response |
| `data.import` | Simulates an import from `payload.source` |

The API rejects a job type that has no registered handler, so the allow-list and
the worker dispatch table cannot drift.

> **Identity note:** tenancy and authorization come from the verified API key.
> `X-Tenant-ID` is not accepted, and payload fields cannot select a tenant. Use
> the `client` role for submission/read clients and the `operator` role only for
> controlled dead-letter replay.

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
`locked_until`, `heartbeat_at` and `started_at`, and returns the job while
committing the claim. No row lock is held while the handler runs.

While a handler runs, the worker renews `locked_until` every
`HEARTBEAT_INTERVAL`. A zero-row heartbeat means another worker owns the job, so
the handler context is cancelled and its result is discarded. Success and failure
writes use the same lease-token condition, which fences a stale worker after one
expired lease has been recovered.

The heartbeat also reports a durable `cancel_requested_at` set by
`POST /v1/jobs/{id}/cancel`. The worker cancels the handler context immediately
and, after the handler returns, fences the attempt to `cancelled` with the lease
token instead of recording success or failure.

A failed handler is classified:

- transient, cancelled and unknown failures retry with
  `random(0, min(RETRY_BASE_DELAY * 2^(attempt-1), RETRY_MAX_DELAY))`;
- permanent failures and exhausted attempts become `dead` immediately;
- the sanitized error, attempt count and timestamps remain on the row.

A separate recovery loop runs every `RECOVERY_INTERVAL` inside each worker. It
finds `running` rows with `locked_until < now()`, takes them with
`FOR UPDATE SKIP LOCKED`, and returns them to `retry_wait`, dead-letters them
according to their attempt count, or finalizes a pending cancellation as
`cancelled`. The scan and update are one statement, so several workers can run
recovery concurrently without stealing the same row.

Observe a job after submitting it:

```bash
curl http://localhost:8080/v1/jobs/<job-id> \
  -H "Authorization: Bearer $API_KEY"
```

The response moves from `queued` to `running` and then to `succeeded` or
`retry_wait`. Replay a dead job:

```bash
curl -X POST http://localhost:8080/v1/jobs/<job-id>/replay \
  -H "Authorization: Bearer $OPERATOR_API_KEY"
```

Only `dead` jobs are eligible, and the key must have the `operator` role; a
`client` key receives `403`. A replay returns the job with `status=retry_wait`,
`attempts=0` and an incremented `replay_count`; the operator key name is stored
in `replayed_by`. Replaying any other status returns `409`, and another tenant's
job returns `404`.

Inspect the durable row directly for internal fields:

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
| `AUTO_MIGRATE` | `false` | Apply migrations on API or worker startup |
| `SHUTDOWN_TIMEOUT` | `15s` | Graceful shutdown budget for API and worker |
| `WORKER_CONCURRENCY` | `4` | Maximum jobs a worker executes simultaneously |
| `POLL_INTERVAL` | `1s` | Delay between claim attempts when no job is ready |
| `LEASE_DURATION` | `60s` | Claim lease recorded in `locked_until` |
| `HEARTBEAT_INTERVAL` | `15s` | Lease renewal cadence while a handler runs |
| `HANDLER_TIMEOUT` | `30s` | Per-handler execution deadline |
| `RETRY_BASE_DELAY` | `5s` | First-attempt backoff ceiling |
| `RETRY_MAX_DELAY` | `15m` | Maximum exponential backoff ceiling |
| `RECOVERY_INTERVAL` | `30s` | Background expired-lease scan cadence |
| `MAX_ATTEMPTS` | `5` | Default maximum attempts per job |
| `MAX_PAYLOAD_BYTES` | `262144` | Maximum JSON payload size |
| `TENANT_MAX_OUTSTANDING_JOBS` | `1000` | Queued, running and retry-wait jobs per tenant; `0` disables |
| `TENANT_RATE_LIMIT_PER_MINUTE` | `600` | Per-tenant API requests per fixed minute; `0` disables |
| `TENANT_SUBMIT_RATE_LIMIT_PER_MINUTE` | `120` | Per-tenant `POST /v1/jobs` requests per fixed minute, in addition to the general limit; `0` disables |

`LEASE_DURATION` must be greater than `HEARTBEAT_INTERVAL`. `HANDLER_TIMEOUT` may
exceed the lease because heartbeats keep the lease alive for the whole handler.

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
indexes. `migrations/0002_add_replay_audit.sql` adds the durable
`replay_count`, `replayed_by` and `replayed_at` audit columns.
`migrations/0003_add_api_keys.sql` adds the `api_keys` table with role checks,
per-tenant unique names, unique 32-byte SHA-256 digests and revocation
timestamps. `migrations/0004_add_cancellation.sql` adds `cancel_requested_at`
and the `(tenant_id, created_at DESC, id DESC)` index used by cursor pagination.

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
- missing, malformed, unknown and revoked API keys return `401`;
- client keys cannot replay dead jobs while operator keys can;
- `api_keys` stores only a 32-byte SHA-256 digest;
- invalid payloads are rejected before persistence;
- migrations are idempotent;
- workers claim jobs atomically across concurrent claimers;
- priority/run-at ordering and skip rules are honoured;
- completions and failures are fenced by the lease token;
- handlers move jobs through success, retry and dead-letter states;
- worker concurrency stays bounded and graceful shutdown drains in flight work;
- heartbeats keep a long-running handler's lease alive beyond its original
  expiry, while recovery leaves live leases alone;
- expired leases are requeued or dead-lettered according to attempts;
- permanent failures dead-letter immediately and transient failures schedule a
  jittered retry;
- replay moves a dead job back to `retry_wait`, resets attempts, records the
  audit columns and rejects non-dead or cross-tenant jobs;
- only operator keys can replay, and `replayed_by` records the operator key name;
- listing paginates with an opaque cursor, applies status/type/time filters and
  never leaks another tenant's jobs;
- queued cancellation is immediate, running cancellation is cooperative and
  lease-fenced, and terminal jobs return `409`;
- recovery finalizes a pending cancellation after a lease expires;
- outstanding quotas reject excess submissions atomically and idempotent
  replays bypass the quota;
- the API maps quota exhaustion to `429 quota_exceeded`.

Phase 2–3 claim and concurrency tests run in throwaway PostgreSQL schemas, so
they do not depend on or disturb development data. Integration tests are skipped
when no `TEST_DATABASE_URL` or `DATABASE_URL` is set.

## Project layout

```
cmd/api/                 API entrypoint and graceful shutdown
cmd/apikey/              API-key provisioning, listing and revocation CLI
cmd/migrate/             Migration entrypoint
cmd/worker/              Worker entrypoint and graceful shutdown
internal/auth/           API-key generation, hashing, roles and authentication
internal/config/         Environment configuration and validation
internal/domain/         Job model, statuses, transitions, errors
internal/jobs/           Repository interface, validator and service
internal/postgres/       Pool, migrations, repositories, transactions
internal/handlers/       Handler registry and built-in job handlers
internal/queue/          Queue store contract used by the worker
internal/retry/          Failure classification and jittered backoff policy
internal/worker/         Claim loops, heartbeats, recovery, dispatch and shutdown
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
| 3 — Concurrency and bounded pool | Implemented with Phase 2 |
| 4 — Leases, heartbeat and recovery | Implemented with Phase 3 |
| 5 — Retry, dead-letter and replay | Implemented with Phase 3 |
| 6 — Authentication and tenant hardening | Implemented: API keys, owner/operator authz, schema validation, listing, cancellation, quotas and separate rate limits |
| 7 — Observability | Planned |
| 8 — Containers and CI | Planned |
| 9 — AWS and Terraform | Planned |
| 10 — Kubernetes lab | Planned |

The authoritative design is in
`docs/architecture/Durable_Job_Processor_Architecture_Design.docx`.
