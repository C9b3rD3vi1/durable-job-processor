# Durable Job Processor

A durable background job processing system built in Go with PostgreSQL as the
source of truth. The API accepts typed jobs, the worker executes them with
bounded concurrency, and PostgreSQL stores the queue, leases, attempts, results,
errors, replay history and API credentials.

This document is the technical reference for running, operating and using the
system. For design history and delivery status, see [`docs/ROADMAP.md`](docs/ROADMAP.md).

## Contents

- [System overview](#system-overview)
- [Quick start](#quick-start)
- [Access points](#access-points)
- [Step-by-step usage guide](#step-by-step-usage-guide)
- [Authentication and authorization](#authentication-and-authorization)
- [HTTP API](#http-api)
- [Job types](#job-types)
- [Job lifecycle and execution model](#job-lifecycle-and-execution-model)
- [Data model](#data-model)
- [Configuration](#configuration)
- [Observability](#observability)
- [Security boundaries](#security-boundaries)
- [Operations](#operations)
- [Testing](#testing)
- [Container delivery and CI](#container-delivery-and-ci)
- [Project layout](#project-layout)
- [Further documentation](#further-documentation)

## System overview

The repository builds four binaries from one codebase:

| Binary | Responsibility |
|---|---|
| `api` | Authenticated HTTP API for submitting, reading, listing, cancelling and replaying jobs |
| `worker` | Atomic claim loop, handler execution, heartbeats, retries, recovery and shutdown |
| `migrate` | Applies embedded, versioned PostgreSQL migrations |
| `apikey` | Creates, lists and revokes API keys |

The API and worker share the same PostgreSQL database and the same handler
registry. The registry is both the submit-time allow-list and the worker
dispatch table, so a job type accepted by the API is always executable by the
worker.

Request path:

```text
client
  |
  v
API router -> auth -> rate limits -> handler -> validator -> PostgreSQL
                                                        |
                                                        v
worker claim loops <- FOR UPDATE SKIP LOCKED <- queue rows
  |
  v
typed handler -> heartbeat/fencing -> result, retry or dead-letter
```

There is no browser-based job management UI. Grafana is provided for metrics
only; job operations are performed through the HTTP API.

Implemented capabilities include:

- durable submission and retrieval;
- idempotent submission and cursor-paginated listing;
- owner/operator authorization with tenant isolation;
- immediate and cooperative cancellation;
- bounded worker concurrency with atomic claims;
- expiring leases, heartbeats and lease-token fencing;
- full-jitter exponential backoff, dead-lettering and audited replay;
- per-tenant quotas and shared PostgreSQL-backed rate limits;
- SSRF-hardened webhook delivery;
- structured logs, health probes and Prometheus metrics;
- multi-stage container images, a Compose stack and CI.

Not included in this repository: a job-management web UI, job update/delete API,
AWS/Terraform deployment and a Kubernetes lab. See
[`docs/ROADMAP.md`](docs/ROADMAP.md).

## Quick start

Requirements:

- Go 1.26+
- PostgreSQL 14+ (16 recommended)
- Docker or Podman Compose, or an existing PostgreSQL instance
- `python3` for JSON inspection in the examples
- `jq` and `uuidgen` for the failure drills and some examples

Start the full local stack:

```bash
make up
```

`make up` runs `docker compose up --build` in the foreground. To run it detached
instead:

```bash
docker compose up --build -d
```

Compose starts PostgreSQL first, waits for its health check, applies migrations,
then starts one API, one worker, Prometheus and Grafana. The `apikey` Compose
service is a one-shot CLI and does not stay running; use
`docker compose run --rm apikey ...` for key commands.

Provision a tenant and an operator key:

```bash
TENANT_ID="$(uuidgen)"

docker compose run --rm apikey create \
  -tenant "$TENANT_ID" \
  -name local-dev \
  -role operator
```

Copy the printed `djp_...` key. It is shown once and cannot be recovered.

```bash
export API_KEY='djp_...'

curl -i -H "Authorization: Bearer $API_KEY" \
  http://localhost:8080/health/ready
```

Stop the stack with:

```bash
make down
```

Named Docker volumes retain local data unless removed explicitly.

To run the Go binaries directly instead of using Compose:

```bash
docker compose up -d postgres

cp .env.example .env
set -a; source .env; set +a

go run ./cmd/migrate

TENANT_ID="$(uuidgen)"
go run ./cmd/apikey create -tenant "$TENANT_ID" -name local-dev -role operator

go run ./cmd/api
```

In a second terminal with the same environment loaded:

```bash
go run ./cmd/worker
```

`AUTO_MIGRATE=true` applies migrations during API or worker startup, but a
separate `cmd/migrate` deployment is recommended for controlled rollouts.

## Access points

| Service | URL | Credentials |
|---|---|---|
| Job API | `http://localhost:8080` | Bearer API key |
| API metrics | `http://localhost:9092/metrics` | Bearer `local-metrics-token` |
| Worker metrics | `http://localhost:9093/metrics` | Bearer `local-metrics-token` |
| Prometheus | `http://localhost:9090` | none |
| Grafana | `http://localhost:3000` | `admin` / `admin` |
| PostgreSQL | `localhost:5432` | `jobs` / `jobs` |

Prometheus is configured to scrape both metrics listeners with
`local-metrics-token`. If you override `METRICS_TOKEN`, update
`monitoring/prometheus/prometheus.yml` to match.

## Step-by-step usage guide

This section walks through every supported local operation. The deeper
[HTTP API](#http-api) and [configuration](#configuration) sections document the
contracts and all available options.

### 1. Set the local environment

```bash
export BASE='http://localhost:8080'
: "${API_KEY:?export the operator key printed by apikey create}"
: "${TENANT_ID:?export the tenant UUID used with apikey create}"
```

Use the key and tenant from the [quick start](#quick-start). The operator key
can perform all job actions in its tenant. Keys are shown once and cannot be
recovered; never commit a real key to source control.

Health check:

```bash
curl -i "$BASE/health/ready"
```

### 2. Supported API actions

| Action | Method and route | Who can do it |
|---|---|---|
| Create job | `POST /v1/jobs` | client or operator |
| View one job | `GET /v1/jobs/{id}` | owner client or operator |
| List jobs | `GET /v1/jobs` | client or operator |
| Cancel job | `POST /v1/jobs/{id}/cancel` | owner client or operator |
| Replay dead job | `POST /v1/jobs/{id}/replay` | operator only |
| Liveness | `GET /health/live` | public |
| Readiness | `GET /health/ready` | public |

There is no `PUT`, `PATCH`, or `DELETE /v1/jobs/{id}` endpoint.

### 3. Create a job

#### `report.generate`

```bash
curl -i -X POST "$BASE/v1/jobs" \
  -H 'Content-Type: application/json' \
  -H "Authorization: Bearer $API_KEY" \
  -H "Idempotency-Key: report-$(uuidgen)" \
  -d '{
    "job_type": "report.generate",
    "payload": {
      "report_id": "r-1001",
      "format": "pdf"
    },
    "priority": 0,
    "max_attempts": 5
  }'
```

Successful response: `202 Accepted` with a `Location` header.

Capture the job ID:

```bash
RESPONSE="$(curl -sS -X POST "$BASE/v1/jobs" \
  -H 'Content-Type: application/json' \
  -H "Authorization: Bearer $API_KEY" \
  -H "Idempotency-Key: report-$(uuidgen)" \
  -d '{"job_type":"report.generate","payload":{"report_id":"r-1002","format":"pdf"}}')"

JOB_ID="$(python3 -c 'import json,sys; print(json.load(sys.stdin)["id"])' <<<"$RESPONSE")"

echo "$JOB_ID"
```

#### `email.send`

```bash
curl -i -X POST "$BASE/v1/jobs" \
  -H 'Content-Type: application/json' \
  -H "Authorization: Bearer $API_KEY" \
  -H "Idempotency-Key: email-$(uuidgen)" \
  -d '{
    "job_type": "email.send",
    "payload": {
      "to": "ops@example.com",
      "template": "welcome"
    }
  }'
```

#### `data.import`

```bash
curl -i -X POST "$BASE/v1/jobs" \
  -H 'Content-Type: application/json' \
  -H "Authorization: Bearer $API_KEY" \
  -H "Idempotency-Key: import-$(uuidgen)" \
  -d '{
    "job_type": "data.import",
    "payload": {
      "source": "s3://bucket/key",
      "format": "csv"
    }
  }'
```

#### `webhook.deliver`

Only public HTTP(S) destinations are allowed by default.

```bash
curl -i -X POST "$BASE/v1/jobs" \
  -H 'Content-Type: application/json' \
  -H "Authorization: Bearer $API_KEY" \
  -H "Idempotency-Key: webhook-$(uuidgen)" \
  -d '{
    "job_type": "webhook.deliver",
    "payload": {
      "url": "https://example.com/webhook",
      "body": {"event": "test"},
      "headers": {"x-webhook-event": "test"}
    }
  }'
```

A URL like `http://127.0.0.1:8080/...` is rejected.

#### Optional submit fields

```json
{
  "scheduled_at": "2026-11-24T10:00:00Z",
  "priority": 5,
  "max_attempts": 3
}
```

Rules:

- `priority`: `-10` to `10`
- `max_attempts`: `1` to `10`
- `Idempotency-Key`: max 255 chars
- `payload` must be a JSON object

An idempotent repeat returns `200` and this header:

```text
Idempotent-Replayed: true
```

### 4. View one job

```bash
curl -sS "$BASE/v1/jobs/$JOB_ID" \
  -H "Authorization: Bearer $API_KEY" \
  | python3 -m json.tool
```

You will see fields such as:

```json
{
  "id": "...",
  "status": "succeeded",
  "attempts": 1,
  "result": {
    "status": "generated"
  }
}
```

Possible statuses:

```text
queued
running
retry_wait
succeeded
dead
cancelled
```

### 5. List jobs

All jobs:

```bash
curl -sS "$BASE/v1/jobs" \
  -H "Authorization: Bearer $API_KEY" \
  | python3 -m json.tool
```

Filter by status and type:

```bash
curl -sS "$BASE/v1/jobs?status=succeeded&job_type=report.generate&limit=25" \
  -H "Authorization: Bearer $API_KEY" \
  | python3 -m json.tool
```

Follow the cursor:

```bash
NEXT_CURSOR='paste-next_cursor-here'

curl -sS "$BASE/v1/jobs?limit=25&cursor=$NEXT_CURSOR" \
  -H "Authorization: Bearer $API_KEY" \
  | python3 -m json.tool
```

Available filters:

```text
status
job_type
created_after
created_before
limit
cursor
```

`limit` defaults to `50`, maximum `100`.

### 6. Cancel a job

For a queued or retry-waiting job:

```bash
curl -i -X POST "$BASE/v1/jobs/$JOB_ID/cancel" \
  -H "Authorization: Bearer $API_KEY"
```

Expected:

- `200` — cancelled or already cancelled
- `202` — running job got a cooperative cancellation request
- `409` — already succeeded, dead, or otherwise not cancellable
- `404` — not found or not visible to your key

### 7. Replay a dead job

Requires an operator key.

```bash
curl -i -X POST "$BASE/v1/jobs/$JOB_ID/replay" \
  -H "Authorization: Bearer $API_KEY"
```

Expected:

- `200` — job moved to `retry_wait`, attempts reset, `replay_count` incremented
- `403` — client key used
- `404` — other tenant or unknown job
- `409` — job is not dead

### 8. Edit a job

There is no edit/update operation. Jobs are immutable after submission.

To “edit” a job:

1. Cancel it if it is still cancellable.
2. Submit a new job with the corrected payload.

```bash
curl -i -X POST "$BASE/v1/jobs/$JOB_ID/cancel" \
  -H "Authorization: Bearer $API_KEY"

curl -i -X POST "$BASE/v1/jobs" \
  -H 'Content-Type: application/json' \
  -H "Authorization: Bearer $API_KEY" \
  -H "Idempotency-Key: corrected-$(uuidgen)" \
  -d '{
    "job_type": "report.generate",
    "payload": {
      "report_id": "r-1002-corrected",
      "format": "pdf"
    }
  }'
```

### 9. Delete a job

There is no delete API. Completed and cancelled jobs remain as durable history.

For local lab cleanup only, you can delete directly in PostgreSQL:

```bash
docker compose exec postgres psql -U jobs -d jobs \
  -c "DELETE FROM jobs WHERE id='$JOB_ID';"
```

That bypasses the application and should not be treated as a supported product
operation.

### 10. Manage API keys

Create a new tenant:

```bash
TENANT_ID="$(uuidgen)"
```

Create a client key:

```bash
docker compose run --rm apikey create \
  -tenant "$TENANT_ID" \
  -name reporting-client \
  -role client
```

Create an operator key:

```bash
docker compose run --rm apikey create \
  -tenant "$TENANT_ID" \
  -name on-call-operator \
  -role operator
```

List keys:

```bash
docker compose run --rm apikey list -tenant "$TENANT_ID"
```

Revoke a key:

```bash
docker compose run --rm apikey revoke \
  -tenant "$TENANT_ID" \
  -name reporting-client
```

The plaintext key is shown only once at creation. If lost, revoke it and create
a new one.

### 11. View internal database state

```bash
docker compose exec postgres psql -U jobs -d jobs \
  -c "SELECT id, job_type, status, attempts, max_attempts, last_error FROM jobs ORDER BY created_at DESC LIMIT 20;"
```

### 12. Monitoring and logs

```bash
docker compose ps --all
docker compose logs -f api worker
```

Metrics:

```bash
curl -H 'Authorization: Bearer local-metrics-token' \
  http://localhost:9092/metrics

curl -H 'Authorization: Bearer local-metrics-token' \
  http://localhost:9093/metrics
```

Open:

- Prometheus: `http://localhost:9090`
- Grafana: `http://localhost:3000`, login `admin` / `admin`

The short version: `create`, `view`, `list`, `cancel` and `replay` are API
operations; `edit` and `delete` jobs are not supported. For those, cancel and
resubmit, or use raw PostgreSQL locally for cleanup.

## Authentication and authorization

Every `/v1` request requires:

```http
Authorization: Bearer djp_...
```

API keys contain 256 bits of entropy. Only their SHA-256 digests are stored in
`api_keys`; plaintext is printed once by `apikey create`. A lost key must be
revoked and replaced.

Each key belongs to one tenant and one role:

| Role | Permissions |
|---|---|
| `client` | Submit jobs; read and cancel jobs it created |
| `operator` | Submit jobs; read and cancel any job in its tenant; replay dead jobs |

The tenant and role are derived from the verified key. Caller-supplied tenant
headers such as `X-Tenant-ID` are ignored, and payload fields cannot select a
tenant. Cross-tenant reads and replays return `404` rather than revealing that a
job exists. Jobs without a recorded creator remain tenant-visible to
authenticated members of that tenant.

Manage keys with `DATABASE_URL` set. Inside Compose:

```bash
docker compose run --rm apikey create \
  -tenant "$TENANT_ID" -name reporting-client -role client

docker compose run --rm apikey create \
  -tenant "$TENANT_ID" -name on-call-operator -role operator

docker compose run --rm apikey list -tenant "$TENANT_ID"

docker compose run --rm apikey revoke \
  -tenant "$TENANT_ID" -name reporting-client
```

Key names are unique within a tenant. Revocation takes effect on the next
request. Missing, malformed, unknown and revoked keys return `401`; a valid key
without the operator role receives `403` on replay.

## HTTP API

All responses use `application/json`. Errors have a stable envelope:

```json
{
  "error": {
    "code": "invalid_job",
    "message": "payload for job_type \"report.generate\" is invalid: report.generate requires report_id"
  }
}
```

Common error codes:

| Code | HTTP status | Meaning |
|---|---|---|
| `authentication_required` | `401` | No bearer token was supplied |
| `invalid_api_key` | `401` | Token is unknown, malformed or revoked |
| `forbidden` | `403` | Caller lacks the required role |
| `job_not_found` | `404` | Job does not exist or is not visible to the caller |
| `not_found` | `404` | Route does not exist |
| `invalid_json` | `400` | Body is not one valid JSON object |
| `invalid_job` | `400` | Job type or payload failed validation |
| `invalid_job_id` | `400` | Path ID is not a valid UUID |
| `invalid_argument` | `400` | Query value or argument is out of range |
| `invalid_query` | `400` | Status, limit or timestamp filter is invalid |
| `request_canceled` | `408` | The request context was canceled |
| `request_too_large` | `413` | Request body exceeds the configured limit |
| `invalid_transition` | `409` | Job state does not allow the operation |
| `quota_exceeded` | `429` | Tenant outstanding-job quota reached |
| `rate_limited` | `429` | General or submission rate limit reached |
| `rate_limiter_unavailable` | `503` | Shared rate-limit store could not be reached; the request is denied |
| `internal_error` | `500` | Unexpected server error |

### `POST /v1/jobs`

Submits a job.

Headers:

- `Authorization: Bearer <api-key>` — required
- `Content-Type: application/json` — required
- `Idempotency-Key: <string>` — optional, max 255 characters

Request body:

```json
{
  "job_type": "report.generate",
  "payload": {"report_id": "r-1", "format": "pdf"},
  "scheduled_at": "2026-11-24T09:00:00Z",
  "priority": 0,
  "max_attempts": 5
}
```

| Field | Type | Required | Notes |
|---|---|---|---|
| `job_type` | string | yes | Must be an allow-listed registered job type |
| `payload` | JSON object | yes | Validated against the handler's schema |
| `scheduled_at` | RFC 3339 timestamp | no | Defaults to now; stored as `run_at` |
| `priority` | integer | no | `-10` to `10`, default `0` |
| `max_attempts` | integer | no | `1` to `10`, default from `MAX_ATTEMPTS` |

Responses:

- `202 Accepted` for a new job, with a `Location: /v1/jobs/{id}` header.
- `200 OK` with `Idempotent-Replayed: true` when the idempotency key already
  exists for the tenant. The original job is returned unchanged.
- `400` for invalid JSON, unsupported job type or invalid payload.
- `413` when the body exceeds the request limit.
- `429` with `quota_exceeded` or `rate_limited` when a limit is reached.

Example:

```bash
curl -i -X POST http://localhost:8080/v1/jobs \
  -H 'Content-Type: application/json' \
  -H "Authorization: Bearer $API_KEY" \
  -H "Idempotency-Key: report-2026-11-24-001" \
  -d '{
    "job_type": "report.generate",
    "payload": {"report_id": "r-1", "format": "pdf"},
    "priority": 0,
    "max_attempts": 5
  }'
```

Successful job response:

```json
{
  "id": "4c0f7d6a-0f5f-4e2f-9a0d-0b26a8f1d4a8",
  "tenant_id": "4d6e1a0b-9b4f-4f04-af3c-0f95edc4e3f1",
  "job_type": "report.generate",
  "status": "queued",
  "priority": 0,
  "run_at": "2026-11-24T09:00:00Z",
  "attempts": 0,
  "max_attempts": 5,
  "replay_count": 0,
  "created_at": "2026-11-24T09:00:00Z",
  "updated_at": "2026-11-24T09:00:00Z"
}
```

The public job response deliberately omits `payload`, `idempotency_key`, lease
fields (`locked_by`, `lease_token`, `locked_until`, `heartbeat_at`) and
`created_by_api_key_id`. It includes `result`, `last_error`, `replayed_at`,
`cancel_requested_at`, `started_at` and `finished_at` only when those fields are
set.

### `GET /v1/jobs/{id}`

Returns one tenant-scoped job. A `client` key can read only jobs it created; an
`operator` key can read any job in its tenant.

```bash
curl -sS "http://localhost:8080/v1/jobs/$JOB_ID" \
  -H "Authorization: Bearer $API_KEY" | python3 -m json.tool
```

Responses: `200` on success, `400 invalid_job_id` for a malformed UUID, `404`
when the job is absent or not visible.

### `GET /v1/jobs`

Lists jobs newest first with opaque keyset pagination.

Query parameters:

| Parameter | Notes |
|---|---|
| `status` | `queued`, `running`, `retry_wait`, `succeeded`, `dead` or `cancelled` |
| `job_type` | Exact registered job type |
| `created_after` | RFC 3339 timestamp |
| `created_before` | RFC 3339 timestamp |
| `limit` | `1` to `100`, default `50` |
| `cursor` | Opaque value from a previous `next_cursor`; URL-encode when sending |

```bash
curl -sS "http://localhost:8080/v1/jobs?status=dead&job_type=report.generate&limit=25" \
  -H "Authorization: Bearer $API_KEY" | python3 -m json.tool

curl -sS "http://localhost:8080/v1/jobs?limit=25&cursor=$NEXT_CURSOR" \
  -H "Authorization: Bearer $API_KEY" | python3 -m json.tool
```

The response is:

```json
{
  "jobs": [],
  "next_cursor": "optional-on-more-pages"
}
```

`next_cursor` is omitted on the final page. Client keys see only their own jobs;
operators see all jobs in their tenant.

### `POST /v1/jobs/{id}/cancel`

Requests cancellation.

```bash
curl -i -X POST "http://localhost:8080/v1/jobs/$JOB_ID/cancel" \
  -H "Authorization: Bearer $API_KEY"
```

Behavior:

- `queued` or `retry_wait` job: set to `cancelled` immediately, returns `200`.
- `running` job: returns `202 Accepted` with `Cancel-Requested: true`; the worker
  observes `cancel_requested_at` on its next heartbeat, cancels the handler
  context and fences the attempt to `cancelled`.
- Already `cancelled`: returns `200` idempotently.
- `succeeded`, `dead` or other non-cancellable state: returns `409`.
- Another client key's job or another tenant's job: returns `404`.

### `POST /v1/jobs/{id}/replay`

Returns a dead job to `retry_wait`. Requires the `operator` role.

```bash
curl -i -X POST "http://localhost:8080/v1/jobs/$JOB_ID/replay" \
  -H "Authorization: Bearer $OPERATOR_API_KEY"
```

A successful replay returns `200` with `status: "retry_wait"`, `attempts: 0`, an
incremented `replay_count`, and `replayed_by` / `replayed_at` recorded in the
durable row. A `client` key receives `403`; a non-dead job returns `409`; a job
from another tenant returns `404`.

### Health probes

Health endpoints are public and are not part of `/v1`.

| Route | Purpose |
|---|---|
| `GET /health/live` | Process is running; returns `{"status":"ok"}` |
| `GET /health/ready` | Database ping succeeds; returns `{"status":"ready"}` or `503` |

## Job types

The API rejects unknown job types before persistence.

| Job type | Required payload | Optional payload | Result |
|---|---|---|---|
| `report.generate` | `report_id` | `format` (default `pdf`) | `report_id`, `format`, `status: "generated"` |
| `email.send` | `to` containing `@` | `template` | `to`, `template`, `status: "sent"`, `deliverer: "simulator"` |
| `data.import` | `source` | `format` | `source`, `format`, `status: "imported"` |
| `webhook.deliver` | public HTTP(S) `url` | `body`, `headers` | `delivered: true`, `status_code` |

Examples:

```json
{"report_id": "r-1", "format": "pdf"}
```

```json
{"to": "ops@example.com", "template": "welcome"}
```

```json
{"source": "s3://bucket/key", "format": "csv"}
```

```json
{
  "url": "https://example.com/hook",
  "body": {"event": "job.completed"},
  "headers": {"x-webhook-event": "job.completed"}
}
```

`webhook.deliver` allows only these caller-supplied headers: `Accept`,
`User-Agent`, `X-Webhook-Event`, `X-Webhook-Delivery`, `X-Request-Id` and
`X-Idempotency-Key`. A 2xx response succeeds. 4xx responses are permanent
failures; 5xx, network errors and timeouts are transient failures.

## Job lifecycle and execution model

States:

```text
queued ──claim──> running ──success──> succeeded
   │                 │
   │                 ├──retryable failure──> retry_wait
   │                 ├──permanent/exhausted──> dead
   │                 └──cancellation──> cancelled
   │
   └──cancel──> cancelled

retry_wait ──claim──> running
dead ──operator replay──> retry_wait
```

`succeeded`, `dead` and `cancelled` are terminal. A replay is the only way out
of `dead`, and it is an explicit operator action.

### Claiming

Each worker starts `WORKER_CONCURRENCY` claim loops. Each loop claims and runs at
most one job at a time. Claims use:

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

The full eligibility predicate also excludes rows with a pending cooperative
cancellation (`AND cancel_requested_at IS NULL`). The claim increments
`attempts` and records `locked_by`, `lease_token`, `locked_until`,
`heartbeat_at` and `started_at` in one committed transaction. No row lock is
held while the handler runs.

### Leases, heartbeats and fencing

While a handler runs, the worker renews `locked_until` every
`HEARTBEAT_INTERVAL`. A zero-row heartbeat means another worker owns the job, so
the handler context is cancelled and its result is discarded.

Completion and failure writes require the same `lease_token` as the claim. A
stale worker therefore cannot overwrite a job that was recovered and claimed by
another worker.

### Retries and dead-lettering

Failures are classified as transient, permanent or cancelled. Unknown failures
retry conservatively. Retry delay is full jitter:

```text
random(0, min(RETRY_BASE_DELAY * 2^(attempt - 1), RETRY_MAX_DELAY))
```

Permanent failures and exhausted attempts move the job to `dead` immediately.
Dead rows retain the sanitized error, attempt count and timestamps until an
operator replays them.

### Recovery

Every `RECOVERY_INTERVAL`, each worker scans for `running` jobs with an expired
`locked_until`. The scan and update are one statement using `FOR UPDATE SKIP
LOCKED`, so concurrent workers do not steal the same row. Recovery:

- requeues eligible jobs to `retry_wait`;
- dead-letters jobs that exhausted attempts;
- finalizes a pending cancellation as `cancelled`.

### Shutdown

On `SIGTERM` or `SIGINT`, the worker stops claiming immediately and drains
in-flight handlers for `SHUTDOWN_TIMEOUT`. If that budget expires, handler
contexts are cancelled and the runner waits one final bounded grace period for
fenced persistence writes. Handlers that ignore cancellation are abandoned
rather than blocking process exit; recovery can reclaim their leases.

## Data model

PostgreSQL is the durable source of truth. Schema changes are embedded in the
binaries and applied by `cmd/migrate`.

### `jobs`

Important columns:

| Column group | Columns |
|---|---|
| Identity | `id`, `tenant_id`, `created_by_api_key_id` |
| Definition | `job_type`, `payload`, `priority`, `run_at`, `max_attempts`, `idempotency_key` |
| Execution | `status`, `attempts`, `locked_by`, `lease_token`, `locked_until`, `heartbeat_at`, `started_at`, `finished_at` |
| Outcome | `result`, `last_error`, `cancel_requested_at` |
| Replay audit | `replay_count`, `replayed_by`, `replayed_at` |
| Timestamps | `created_at`, `updated_at` |

Constraints and indexes enforce:

- one idempotency key per tenant;
- valid job status, attempts, priority and replay count;
- efficient ready-queue lookup by priority, run-at and creation time;
- efficient expired-lease recovery;
- tenant/owner listing and `(tenant_id, created_at DESC, id DESC)` cursor order.

### `api_keys`

`api_keys` stores `id`, `tenant_id`, `name`, `role`, a unique 32-byte `key_hash`,
`created_at` and `revoked_at`. Names are unique per tenant. Roles are constrained
to `client` and `operator`.

### `rate_limit_counters`

`rate_limit_counters` stores one shared fixed-window counter per `(scope,
tenant_id)` pair. The scopes are `request` for general API traffic and
`submission` for job creation. Each row records `window_start`, `count` and
`updated_at`.

Every API process updates the same row with an atomic PostgreSQL upsert, so a
tenant's budget is global rather than per-process. A counter resets when the
stored window start is older than the current minute. The counter is capped at
`limit + 1`, so rejected requests do not grow it without bound. Rows are keyed
by tenant and scope rather than by request, so table growth tracks active
tenants rather than traffic; the `updated_at` index supports manual cleanup of
inactive tenants.

### Migrations

| Migration | Purpose |
|---|---|
| `0001_create_jobs.sql` | Job status enum, jobs table, constraints and initial indexes |
| `0002_add_replay_audit.sql` | Replay count, actor and timestamp |
| `0003_add_api_keys.sql` | API key table, role and hash constraints |
| `0004_add_cancellation.sql` | Cancellation timestamp and listing index |
| `0005_add_job_owner.sql` | Creator attribution and tenant/owner index |
| `0006_create_rate_limit_counters.sql` | Shared rate-limit counter table and cleanup index |

Applied versions are tracked in `schema_migrations`. An advisory lock prevents
concurrent migration runs from racing.

Apply migrations explicitly with:

```bash
go run ./cmd/migrate
```

## Configuration

Configuration is read from environment variables. Invalid values fail at
startup.

| Variable | Default | Purpose |
|---|---|---|
| `APP_ENV` | `development` | `development`/`local` use text logs; others use JSON |
| `LOG_LEVEL` | `info` | `debug`, `info`, `warn`, `error` |
| `HTTP_ADDR` | `:8080` | API listen address; overrides `PORT` |
| `PORT` | `8080` | Used when `HTTP_ADDR` is empty |
| `DATABASE_URL` | required | PostgreSQL connection string |
| `DB_MAX_CONNS` | `10` | Maximum pool connections |
| `DB_MIN_CONNS` | `0` | Minimum pool connections |
| `DB_CONNECT_TIMEOUT` | `5s` | Connection and ping timeout |
| `AUTO_MIGRATE` | `false` | Apply migrations on API or worker startup |
| `SHUTDOWN_TIMEOUT` | `15s` | Worker drain budget; API shutdown budget |
| `WORKER_CONCURRENCY` | `4` | Maximum simultaneous handlers |
| `POLL_INTERVAL` | `1s` | Delay between claim attempts when no job is ready |
| `LEASE_DURATION` | `60s` | Claim lease stored in `locked_until` |
| `HEARTBEAT_INTERVAL` | `15s` | Lease renewal cadence |
| `HANDLER_TIMEOUT` | `30s` | Per-handler execution deadline |
| `RETRY_BASE_DELAY` | `5s` | First-attempt backoff ceiling |
| `RETRY_MAX_DELAY` | `15m` | Maximum backoff ceiling |
| `RECOVERY_INTERVAL` | `30s` | Expired-lease scan cadence |
| `MAX_ATTEMPTS` | `5` | Default maximum attempts per job |
| `MAX_PAYLOAD_BYTES` | `262144` | Maximum JSON payload size |
| `TENANT_MAX_OUTSTANDING_JOBS` | `1000` | `queued`/`running`/`retry_wait` jobs per tenant; `0` disables |
| `TENANT_RATE_LIMIT_PER_MINUTE` | `600` | Per-tenant API requests per fixed minute; `0` disables |
| `TENANT_SUBMIT_RATE_LIMIT_PER_MINUTE` | `120` | Additional per-tenant submit budget per fixed minute; `0` disables |
| `API_REPLICAS` | `1` | Declared API replica count; informational, does not change limiter behavior |
| `WEBHOOK_ALLOWED_HOSTS` | empty | Comma-separated destination allow-list; leading `.` permits subdomains |
| `WEBHOOK_ALLOW_PRIVATE_IPS` | `false` | Local/test escape hatch; rejected in production environments |
| `METRICS_ENABLED` | `true` | Serve Prometheus metrics on a dedicated listener |
| `METRICS_ADDR` | `127.0.0.1:9090` | Metrics listener address |
| `METRICS_TOKEN` | empty | Required when `METRICS_ADDR` is not loopback |

Validation rules:

- `LEASE_DURATION` must be greater than `HEARTBEAT_INTERVAL`.
- `HANDLER_TIMEOUT` may exceed the lease because heartbeats keep it alive.
- `MAX_ATTEMPTS` must be between `1` and `10`.
- `API_REPLICAS` must be at least `1`. Any positive value is accepted because
  both rate-limit budgets use shared PostgreSQL counters and remain correct
  across API replicas.
- `WEBHOOK_ALLOW_PRIVATE_IPS=true` is rejected when `APP_ENV` is `production`,
  `prod` or `live`.

The total HTTP body limit is `MAX_PAYLOAD_BYTES + 64 KiB` to allow for JSON
framing.

### Rate limiting

Authentication runs before rate limiting, so budgets are keyed by the tenant of
the verified API key and unknown keys never consume capacity. Two independent
fixed-minute budgets are enforced:

- `request` applies to all authenticated `/v1` routes;
- `submission` applies only to `POST /v1/jobs`, in addition to the general
  budget.

A submission checks the submission budget first, so an exhausted submit budget
does not consume general request capacity. Both counters live in PostgreSQL and
are updated atomically, so the limits apply to the whole deployment rather than
to individual API processes. A `0` budget disables that scope.

A rejected request returns `429 rate_limited` with a `Retry-After` header. If the
counter store cannot be reached, the API denies the request with `503
rate_limiter_unavailable` rather than silently allowing traffic past the budget.

## Observability

### Health

Use `/health/live` for liveness and `/health/ready` for dependency-aware
readiness. Readiness performs a bounded database ping.

### Metrics

Both binaries expose Prometheus text-format metrics at `GET /metrics` on a
dedicated listener, separate from the authenticated API router. The default
`127.0.0.1:9090` bind is private. A routable bind requires `METRICS_TOKEN`, and
startup fails without it. Give API and worker distinct ports when running them
on one host.

Available series include:

- `api_http_requests_total{method,route,status}` and
  `api_http_request_duration_seconds`;
- `api_rate_limited_total{scope}` and
  `api_rate_limiter_unavailable_total{scope}`;
- `jobs_submitted_total{job_type,result}`, `jobs_completed_total`,
  `jobs_failed_total{job_type,class}`, `jobs_retried_total`,
  `jobs_dead_lettered_total`, `jobs_replayed_total`, `jobs_cancelled_total`;
- `job_attempts_total`, `job_wait_duration_seconds`,
  `job_execution_duration_seconds`, `worker_active_count`;
- `worker_heartbeat_failures_total`, `worker_expired_leases_total{outcome}`;
- `jobs_queue_depth`, `jobs_oldest_due_seconds`;
- `db_pool_in_use`, `db_pool_idle`, `db_pool_wait_count`, `db_pool_max_conns`.

Metrics are process-local. Scrape every replica and keep `instance` in panel
legends. Queue gauges are computed from one shared PostgreSQL view but published
by every worker, so aggregate them with `max()`, not `sum()`.

Local verification:

```bash
curl -H 'Authorization: Bearer local-metrics-token' \
  http://localhost:9092/metrics

curl -H 'Authorization: Bearer local-metrics-token' \
  http://localhost:9093/metrics

curl -sS http://localhost:9090/api/v1/targets
```

Prometheus scrape configuration, alert rules, dashboard queries and the on-call
runbook are in [`docs/monitoring`](docs/monitoring/README.md). The Compose stack
loads `monitoring/prometheus/prometheus.yml`, alerts and a provisioned Grafana
dashboard from `monitoring/grafana/`.

## Security boundaries

- Tenant and role come only from the authenticated API key.
- API keys are stored as SHA-256 digests; plaintext is unrecoverable.
- Client keys are owner-scoped for reads and cancellation.
- Cross-tenant resources return `404`.
- Job payloads and internal lease fields are omitted from API responses.
- Request bodies are size-limited and decoded with unknown-field rejection.
- Payloads are validated against the registered handler schema before storage.
- Per-tenant outstanding quotas are enforced durably in PostgreSQL.
- Per-tenant request budgets are shared through PostgreSQL and remain accurate
  across API replicas.
- Webhook egress rejects private, loopback, link-local, metadata-service,
  multicast, reserved and documentation addresses. DNS answers are checked at
  dial time to prevent rebinding.
- Webhook redirects and URL credentials are rejected.
- Non-loopback metrics listeners require a bearer token.
- Production environments reject the private-IP webhook escape hatch.

## Operations

### API keys

```bash
docker compose run --rm apikey create \
  -tenant "$TENANT_ID" -name my-client -role client

docker compose run --rm apikey list -tenant "$TENANT_ID"

docker compose run --rm apikey revoke \
  -tenant "$TENANT_ID" -name my-client
```

### Submitting and inspecting work

```bash
export BASE='http://localhost:8080'

RESPONSE="$(curl -sS -X POST "$BASE/v1/jobs" \
  -H 'Content-Type: application/json' \
  -H "Authorization: Bearer $API_KEY" \
  -H "Idempotency-Key: report-$(uuidgen)" \
  -d '{"job_type":"report.generate","payload":{"report_id":"r-1"}}')"

JOB_ID="$(python3 -c 'import json,sys; print(json.load(sys.stdin)["id"])' <<<"$RESPONSE")"

curl -sS "$BASE/v1/jobs/$JOB_ID" \
  -H "Authorization: Bearer $API_KEY" | python3 -m json.tool
```

Jobs are immutable after submission. There is no update endpoint. To change
work, cancel the old job and submit a new one. There is no delete API; rows are
retained as durable history. For local cleanup only, use direct PostgreSQL
access with care.

### Database inspection

```bash
docker compose exec postgres psql -U jobs -d jobs \
  -c "SELECT id, status, attempts, locked_by, lease_token, last_error FROM jobs ORDER BY created_at DESC LIMIT 20;"
```

### Failure drills

Local procedures for API restart, worker outage, expired lease recovery,
dead-letter replay and metrics authentication are in
[`docs/runbooks/failure-drills.md`](docs/runbooks/failure-drills.md).

## Testing

```bash
# Unit tests; integration tests skip without a database URL
go test ./...

# Race detector
go test -race ./...

# PostgreSQL integration tests
TEST_DATABASE_URL='postgres://jobs:jobs@localhost:5432/jobs?sslmode=disable' \
  go test -race -count=1 ./tests/integration

# Full local quality gate
make check
```

The integration suite verifies persistence across restarts, concurrent
idempotency, tenant isolation, API-key authentication and revocation, replay
authorization, payload rejection, migration idempotency, atomic claims, queue
ordering, lease fencing, retry and dead-letter behavior, worker concurrency and
shutdown, heartbeats, recovery, cancellation, listing, quotas and queue gauges.

Integration tests use throwaway PostgreSQL schemas where appropriate. A missing
`TEST_DATABASE_URL` and `DATABASE_URL` skips only the database-backed tests;
unit tests still run and fail on regressions. CI starts a PostgreSQL service so
the full suite runs on every push and pull request.

## Container delivery and CI

The multi-stage `Dockerfile` builds one image per binary:

```text
ghcr.io/<owner>/durable-job-processor-api:latest
ghcr.io/<owner>/durable-job-processor-worker:latest
ghcr.io/<owner>/durable-job-processor-migrate:latest
ghcr.io/<owner>/durable-job-processor-apikey:latest
```

Repository names are lowercased for GHCR. Branch, full commit SHA and `latest`
tags are produced where applicable.

GitHub Actions:

- checks formatting and runs `go vet`;
- runs unit and race tests;
- runs PostgreSQL integration tests;
- validates the Compose configuration;
- builds all four images;
- publishes images to GHCR only on pushes to `main`;
- builds without pushing on pull requests.

Build locally with:

```bash
make build
make docker-build
```

## Project layout

```text
cmd/api/                 API entrypoint and graceful shutdown
cmd/apikey/              API-key provisioning, listing and revocation CLI
cmd/migrate/             Migration entrypoint
cmd/worker/              Worker entrypoint and graceful shutdown
internal/auth/           API-key generation, hashing, roles and authentication
internal/config/         Environment configuration and validation
internal/domain/         Job model, statuses, transitions and errors
internal/jobs/           Repository interface, validator and service
internal/postgres/       Pool, migrations, repositories and transactions
internal/handlers/       Handler registry and built-in job handlers
internal/queue/          Queue store contract used by the worker
internal/retry/          Failure classification and jittered backoff policy
internal/worker/         Claim loops, heartbeats, recovery, dispatch and shutdown
internal/transport/http/ Router, handlers, DTOs and middleware
migrations/              Embedded versioned SQL migrations
tests/integration/       PostgreSQL-backed API and worker tests
docs/architecture/       Architecture design and diagrams
docs/monitoring/         Scrape, dashboard, alert and runbook guidance
docs/runbooks/           Failure drills
monitoring/prometheus/   Prometheus scrape config and starter alert rules
monitoring/grafana/      Provisioned datasource and dashboard
.github/workflows/       CI pipeline
Dockerfile               Multi-stage images for api, worker, migrate and apikey
compose.yaml             Full local stack
Makefile                 Common build, test and stack targets
```

## Further documentation

- [Architecture design](docs/architecture/Durable_Job_Processor_Architecture_Design.docx)
- [Architecture diagram source](docs/architecture/Durable_Job_Processor_Architecture_Mermaid.md)
- [Monitoring guide](docs/monitoring/README.md)
- [Monitoring runbook](docs/monitoring/runbook.md)
- [Failure drills](docs/runbooks/failure-drills.md)
- [Delivery roadmap](docs/ROADMAP.md)

The Mermaid source is the authoritative architecture diagram. The DOCX, SVG and
PNG files are presentation copies generated from an earlier revision and may
lag the code; prefer the Mermaid source when checking current behavior. Its
Terraform, AWS EC2 and Kubernetes branches are design targets only, not
manifests shipped in this repository.
