# On-call runbook

Alert rules live in [`monitoring/prometheus/alerts.yml`](../../monitoring/prometheus/alerts.yml).
Thresholds are starting points; tune them after a baseline is available.
For proactive verification of the behaviours these alerts cover, run the
[failure drills](../runbooks/failure-drills.md).

## General triage

1. Identify the alert, `job`, `instance`, `route`, `job_type` or `outcome` labels.
2. Check whether the API and worker targets are up:
   `up{job=~"durable-job-(api|worker)"}`.
3. Check health endpoints on the affected API replica: `GET /health/live` and
   `GET /health/ready`. Readiness pings PostgreSQL with a two-second timeout.
4. Scrape the affected instance directly with the bearer token from
   [monitoring/README.md](README.md#prometheus-scrape-example) and inspect the
   relevant series.
5. Correlate with structured logs. Production logs are JSON; search for
   `level=ERROR` and the job ID, route or worker ID from the alert.
6. Check PostgreSQL connectivity, lock contention and connection counts before
   scaling workers. The database is the source of truth for claims, leases and
   backlog age.

Queue gauges refresh every 15 seconds and every worker publishes the same
PostgreSQL-derived values. Use `max(jobs_queue_depth)` and
`max(jobs_oldest_due_seconds)`; summing them multiplies the backlog by the
number of workers.

## DurableJobQueueBacklogHigh

**Meaning:** `max(jobs_queue_depth)` exceeded 1000 (default threshold) for 10
minutes. The metric counts due `queued` and `retry_wait` jobs with attempts
remaining and no pending cancellation.

**Check:**

```promql
max(jobs_queue_depth)
sum(worker_active_count)
up{job="durable-job-worker"}
max(db_pool_in_use / clamp_min(db_pool_max_conns, 1))
sum(rate(jobs_completed_total[5m]))
sum(rate(jobs_submitted_total{result="created"}[5m]))
```

```sql
SELECT id, tenant_id, job_type, status, priority, run_at, attempts, max_attempts
FROM jobs
WHERE status IN ('queued', 'retry_wait')
  AND run_at <= now()
  AND attempts < max_attempts
  AND cancel_requested_at IS NULL
ORDER BY priority DESC, run_at ASC, created_at ASC
LIMIT 20;
```

**Response:**

- If workers are down or not scraping, fix worker availability first.
- If active handlers are saturated, check downstream latency and
  `job_execution_duration_seconds`; scaling `WORKER_CONCURRENCY` or worker
  replicas helps only if PostgreSQL and the downstream dependency have capacity.
- If DB pool usage or waiters are high, follow
  [DurableJobDBPoolSaturated](#durablejobdbpoolsaturated).
- If submissions spiked, confirm it is expected and rate-limit the noisy tenant
  rather than starving other tenants.
- If jobs are stuck in `retry_wait` with far-future `run_at`, inspect recent
  failures and fix the cause before replaying or advancing jobs.

## DurableJobOldestDueAgeHigh

**Meaning:** the oldest due job has waited more than 15 minutes according to the
database clock.

**Check:** the same backlog and worker queries above, plus:

```promql
max(jobs_oldest_due_seconds)
histogram_quantile(0.95, sum by (le) (rate(job_wait_duration_seconds_bucket[15m])))
```

Inspect the oldest rows with `ORDER BY run_at ASC` from the backlog query. A
large `priority` value can starve lower-priority jobs; a single slow handler can
block one claim loop but not the whole worker.

**Response:** restore workers/DB capacity, then check whether priority ordering
or an upstream retry storm is causing starvation. Age falls naturally once jobs
are claimed; do not manually mutate `run_at` unless you have a specific incident
need and an audit trail.

## DurableJobDeadLetterSpike

**Meaning:** more than 10 jobs entered `dead` in 15 minutes.

**Check:**

```promql
sum by (job_type) (rate(jobs_dead_lettered_total[15m]))
sum by (job_type, class) (rate(jobs_failed_total[15m]))
```

```sql
SELECT id, tenant_id, job_type, attempts, finished_at, last_error
FROM jobs
WHERE status = 'dead'
  AND finished_at > now() - interval '30 minutes'
ORDER BY finished_at DESC
LIMIT 20;
```

`class` comes from the retry classifier (`permanent` failures dead-letter
immediately; transient failures exhaust `max_attempts` first).

**Response:** identify the failing `job_type` and root cause from `last_error`
and worker logs. Fix the dependency or handler bug before replaying. Replay is an
operator action: `POST /v1/jobs/{id}/replay` with an operator key. Replays reset
attempts, record audit columns and bypass the outstanding-job quota by design,
so avoid bulk replay while the root cause is active.

## DurableJobHeartbeatFailures

**Meaning:** lease renewals have failed at a sustained rate for five minutes.
The worker stops the handler if its lease is lost; jobs may be recovered by
another worker.

**Check:**

```promql
sum(rate(worker_heartbeat_failures_total[10m]))
sum(increase(worker_expired_leases_total[outcome="dead"][30m]))
max(db_pool_in_use / clamp_min(db_pool_max_conns, 1))
```

Review worker logs for `heartbeat` errors and check `LEASE_DURATION` and
`HEARTBEAT_INTERVAL`. The lease duration must be greater than the heartbeat
interval; defaults are 60s and 15s. Look for long garbage-collection pauses, CPU
starvation, network partitions or database latency.

**Response:** restore DB/network health. If a worker repeatedly loses its lease,
drain or restart it so recovery can finalize jobs cleanly. Do not raise the
heartbeat interval above the lease duration as a workaround.

## DurableJobExpiredLeaseTerminalOutcomes

**Meaning:** recovery dead-lettered or cancelled more than five expired leases
in 30 minutes. This usually follows heartbeat failures, worker crashes, pauses
or cooperative cancellation of long-running handlers.

**Check:**

```promql
sum by (outcome) (rate(worker_expired_leases_total[30m]))
sum(rate(worker_heartbeat_failures_total[10m]))
sum by (job_type) (rate(jobs_dead_lettered_total[30m]))
```

```sql
SELECT id, job_type, locked_by, locked_until, heartbeat_at, attempts, max_attempts
FROM jobs
WHERE status = 'running' AND locked_until < now()
ORDER BY locked_until
LIMIT 20;
```

**Response:** find the `locked_by` worker and its logs. If the worker is alive
but stalled, investigate handler hangs and timeouts. If it crashed, confirm
recovery requeued still-viable jobs and only terminal outcomes are elevated.
Replay dead jobs after the root cause is fixed; cancelled jobs require a new
submission or an explicit operator decision.

## DurableJobAPIHighErrorRate

**Meaning:** the API served more than 5% 5xx responses for five minutes.

**Check:**

```promql
sum by (route, status) (increase(api_http_requests_total{status=~"5.."}[15m]))
sum by (route) (rate(api_http_request_duration_seconds_count[5m]))
max(db_pool_in_use / clamp_min(db_pool_max_conns, 1))
```

Then inspect `GET /health/ready` on each API replica, structured `level=ERROR`
logs, and PostgreSQL `pg_stat_activity`. Note that `/metrics` is on a separate
listener and does not contribute to `api_http_requests_total`.

**Response:** roll back a recent deploy if correlated, remove a bad replica from
rotation, restore database/dependency health, or reduce load. Verify the ratio
returns below threshold before closing the incident.

## DurableJobMetricsUnavailable

**Meaning:** Prometheus has not scraped an API or worker target for five minutes.

**Check:**

- `up{job=~"durable-job-(api|worker)"}` and target labels in Prometheus.
- The process is running and `METRICS_ENABLED` is not `false`.
- `METRICS_ADDR` is reachable from Prometheus. A non-loopback bind requires
  `METRICS_TOKEN`; startup fails with a validation error otherwise.
- The bearer token file matches `METRICS_TOKEN` and has not rotated.
- Network policies, security groups and container port mappings allow the
  dedicated metrics port. API keys do not authorize `/metrics`.
- Manual scrape returns `200`; `401` means token mismatch, connection refused
  means listener/network trouble.

**Response:** restore the listener or token. If the process exited, check startup
logs first. This alert is critical because it also masks every other metric-based
alert for that instance.

## DurableJobRateLimitSpike

**Meaning:** more than 100 requests were rejected by tenant rate limits in 15
minutes.

**Check:**

```promql
sum by (scope) (increase(api_rate_limited_total[15m]))
```

`scope` distinguishes the general per-tenant budget from the submission budget.
Find affected tenants in API logs and check for a client retry loop or bulk
import.

**Response:** confirm expected load. If legitimate, adjust
`TENANT_RATE_LIMIT_PER_MINUTE` or `TENANT_SUBMIT_RATE_LIMIT_PER_MINUTE`
deliberately and record the change. If abusive, block or throttle the client
upstream. Both budgets are shared through PostgreSQL `rate_limit_counters`, so
the totals already include every API replica. A `rate_limiter_unavailable` spike
instead indicates the counter store is failing; check database connectivity and
pool saturation rather than changing the budgets.

## DurableJobDBPoolSaturated

**Meaning:** at least one process used more than 90% of its configured database
connections for 10 minutes.

**Check:**

```promql
max(db_pool_in_use / clamp_min(db_pool_max_conns, 1))
max(db_pool_idle)
sum(db_pool_wait_count)
```

`db_pool_wait_count` is a cumulative gauge, so do not wrap it in `rate()`.
`db_pool_max_conns` reflects `DB_MAX_CONNS` **per process**, so total possible
connections are replicas times that value. Inspect `pg_stat_activity` for idle
in transactions, long queries, lock waits and connection limits.

**Response:** fix long transactions or lock contention first. If capacity is the
real constraint, raise `DB_MAX_CONNS` only within PostgreSQL's connection budget
and account for every API and worker replica. Restart is not needed if the
configuration supports dynamic pool changes in your deployment.

## DurableJobJobWaitTimeHigh

**Meaning:** p95 time from job creation to execution exceeded five minutes for
10 minutes.

**Check:** combine the backlog, throughput, execution-latency and worker
availability queries from the earlier sections. Break down wait time by
`job_type` if needed. Long waits usually mean insufficient claim capacity,
far-future `run_at`/retry delays, or a priority-starved backlog.

**Response:** resolve the backlog or dependency issue; do not tune the histogram
buckets to hide the symptom. If retry backoff is intentionally long, adjust the
alert threshold to match the documented job SLA instead.
