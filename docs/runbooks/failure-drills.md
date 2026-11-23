# Failure drills

These exercises verify the recovery behaviour of the local Compose stack. Run
them against a disposable environment, not real data. They assume `make up` is
running and use `jq` to extract job IDs. Set the local environment first:

```bash
export BASE='http://localhost:8080'
export API_KEY='djp_VJXH5VdBqaXeYubgJ5xCR3rCcICc-VvLSfizIslPGns'
export TENANT_ID='2ea5964c-ad09-49fd-975d-fb48421b1cc4'
```

The values above are the local operator key and tenant from the development
stack. Use your own key and tenant when running elsewhere.

Useful defaults from `compose.yaml`:

- recovery interval: `30s`
- lease duration: `60s`
- heartbeat interval: `15s`
- tenant outstanding-job quota: `1000`
- general rate limit: `600` requests/minute
- submission rate limit: `120` requests/minute
- metrics token: `local-metrics-token`

## 1. API restart

```bash
docker compose stop api
curl -fsS http://localhost:8080/health/live || echo "API is down as expected"

docker compose start api
curl -fsS http://localhost:8080/health/ready
```

**Expected:** the API stops answering while down, then returns `{"status":"ready"}`.
Previously submitted jobs are unchanged. Prometheus reports `up == 0` for
`durable-job-api` during the outage and recovers afterwards.

## 2. Worker outage and backlog drain

```bash
docker compose stop worker

curl -sS -X POST http://localhost:8080/v1/jobs \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $API_KEY" \
  -H "Idempotency-Key: drill-worker-outage-1" \
  -d '{"job_type":"report.generate","payload":{"report_id":"drill-1","format":"pdf"}}' > /tmp/drill-job.json

JOB_ID="$(jq -r .id /tmp/drill-job.json)"
curl -sS "http://localhost:8080/v1/jobs/$JOB_ID" -H "Authorization: Bearer $API_KEY" | jq .status

docker compose start worker
sleep 5
curl -sS "http://localhost:8080/v1/jobs/$JOB_ID" -H "Authorization: Bearer $API_KEY" | jq .status
```

**Expected:** the job remains `queued` with the worker stopped, then moves to
`succeeded` after the worker returns. No duplicate execution is recorded.

## 3. Expired lease recovery

```bash
docker compose stop worker

curl -sS -X POST http://localhost:8080/v1/jobs \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $API_KEY" \
  -H "Idempotency-Key: drill-expired-lease-1" \
  -d '{"job_type":"report.generate","payload":{"report_id":"drill-lease","format":"pdf"}}' > /tmp/drill-lease.json

JOB_ID="$(jq -r .id /tmp/drill-lease.json)"

docker compose exec -T postgres psql -U jobs -d jobs -c "
  UPDATE jobs
  SET status = 'running',
      attempts = 1,
      locked_by = 'failure-drill',
      lease_token = gen_random_uuid(),
      locked_until = now() - interval '1 second'
  WHERE id = '$JOB_ID';
"

docker compose start worker
sleep 40
curl -sS "http://localhost:8080/v1/jobs/$JOB_ID" -H "Authorization: Bearer $API_KEY" | jq '{status, attempts}'
```

**Expected:** recovery detects `locked_until < now()`, requeues the job because
attempts remain, and the worker completes it. `worker_expired_leases_total{outcome="requeued"}`
increments.

## 4. Dead-letter and operator replay

Use the tenant UUID from the operator key used to create the stack. Insert a
terminal job directly so the drill does not depend on an external failure:

```bash
TENANT_ID="<tenant-uuid-from-apikey-list>"

DEAD_ID="$(docker compose exec -T postgres psql -U jobs -d jobs -t -A -c "
  INSERT INTO jobs (
    id, tenant_id, job_type, payload, status, attempts, max_attempts,
    last_error, finished_at
  )
  VALUES (
    gen_random_uuid(), '$TENANT_ID', 'report.generate',
    '{\"report_id\":\"drill-dead\"}'::jsonb,
    'dead', 5, 5, 'simulated permanent failure', now()
  )
  RETURNING id;
")"

curl -sS -X POST "http://localhost:8080/v1/jobs/$DEAD_ID/replay" \
  -H "Authorization: Bearer $API_KEY" | jq '{status, replay_count}'
```

**Expected:** the job returns to `retry_wait`, `attempts` resets to `0`,
`replay_count` becomes `1`, and `jobs_replayed_total` increments. Only an
operator key can perform this action.

## 5. Metrics authentication

```bash
curl -sS -o /dev/null -w '%{http_code}\n' http://localhost:9092/metrics
curl -sS -o /dev/null -w '%{http_code}\n' \
  -H 'Authorization: Bearer local-metrics-token' \
  http://localhost:9092/metrics
```

**Expected:** `401` without the token, `200` with it. The worker endpoint on
`127.0.0.1:9093` behaves the same way. Fatal listener startup behavior is
covered in [drill 11](#11-metrics-listener-fatal-startup).

## 6. Tenant quota enforcement

The default quota is 1000 outstanding (`queued`, `running`, `retry_wait`) jobs
per tenant. Stop the worker so the drill jobs stay outstanding, then fill the
tenant directly:

```bash
docker compose stop worker

docker compose exec -T postgres psql -U jobs -d jobs -c "
  INSERT INTO jobs (id, tenant_id, job_type, payload, status, run_at)
  SELECT gen_random_uuid(), '$TENANT_ID', 'report.generate',
         jsonb_build_object('report_id', 'drill-quota-' || n),
         'queued', now() + interval '1 day' * n
  FROM generate_series(1, 1000) n;
"

curl -sS -i -X POST "$BASE/v1/jobs" \
  -H 'Content-Type: application/json' \
  -H "Authorization: Bearer $API_KEY" \
  -H "Idempotency-Key: drill-quota-reject-1" \
  -d '{"job_type":"report.generate","payload":{"report_id":"drill-quota-reject"}}'
```

**Expected:** `429` with `{"error":{"code":"quota_exceeded",...}}`. An idempotent
repeat of a key that already exists still returns `200` because that job is
already counted. The quota check, lookup and insert share one transaction and a
tenant-scoped advisory lock, so concurrent submissions cannot overshoot.

## 7. Shared rate-limit behavior

The submission budget is 120 requests per minute per tenant. Stop the worker so
the drill jobs are not executed, then exhaust it:

```bash
docker compose stop worker

for i in $(seq 1 120); do
  curl -sS -o /dev/null -w '%{http_code}\n' -X POST "$BASE/v1/jobs" \
    -H 'Content-Type: application/json' \
    -H "Authorization: Bearer $API_KEY" \
    -H "Idempotency-Key: drill-rate-$i" \
    -d "{\"job_type\":\"report.generate\",\"payload\":{\"report_id\":\"drill-rate-$i\"}}"
done | sort | uniq -c

curl -sS -i -X POST "$BASE/v1/jobs" \
  -H 'Content-Type: application/json' \
  -H "Authorization: Bearer $API_KEY" \
  -H "Idempotency-Key: drill-rate-over" \
  -d '{"job_type":"report.generate","payload":{"report_id":"drill-rate-over"}}'
```

**Expected:** the loop prints `120 202`; the next request returns `429` with
`rate_limited` and a `Retry-After` header. Ordinary reads still succeed because
the general budget is independent.

To confirm the counter is shared rather than process-local, start a second API
process on port 8081 and repeat with the same key:

```bash
docker compose run -d --rm --name djp-rate-drill-replica \
  -p 8081:8080 -e METRICS_ENABLED=false api

until curl -fsS http://localhost:8081/health/ready >/dev/null; do sleep 1; done

curl -sS -o /dev/null -w '%{http_code}\n' -X POST http://localhost:8081/v1/jobs \
  -H 'Content-Type: application/json' \
  -H "Authorization: Bearer $API_KEY" \
  -H "Idempotency-Key: drill-rate-replica" \
  -d '{"job_type":"report.generate","payload":{"report_id":"drill-rate-replica"}}'

docker rm -f djp-rate-drill-replica >/dev/null
```

**Expected:** `429`. The second process reads the same `rate_limit_counters` row.
The counter resets at the next minute boundary. If the store is unavailable,
requests fail closed with `503 rate_limiter_unavailable` instead of bypassing
the budget.

## 8. Webhook SSRF rejection

Submission-time validation rejects non-public destinations before a row is
created:

```bash
for URL in \
  'http://127.0.0.1:8080/health/live' \
  'http://[::1]:8080/health/live' \
  'http://169.254.169.254/latest/meta-data/' \
  'http://10.0.0.5/hook'; do
  printf '%s -> ' "$URL"
  curl -sS -o /tmp/drill-ssrf.json -w '%{http_code}\n' -X POST "$BASE/v1/jobs" \
    -H 'Content-Type: application/json' \
    -H "Authorization: Bearer $API_KEY" \
    -H "Idempotency-Key: ssrf-$(uuidgen)" \
    -d "{\"job_type\":\"webhook.deliver\",\"payload\":{\"url\":\"$URL\"}}"
done
cat /tmp/drill-ssrf.json
```

**Expected:** every request returns `400 invalid_job` and the message names the
rejected address or host. Loopback, private, link-local, metadata-service and
other non-global addresses are blocked. DNS answers are also checked at dial
time, so rebinding cannot bypass the policy. Credentials in URLs and redirects
are rejected.

A public destination is accepted:

```bash
RESPONSE="$(curl -sS -X POST "$BASE/v1/jobs" \
  -H 'Content-Type: application/json' \
  -H "Authorization: Bearer $API_KEY" \
  -H "Idempotency-Key: drill-ssrf-public" \
  -d '{"job_type":"webhook.deliver","payload":{"url":"https://example.com/webhook"}}')"
echo "$RESPONSE" | jq '{id, status}'
```

**Expected:** `202` with a queued job. Delivery outcome is separate from
submission validation; a non-2xx response is recorded as a handler failure.

## 9. Cooperative cancellation of a running job

The built-in handlers finish too quickly to intercept, so simulate an in-flight
attempt while the worker is stopped:

```bash
docker compose stop worker

RESPONSE="$(curl -sS -X POST "$BASE/v1/jobs" \
  -H 'Content-Type: application/json' \
  -H "Authorization: Bearer $API_KEY" \
  -H "Idempotency-Key: drill-cancel-running-$(uuidgen)" \
  -d '{"job_type":"report.generate","payload":{"report_id":"drill-cancel-running"}}')"
JOB_ID="$(jq -r .id <<<"$RESPONSE")"

docker compose exec -T postgres psql -U jobs -d jobs -c "
  UPDATE jobs
  SET status = 'running', attempts = 1, locked_by = 'drill-runner',
      lease_token = gen_random_uuid(), locked_until = now() + interval '2 seconds',
      heartbeat_at = now(), started_at = now()
  WHERE id = '$JOB_ID';
"

curl -sS -i -X POST "$BASE/v1/jobs/$JOB_ID/cancel" \
  -H "Authorization: Bearer $API_KEY"

docker compose exec -T postgres psql -U jobs -d jobs -t -A -c \
  "SELECT cancel_requested_at IS NOT NULL FROM jobs WHERE id = '$JOB_ID';"

docker compose start worker
sleep 40
curl -sS "$BASE/v1/jobs/$JOB_ID" \
  -H "Authorization: Bearer $API_KEY" | jq '{status, cancel_requested_at, finished_at}'
```

**Expected:** the cancel response is `202` with `Cancel-Requested: true`; the SQL
check prints `t`; after recovery the job is `cancelled` with both timestamps
set and lease fields cleared. `worker_expired_leases_total{outcome="cancelled"}`
increments if the lease expires before the request is observed in a real
handler.

## 10. Stale-worker fencing

A worker that lost its lease must not overwrite the job recovered by another
worker. Capture the old lease token, let recovery and a new claim finish, then
replay the stale write:

```bash
docker compose stop worker

RESPONSE="$(curl -sS -X POST "$BASE/v1/jobs" \
  -H 'Content-Type: application/json' \
  -H "Authorization: Bearer $API_KEY" \
  -H "Idempotency-Key: drill-stale-fence-$(uuidgen)" \
  -d '{"job_type":"report.generate","payload":{"report_id":"drill-stale-fence"}}')"
JOB_ID="$(jq -r .id <<<"$RESPONSE")"

OLD_TOKEN="$(docker compose exec -T postgres psql -U jobs -d jobs -t -A -c "
  UPDATE jobs
  SET status = 'running', attempts = 1, locked_by = 'stale-worker',
      lease_token = gen_random_uuid(), locked_until = now() - interval '1 second',
      heartbeat_at = now(), started_at = now()
  WHERE id = '$JOB_ID'
  RETURNING lease_token;
")"

docker compose start worker
sleep 45
curl -sS "$BASE/v1/jobs/$JOB_ID" \
  -H "Authorization: Bearer $API_KEY" | jq '{status, attempts, lease_token}'

docker compose exec -T postgres psql -U jobs -d jobs -c "
  UPDATE jobs
  SET status = 'succeeded', last_error = 'stale worker write'
  WHERE id = '$JOB_ID'
    AND status = 'running'
    AND lease_token = '$OLD_TOKEN';
"
```

**Expected:** the job is `succeeded` with `attempts = 2` and `lease_token = null`
after the expired attempt is requeued and reclaimed. The final update reports
`UPDATE 0`: completion, failure and heartbeat writes all require the current
lease token, so the stale worker cannot mutate the terminal row.

## 11. Metrics listener fatal startup

The metrics listener must never fail silently. Occupy a host port, then start
the API binary directly so it tries to bind that same port:

```bash
docker compose stop api

python3 -m http.server 19090 --bind 127.0.0.1 >/tmp/drill-metrics-blocker.log 2>&1 &
BLOCKER_PID=$!
sleep 1

if [ -f .env ]; then set -a; . ./.env; set +a; else set -a; . ./.env.example; set +a; fi

METRICS_ADDR=127.0.0.1:19090 HTTP_ADDR=:18080 go run ./cmd/api
```

**Expected:** the process exits non-zero with an `api stopped` error containing
`metrics server: serve metrics` and `address already in use`. It does not keep
serving jobs while metrics are unavailable.

Release the port and confirm a clean start:

```bash
kill "$BLOCKER_PID"
timeout 5s env METRICS_ADDR=127.0.0.1:19090 HTTP_ADDR=:18080 go run ./cmd/api || true
```

**Expected:** logs show `api listening`; the `timeout` exit status is `124`, not
a startup error. The configuration guard is separate:

```bash
METRICS_ADDR=:19090 METRICS_TOKEN= HTTP_ADDR=:18080 go run ./cmd/api
```

**Expected:** startup fails with `METRICS_TOKEN is required when METRICS_ADDR is
not loopback`. Restore the stack with `docker compose start api`.

## Cleanup

```bash
docker compose restart api worker
docker compose exec -T postgres psql -U jobs -d jobs -c "
  DELETE FROM jobs WHERE payload->>'report_id' LIKE 'drill-%';
"
```

The cleanup removes drill jobs but not the `rate_limit_counters` rows; those
reset at the next minute boundary. Remove inactive counters with
`DELETE FROM rate_limit_counters WHERE updated_at < now() - interval '1 day';`
when needed.

Record the observed Prometheus graphs and alert states after each drill. If an
expected state does not appear within two recovery intervals, check the
structured logs for the worker ID and job ID before repeating the exercise.
