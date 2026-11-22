# Failure drills

These exercises verify the recovery behaviour of the local Compose stack. Run
them against a disposable environment, not real data. They assume `make up` is
running and an operator key is exported as `API_KEY`. The examples use `jq` to
extract job IDs.

Useful defaults from `compose.yaml`:

- recovery interval: `30s`
- lease duration: `60s`
- heartbeat interval: `15s`
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

## 5. Metrics authentication and fatal listener

```bash
curl -sS -o /dev/null -w '%{http_code}\n' http://localhost:9092/metrics
curl -sS -o /dev/null -w '%{http_code}\n' \
  -H 'Authorization: Bearer local-metrics-token' \
  http://localhost:9092/metrics
```

**Expected:** `401` without the token, `200` with it. The worker endpoint on
`127.0.0.1:9093` behaves the same way.

To verify fatal listener semantics, stop the API, occupy its host metrics port
with another process, and start the API again. It should exit with a
`metrics server` error instead of running unobserved. Release the port and the
API starts normally.

## Cleanup

```bash
docker compose restart api worker
docker compose exec -T postgres psql -U jobs -d jobs -c "
  DELETE FROM jobs WHERE payload->>'report_id' LIKE 'drill-%';
"
```

Record the observed Prometheus graphs and alert states after each drill. If an
expected state does not appear within two recovery intervals, check the
structured logs for the worker ID and job ID before repeating the exercise.
