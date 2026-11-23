# Monitoring

The API and worker expose a dependency-free Prometheus text endpoint on a
dedicated listener controlled by `METRICS_ENABLED`, `METRICS_ADDR` and
`METRICS_TOKEN`. It is **not** served by the authenticated API router and is not
protected by API keys.

## Security and network placement

- The default `METRICS_ADDR=127.0.0.1:9090` binds to loopback only. Keep it
  loopback and scrape from a local agent, sidecar or SSH tunnel where possible.
- If Prometheus must reach the listener from another host or container, set a
  token and bind a reachable address. Startup fails when a non-loopback address
  is configured without `METRICS_TOKEN`.
- Apply network policy/firewall rules so only Prometheus can reach the metrics
  port. Use TLS through a sidecar or reverse proxy when traffic crosses an
  untrusted network.
- Give API and worker different ports when they run on the same host.

## Prometheus scrape example

```yaml
scrape_configs:
  - job_name: durable-job-api
    metrics_path: /metrics
    scrape_interval: 15s
    static_configs:
      - targets: ["api-1:9090", "api-2:9090"]
    authorization:
      type: Bearer
      credentials_file: /etc/prometheus/secrets/durable_job_metrics_token

  - job_name: durable-job-worker
    metrics_path: /metrics
    scrape_interval: 15s
    static_configs:
      - targets: ["worker-1:9091", "worker-2:9091"]
    authorization:
      type: Bearer
      credentials_file: /etc/prometheus/secrets/durable_job_metrics_token
```

Omit the `authorization` block when `METRICS_TOKEN` is empty. The job names above
match the `DurableJobMetricsUnavailable` alert; adjust the expression if you use
different names.

The local Compose stack already loads
[`monitoring/prometheus/prometheus.yml`](../../monitoring/prometheus/prometheus.yml),
the alert rules and a provisioned Grafana dashboard. Its Prometheus config uses
the development token `local-metrics-token`; change it if you override
`METRICS_TOKEN`.

Verify a target by hand:

```bash
curl -fsS \
  -H "Authorization: Bearer $(cat /etc/prometheus/secrets/durable_job_metrics_token)" \
  http://worker-1:9091/metrics | head
```

A missing or wrong token returns `401`; a non-loopback bind without a token
prevents startup.

## Alert rules

Starter rules are in [`monitoring/prometheus/alerts.yml`](../../monitoring/prometheus/alerts.yml).
Thresholds are conservative examples; establish a baseline and tune them to
your traffic and SLOs. Alert annotations link to the [runbook](runbook.md).

## Dashboard queries

The provisioned dashboard in
[`monitoring/grafana/dashboards/durable-job-processor.json`](../../monitoring/grafana/dashboards/durable-job-processor.json)
contains exactly these panels:

| Panel | Prometheus expression |
|---|---|
| Queue depth | `max(jobs_queue_depth)` |
| Oldest due job age | `max(jobs_oldest_due_seconds)` |
| Job throughput | `sum(rate(jobs_submitted_total{result="created"}[5m]))` and `sum(rate(jobs_completed_total[5m]))` |
| Failures | `sum(rate(jobs_failed_total[5m]))` and `sum(rate(jobs_dead_lettered_total[5m]))` |
| API requests by status | `sum by (status) (rate(api_http_requests_total[5m]))` |
| Database pool usage | `max(db_pool_in_use / clamp_min(db_pool_max_conns, 1))` |
| Job wait time | `histogram_quantile(0.95, sum by (le) (rate(job_wait_duration_seconds_bucket[15m])))` |
| Active handlers | `sum(worker_active_count)` |

Metrics are process-local, so scrape every replica and aggregate explicitly.
Queue depth and age gauges are computed from one shared PostgreSQL view but
published by every worker; use `max()` for them, **not** `sum()`, or the backlog
is multiplied by the worker count. Queue gauges refresh every 15 seconds. Keep
`instance` in panel legends when debugging a single replica.

Additional queries are useful for ad-hoc diagnosis but are not provisioned as
dashboard panels:

```promql
sum by (job_type, class) (rate(jobs_failed_total[5m]))
sum(increase(jobs_cancelled_total[15m]))
sum by (scope) (rate(api_rate_limited_total[5m]))
sum by (scope) (rate(api_rate_limiter_unavailable_total[5m]))
histogram_quantile(0.95, sum by (le) (rate(job_execution_duration_seconds_bucket[5m])))
histogram_quantile(0.95, sum by (le) (rate(api_http_request_duration_seconds_bucket[5m])))
sum(db_pool_wait_count)
sum(rate(api_http_requests_total{status=~"5.."}[5m]))
  / clamp_min(sum(rate(api_http_requests_total[5m])), 0.001)
absent(up{job="durable-job-api"})
absent(up{job="durable-job-worker"})
```

Useful Grafana variables for user-created dashboards:

- `job`: `label_values(up{job=~"durable-job-.*"}, job)`
- `instance`: `label_values(up{job="$job"}, instance)`
- `job_type`: `label_values(jobs_submitted_total, job_type)`
- `route`: `label_values(api_http_requests_total, route)`

The shipped dashboard provisions no template variables; add these locally when
customising it.

## Alert coverage

The starter rules include `up == 0` checks for unreachable targets and separate
`absent()` rules for missing API and worker scrape targets. A target can vanish
from discovery entirely, in which case `up == 0` produces no series; the
`absent()` rules cover that case. See [`runbook.md`](runbook.md) for responses.
