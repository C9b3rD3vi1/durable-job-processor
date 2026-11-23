# Delivery roadmap

This document records delivery history and future work. Runtime behavior,
configuration and operating procedures are documented in the
[README](../README.md).

## Status

| Item | Status | Notes |
|---|---|---|
| Job submission and persistence | Delivered | Durable submission, retrieval, idempotency and migrations |
| Worker execution | Delivered | Typed handler registry, bounded concurrency and graceful shutdown |
| Leases, heartbeat and recovery | Delivered | Expiring leases, lease-token fencing and expired-lease recovery |
| Retry, dead-letter and replay | Delivered | Jittered backoff, dead-letter rows and audited operator replay |
| Authentication and tenant hardening | Delivered | API keys, owner/operator authorization, validation, listing, cancellation, quotas and separate rate limits |
| Observability | Delivered | Structured logs, health checks, Prometheus metrics, queue gauges, alerts, runbooks and dashboard queries |
| Containers and CI | Delivered | Multi-stage images, Compose stack, Makefile targets and GitHub Actions with PostgreSQL integration tests |
| AWS and Terraform | Planned | Infrastructure as code and cloud deployment are not present in this repository |
| Kubernetes lab | Planned | Kind/minikube manifests and drills are not present in this repository |

## Design reference

The authoritative architecture design is
[`Durable_Job_Processor_Architecture_Design.docx`](architecture/Durable_Job_Processor_Architecture_Design.docx).
The diagram source is available as
[`Durable_Job_Processor_Architecture_Mermaid.md`](architecture/Durable_Job_Processor_Architecture_Mermaid.md),
with rendered SVG and PNG files in the same directory.
