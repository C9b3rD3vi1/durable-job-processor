# Durable Job Processor — Mermaid architecture source

```mermaid
flowchart TB
  C[Client / Scheduler] -->|HTTPS| EDGE[DNS + TLS + Reverse Proxy]
  EDGE --> API[Go API Service]
  API -->|transactional create / query| DB[(PostgreSQL)]
  ADMIN[Admin / Operator] -->|health, metrics, controlled replay| API

  subgraph CORE[Durable Processing Core]
    API --> LIFE[Job Lifecycle API]
    LIFE -->|atomic state changes| DB
    CLAIM[Claim / Lease Coordinator] -->|FOR UPDATE SKIP LOCKED| DB
    CLAIM --> WORKERS[Bounded Go Worker Pool]
    WORKERS -->|heartbeat / complete / fail| DB
    REC[Recovery + Retry Loop] -->|expired lease recovery| DB
    REC -->|requeue or dead-letter| DB
    WORKERS --> HANDLERS[Typed Job Handlers]
    HANDLERS --> EXT[External APIs / Email / Webhooks]
  end

  WORKERS --> OBS[Prometheus + Structured Logs]
  API --> OBS
  REC --> OBS
  OBS --> GRAF[Grafana + Alerts]

  subgraph DELIVERY[Build and Delivery]
    SRC[Git Repository] --> CI[GitHub Actions]
    CI --> REG[Container Image Registry]
    TF[Terraform] --> INFRA[Network + Compute + IAM + Secrets]
    REG --> RUN[Docker Compose / AWS EC2 / kind or minikube]
    INFRA --> RUN
  end
  RUN --> API
  RUN --> WORKERS
  RUN --> DB
```

> Build delivery (`Git Repository → GitHub Actions → registry → Docker Compose`)
> is implemented in Phase 8. The Terraform, AWS EC2 and kind/minikube branches
> are planned for Phases 9 and 10 and are not present yet.
