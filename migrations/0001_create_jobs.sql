CREATE TYPE job_status AS ENUM (
    'queued',
    'running',
    'retry_wait',
    'succeeded',
    'dead',
    'cancelled'
);

CREATE TABLE jobs (
    id              uuid PRIMARY KEY,
    tenant_id       uuid NOT NULL,
    job_type        text NOT NULL,
    payload         jsonb NOT NULL,
    status          job_status NOT NULL DEFAULT 'queued',
    priority        smallint NOT NULL DEFAULT 0,
    run_at          timestamptz NOT NULL DEFAULT now(),
    attempts        integer NOT NULL DEFAULT 0,
    max_attempts    integer NOT NULL DEFAULT 5,
    idempotency_key text,
    locked_by       text,
    lease_token     uuid,
    locked_until    timestamptz,
    heartbeat_at    timestamptz,
    result          jsonb,
    last_error      text,
    created_at      timestamptz NOT NULL DEFAULT now(),
    updated_at      timestamptz NOT NULL DEFAULT now(),
    started_at      timestamptz,
    finished_at     timestamptz,
    CONSTRAINT jobs_attempts_valid CHECK (attempts >= 0),
    CONSTRAINT jobs_max_attempts_valid CHECK (max_attempts > 0),
    CONSTRAINT jobs_priority_valid CHECK (priority BETWEEN -10 AND 10)
);

CREATE UNIQUE INDEX jobs_tenant_idempotency_uq
    ON jobs (tenant_id, idempotency_key)
    WHERE idempotency_key IS NOT NULL;

CREATE INDEX jobs_ready_idx
    ON jobs (priority DESC, run_at ASC, created_at ASC)
    WHERE status IN ('queued', 'retry_wait');

CREATE INDEX jobs_expired_lease_idx
    ON jobs (locked_until)
    WHERE status = 'running';

CREATE INDEX jobs_tenant_created_idx
    ON jobs (tenant_id, created_at DESC);
