-- Shared fixed-window rate-limit counters. Every API process updates the same
-- row for a (scope, tenant) pair, so a tenant budget is global instead of
-- per-process. Rows are keyed by scope and tenant rather than by request, so
-- the table grows with active tenants rather than with traffic.
CREATE TABLE rate_limit_counters (
    scope        text        NOT NULL,
    tenant_id    uuid        NOT NULL,
    window_start timestamptz NOT NULL,
    count        bigint      NOT NULL DEFAULT 1,
    updated_at   timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT rate_limit_counters_pkey PRIMARY KEY (scope, tenant_id),
    CONSTRAINT rate_limit_counters_count_valid CHECK (count > 0)
);

-- Supports manual cleanup of counters belonging to tenants no longer active.
CREATE INDEX rate_limit_counters_updated_idx ON rate_limit_counters (updated_at);
