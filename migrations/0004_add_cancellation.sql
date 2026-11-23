-- Cancellation and listing support.
--
-- cancel_requested_at records a cooperative cancellation request for a running
-- job. The worker observes it on its next heartbeat, cancels the handler
-- context, and fences the attempt as cancelled. Queued and retry_wait jobs are
-- cancelled immediately instead.
ALTER TABLE jobs
    ADD COLUMN cancel_requested_at timestamptz;

-- Cursor pagination orders by (created_at DESC, id DESC) so the cursor is a
-- stable keyset even when several jobs share a timestamp.
CREATE INDEX jobs_tenant_created_id_idx
    ON jobs (tenant_id, created_at DESC, id DESC);
