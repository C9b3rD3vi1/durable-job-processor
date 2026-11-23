-- Authorization: attribute each job to the API key that submitted it.
--
-- NULL means a legacy or directly inserted job with no recorded creator. Such
-- jobs remain visible and cancellable to authenticated members of the tenant so
-- existing queues keep working. Client keys can only inspect and cancel jobs
-- they created; operator keys can act on any job in their tenant.
ALTER TABLE jobs
    ADD COLUMN created_by_api_key_id uuid;

CREATE INDEX jobs_tenant_owner_idx
    ON jobs (tenant_id, created_by_api_key_id);
