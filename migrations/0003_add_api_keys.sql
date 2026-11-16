-- Phase 6 API authentication. Only the SHA-256 digest of an API key is stored,
-- so a database reader cannot recover a usable credential. Tenant and role are
-- derived from the authenticated key, never from a caller-supplied header.
CREATE TABLE api_keys (
    id         uuid PRIMARY KEY,
    tenant_id  uuid NOT NULL,
    name       text NOT NULL,
    role       text NOT NULL DEFAULT 'client',
    key_hash   bytea NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    revoked_at timestamptz,
    CONSTRAINT api_keys_role_valid CHECK (role IN ('client', 'operator')),
    CONSTRAINT api_keys_name_not_blank CHECK (btrim(name) <> ''),
    CONSTRAINT api_keys_tenant_name_uq UNIQUE (tenant_id, name),
    CONSTRAINT api_keys_key_hash_uq UNIQUE (key_hash),
    CONSTRAINT api_keys_hash_length_valid CHECK (octet_length(key_hash) = 32)
);

CREATE INDEX api_keys_tenant_created_idx ON api_keys (tenant_id, created_at DESC);
