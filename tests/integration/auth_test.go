package integration_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"testing"
	"time"

	"github.com/C9b3rD3vi1/durable-job-processor/internal/auth"
	"github.com/C9b3rD3vi1/durable-job-processor/internal/postgres"
	"github.com/C9b3rD3vi1/durable-job-processor/internal/transport/http/dto"
	"github.com/google/uuid"
)

func TestAPIKeyAuthenticationEndToEnd(t *testing.T) {
	databaseURL := testDatabaseURL(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	pool := mustIsolatedPool(t, databaseURL)
	tenantID := uuid.New()
	otherTenantID := uuid.New()

	server := newAPI(t, pool)
	token := createAPIKey(t, ctx, pool, tenantID, "client", auth.RoleClient)
	otherToken := createAPIKey(t, ctx, pool, otherTenantID, "other-client", auth.RoleClient)

	jobID := uuid.New()

	// No key and a malformed key are rejected before any tenant resolution.
	resp, body := doRequest(t, http.MethodGet, server.URL+"/v1/jobs/"+jobID.String(), "", "", "")
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("missing-key status = %d, want 401; body=%s", resp.StatusCode, body)
	}

	req, err := http.NewRequest(http.MethodGet, server.URL+"/v1/jobs/"+jobID.String(), nil)
	if err != nil {
		t.Fatalf("create request: %v", err)
	}
	req.Header.Set("Authorization", "Bearer not-a-real-key")
	resp, err = (&http.Client{Timeout: 5 * time.Second}).Do(req)
	if err != nil {
		t.Fatalf("perform request: %v", err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("malformed-key status = %d, want 401", resp.StatusCode)
	}

	// A valid key can submit, and the tenant comes from the key, not a header.
	submitBody := `{"job_type":"report.generate","payload":{"report_id":"r-1"}}`
	resp, body = doRequest(t, http.MethodPost, server.URL+"/v1/jobs", token, "auth-e2e", submitBody)
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("submit status = %d, want 202; body=%s", resp.StatusCode, body)
	}
	var created dto.JobResponse
	if err := json.Unmarshal(body, &created); err != nil {
		t.Fatalf("decode submitted job: %v", err)
	}
	if created.TenantID != tenantID {
		t.Fatalf("job tenant = %v, want key tenant %v", created.TenantID, tenantID)
	}

	// Another tenant's valid key cannot read the job.
	resp, body = doRequest(t, http.MethodGet, server.URL+"/v1/jobs/"+created.ID.String(), otherToken, "", "")
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("cross-tenant read status = %d, want 404; body=%s", resp.StatusCode, body)
	}

	// Revocation is immediate: the same token stops authenticating.
	if _, err := postgres.NewAPIKeyRepository(pool).RevokeAPIKey(ctx, tenantID, "client"); err != nil {
		t.Fatalf("revoke key: %v", err)
	}
	resp, body = doRequest(t, http.MethodGet, server.URL+"/v1/jobs/"+created.ID.String(), token, "", "")
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("revoked-key status = %d, want 401; body=%s", resp.StatusCode, body)
	}
}

func TestAPIKeyRepositoryStoresOnlyHash(t *testing.T) {
	databaseURL := testDatabaseURL(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	pool := mustIsolatedPool(t, databaseURL)
	tenantID := uuid.New()
	repository := postgres.NewAPIKeyRepository(pool)

	token, hash, err := auth.GenerateToken()
	if err != nil {
		t.Fatalf("GenerateToken() error = %v", err)
	}

	created, err := repository.CreateAPIKey(ctx, tenantID, "ops-frank", auth.RoleOperator, hash)
	if err != nil {
		t.Fatalf("CreateAPIKey() error = %v", err)
	}
	if created.TenantID != tenantID || created.Name != "ops-frank" || created.Role != auth.RoleOperator {
		t.Fatalf("created key = %+v, want tenant %v operator ops-frank", created, tenantID)
	}
	if !created.Active() {
		t.Fatal("new key is not active")
	}

	var (
		storedHash []byte
		hashLength int
	)
	if err := pool.QueryRow(ctx,
		`SELECT key_hash, octet_length(key_hash) FROM api_keys WHERE id = $1`,
		created.ID.String(),
	).Scan(&storedHash, &hashLength); err != nil {
		t.Fatalf("read stored key hash: %v", err)
	}
	if bytes.Equal(storedHash, []byte(token)) {
		t.Fatal("stored key hash equals the plaintext token")
	}
	if !bytes.Equal(storedHash, hash) {
		t.Fatal("stored key hash does not match the generated digest")
	}
	if hashLength != 32 {
		t.Fatalf("key_hash length = %d, want 32", hashLength)
	}

	found, err := repository.FindActiveAPIKeyByHash(ctx, hash)
	if err != nil {
		t.Fatalf("FindActiveAPIKeyByHash() error = %v", err)
	}
	if found.ID != created.ID {
		t.Errorf("found key = %v, want %v", found.ID, created.ID)
	}

	// Names are unique within a tenant.
	_, duplicateHash, err := auth.GenerateToken()
	if err != nil {
		t.Fatalf("GenerateToken() error = %v", err)
	}
	if _, err := repository.CreateAPIKey(ctx, tenantID, "ops-frank", auth.RoleOperator, duplicateHash); !errors.Is(err, auth.ErrAlreadyExists) {
		t.Fatalf("duplicate CreateAPIKey() error = %v, want ErrAlreadyExists", err)
	}

	revoked, err := repository.RevokeAPIKey(ctx, tenantID, "ops-frank")
	if err != nil {
		t.Fatalf("RevokeAPIKey() error = %v", err)
	}
	if revoked.RevokedAt == nil {
		t.Fatal("revoked_at is nil after revoke")
	}
	if _, err := repository.FindActiveAPIKeyByHash(ctx, hash); !errors.Is(err, auth.ErrInvalidCredential) {
		t.Fatalf("FindActiveAPIKeyByHash() after revoke error = %v, want ErrInvalidCredential", err)
	}

	again, err := repository.RevokeAPIKey(ctx, tenantID, "ops-frank")
	if err != nil {
		t.Fatalf("idempotent RevokeAPIKey() error = %v", err)
	}
	if again.RevokedAt == nil || !again.RevokedAt.Equal(*revoked.RevokedAt) {
		t.Errorf("second revoke changed revoked_at: %v then %v", revoked.RevokedAt, again.RevokedAt)
	}

	if _, err := repository.RevokeAPIKey(ctx, tenantID, "missing"); !errors.Is(err, auth.ErrNotFound) {
		t.Fatalf("revoke unknown key error = %v, want ErrNotFound", err)
	}

	keys, err := repository.ListAPIKeys(ctx, tenantID)
	if err != nil {
		t.Fatalf("ListAPIKeys() error = %v", err)
	}
	if len(keys) != 1 || keys[0].ID != created.ID || keys[0].Active() {
		t.Fatalf("listed keys = %+v, want one revoked key", keys)
	}
}
