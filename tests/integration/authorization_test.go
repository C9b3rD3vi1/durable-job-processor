package integration_test

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/C9b3rD3vi1/durable-job-processor/internal/auth"
	"github.com/C9b3rD3vi1/durable-job-processor/internal/postgres"
	"github.com/C9b3rD3vi1/durable-job-processor/internal/transport/http/dto"
	"github.com/google/uuid"
)

func TestOwnerOrOperatorAuthorization(t *testing.T) {
	databaseURL := testDatabaseURL(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	pool := mustIsolatedPool(t, databaseURL)
	tenantID := uuid.New()
	server := newAPI(t, pool)

	tokenA := createAPIKey(t, ctx, pool, tenantID, "client-a", auth.RoleClient)
	tokenB := createAPIKey(t, ctx, pool, tenantID, "client-b", auth.RoleClient)
	operatorToken := createAPIKey(t, ctx, pool, tenantID, "operator", auth.RoleOperator)

	keys, err := postgres.NewAPIKeyRepository(pool).ListAPIKeys(ctx, tenantID)
	if err != nil {
		t.Fatalf("list API keys: %v", err)
	}
	var keyAID uuid.UUID
	for _, key := range keys {
		if key.Name == "client-a" {
			keyAID = key.ID
		}
	}
	if keyAID == uuid.Nil {
		t.Fatal("could not resolve client-a API key ID")
	}

	body := `{"job_type":"report.generate","payload":{"report_id":"r-1"}}`
	resp, raw := doRequest(t, http.MethodPost, server.URL+"/v1/jobs", tokenA, "owner-job", body)
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("submit status = %d, want 202; body=%s", resp.StatusCode, raw)
	}
	var created dto.JobResponse
	if err := json.Unmarshal(raw, &created); err != nil {
		t.Fatalf("decode submit response: %v", err)
	}

	var storedOwner uuid.UUID
	if err := pool.QueryRow(ctx,
		`SELECT created_by_api_key_id FROM jobs WHERE id = $1`,
		created.ID.String(),
	).Scan(&storedOwner); err != nil {
		t.Fatalf("query job owner: %v", err)
	}
	if storedOwner != keyAID {
		t.Fatalf("created_by_api_key_id = %v, want %v", storedOwner, keyAID)
	}

	// A different client key in the same tenant cannot inspect or cancel it.
	for _, tc := range []struct {
		method string
		path   string
	}{
		{http.MethodGet, "/v1/jobs/" + created.ID.String()},
		{http.MethodPost, "/v1/jobs/" + created.ID.String() + "/cancel"},
	} {
		resp, raw = doRequest(t, tc.method, server.URL+tc.path, tokenB, "", "")
		if resp.StatusCode != http.StatusNotFound {
			t.Errorf("%s as other client status = %d, want 404; body=%s", tc.method, resp.StatusCode, raw)
		}
	}

	// The owner and a tenant operator can inspect it.
	for _, token := range []string{tokenA, operatorToken} {
		resp, raw = doRequest(t, http.MethodGet, server.URL+"/v1/jobs/"+created.ID.String(), token, "", "")
		if resp.StatusCode != http.StatusOK {
			t.Errorf("GET as authorized caller status = %d, want 200; body=%s", resp.StatusCode, raw)
		}
	}

	// Listings are owner-scoped for clients and tenant-wide for operators.
	resp, raw = doRequest(t, http.MethodGet, server.URL+"/v1/jobs", tokenB, "", "")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("other client list status = %d, want 200; body=%s", resp.StatusCode, raw)
	}
	var otherPage dto.ListJobsResponse
	if err := json.Unmarshal(raw, &otherPage); err != nil {
		t.Fatalf("decode other client list: %v", err)
	}
	if len(otherPage.Jobs) != 0 {
		t.Fatalf("other client sees %d jobs, want 0", len(otherPage.Jobs))
	}

	resp, raw = doRequest(t, http.MethodPost, server.URL+"/v1/jobs", tokenB, "other-job", body)
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("other client submit status = %d, want 202; body=%s", resp.StatusCode, raw)
	}

	for _, tc := range []struct {
		name      string
		token     string
		wantCount int
	}{
		{"owner sees own job", tokenA, 1},
		{"other client sees own job", tokenB, 1},
		{"operator sees both", operatorToken, 2},
	} {
		resp, raw = doRequest(t, http.MethodGet, server.URL+"/v1/jobs", tc.token, "", "")
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("%s: list status = %d, want 200; body=%s", tc.name, resp.StatusCode, raw)
		}
		var page dto.ListJobsResponse
		if err := json.Unmarshal(raw, &page); err != nil {
			t.Fatalf("decode %s list: %v", tc.name, err)
		}
		if len(page.Jobs) != tc.wantCount {
			t.Errorf("%s: got %d jobs, want %d", tc.name, len(page.Jobs), tc.wantCount)
		}
	}

	// An operator can cancel a job submitted by another key.
	resp, raw = doRequest(t, http.MethodPost, server.URL+"/v1/jobs/"+created.ID.String()+"/cancel", operatorToken, "", "")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("operator cancel status = %d, want 200; body=%s", resp.StatusCode, raw)
	}
}
