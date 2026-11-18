package integration_test

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/C9b3rD3vi1/durable-job-processor/internal/auth"
	"github.com/C9b3rD3vi1/durable-job-processor/internal/transport/http/dto"
	"github.com/google/uuid"
)

func TestSubmitRejectsPayloadThatFailsJobSchema(t *testing.T) {
	databaseURL := testDatabaseURL(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	pool := mustIsolatedPool(t, databaseURL)
	tenantID := uuid.New()
	server := newAPI(t, pool)
	token := createAPIKey(t, ctx, pool, tenantID, "schema-client", auth.RoleClient)

	tests := []struct {
		name string
		body string
	}{
		{"report without id", `{"job_type":"report.generate","payload":{}}`},
		{"email without recipient", `{"job_type":"email.send","payload":{"template":"welcome"}}`},
		{"import without source", `{"job_type":"data.import","payload":{}}`},
		{"webhook with invalid url", `{"job_type":"webhook.deliver","payload":{"url":"file:///tmp/hook"}}`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resp, body := doRequest(t, http.MethodPost, server.URL+"/v1/jobs", token, "", tt.body)
			if resp.StatusCode != http.StatusBadRequest {
				t.Fatalf("submit status = %d, want 400; body=%s", resp.StatusCode, body)
			}
			var response dto.ErrorResponse
			if err := json.Unmarshal(body, &response); err != nil {
				t.Fatalf("decode error response: %v", err)
			}
			if response.Error.Code != "invalid_job" {
				t.Errorf("error code = %q, want invalid_job", response.Error.Code)
			}
		})
	}

	var count int
	if err := pool.QueryRow(ctx,
		`SELECT count(*) FROM jobs WHERE tenant_id = $1`,
		tenantID.String(),
	).Scan(&count); err != nil {
		t.Fatalf("count jobs: %v", err)
	}
	if count != 0 {
		t.Fatalf("invalid submissions created %d durable jobs, want 0", count)
	}

	// A valid body for the same job type still succeeds.
	resp, body := doRequest(t, http.MethodPost, server.URL+"/v1/jobs", token, "",
		`{"job_type":"report.generate","payload":{"report_id":"r-1"}}`)
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("valid submit status = %d, want 202; body=%s", resp.StatusCode, body)
	}
}
