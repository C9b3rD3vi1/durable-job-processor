package integration_test

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/C9b3rD3vi1/durable-job-processor/internal/auth"
	"github.com/C9b3rD3vi1/durable-job-processor/internal/handlers"
	"github.com/C9b3rD3vi1/durable-job-processor/internal/jobs"
	"github.com/C9b3rD3vi1/durable-job-processor/internal/observability"
	"github.com/C9b3rD3vi1/durable-job-processor/internal/postgres"
	httpapi "github.com/C9b3rD3vi1/durable-job-processor/internal/transport/http"
	"github.com/C9b3rD3vi1/durable-job-processor/internal/transport/http/dto"
	"github.com/google/uuid"
)

func TestHTTPSubmissionRateLimitIsSharedAcrossServers(t *testing.T) {
	pool := mustPool(t, testDatabaseURL(t))
	defer pool.Close()
	applyMigrations(t, pool)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	tenantID := uuid.New()
	token := createAPIKey(t, ctx, pool, tenantID, "http-rate-limit", auth.RoleOperator)
	defer func() {
		_, _ = pool.Exec(context.Background(), `DELETE FROM jobs WHERE tenant_id = $1`, tenantID)
		_, _ = pool.Exec(context.Background(), `DELETE FROM api_keys WHERE tenant_id = $1`, tenantID)
		_, _ = pool.Exec(context.Background(), `DELETE FROM rate_limit_counters WHERE tenant_id = $1`, tenantID)
	}()

	metrics := observability.NewMetrics()
	newServer := func() *httptest.Server {
		router := httpapi.NewRouter(httpapi.Options{
			JobService: jobs.NewService(
				postgres.NewJobRepository(pool),
				jobs.NewValidator(handlers.DefaultRegistry(), 256*1024, 5),
			),
			Pinger:                   pool,
			Authenticator:            auth.NewAuthenticator(postgres.NewAPIKeyRepository(pool)),
			Logger:                   slog.New(slog.NewTextHandler(io.Discard, nil)),
			Metrics:                  metrics,
			MaxBodyBytes:             1 << 20,
			SubmitRateLimitPerMinute: 2,
			RateLimitStore:           postgres.NewRateLimitRepository(pool),
		})
		server := httptest.NewServer(router)
		t.Cleanup(server.Close)
		return server
	}

	first := newServer()
	second := newServer()
	body := `{"job_type":"report.generate","payload":{"report_id":"http-rate-limit"}}`

	responses := []struct {
		server *httptest.Server
		want   int
	}{
		{first, http.StatusAccepted},
		{second, http.StatusAccepted},
		{first, http.StatusTooManyRequests},
	}
	for i, tt := range responses {
		resp, data := doRequest(t, http.MethodPost, tt.server.URL+"/v1/jobs", token,
			"http-rate-"+uuid.NewString(), body)
		if resp.StatusCode != tt.want {
			t.Fatalf("request %d status = %d, want %d; body=%s", i, resp.StatusCode, tt.want, data)
		}
		if tt.want == http.StatusTooManyRequests {
			if resp.Header.Get("Retry-After") == "" {
				t.Error("rate limit response is missing Retry-After")
			}
			var response dto.ErrorResponse
			if err := json.Unmarshal(data, &response); err != nil {
				t.Fatalf("decode rate limit error: %v", err)
			}
			if response.Error.Code != "rate_limited" {
				t.Errorf("error code = %q, want rate_limited", response.Error.Code)
			}
		}
	}

	// The general budget is disabled in this router, so reads still succeed.
	resp, data := doRequest(t, http.MethodGet, first.URL+"/v1/jobs", token, "", "")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET status = %d, want 200; body=%s", resp.StatusCode, data)
	}

	recorder := httptest.NewRecorder()
	metrics.Handler("").ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	if !strings.Contains(recorder.Body.String(), `api_rate_limited_total{scope="submission"} 1`) {
		t.Errorf("HTTP rate limit rejection was not counted:\n%s", recorder.Body.String())
	}
}
