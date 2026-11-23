package middleware

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/C9b3rD3vi1/durable-job-processor/internal/auth"
	"github.com/C9b3rD3vi1/durable-job-processor/internal/observability"
	"github.com/google/uuid"
)

func exposition(t *testing.T, metrics *observability.Metrics) string {
	t.Helper()
	recorder := httptest.NewRecorder()
	metrics.Handler("").ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	if recorder.Code != http.StatusOK {
		t.Fatalf("metrics status = %d, want 200", recorder.Code)
	}
	return recorder.Body.String()
}

func TestHTTPMetricsRecordsStatusAndNormalizedRoute(t *testing.T) {
	metrics := observability.NewMetrics()
	handler := HTTPMetrics(metrics)(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusAccepted)
	}))

	req := httptest.NewRequest(http.MethodPost, "/v1/jobs/123e4567-e89b-12d3-a456-426614174000/cancel", nil)
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, req)
	if recorder.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want 202", recorder.Code)
	}

	body := exposition(t, metrics)
	for _, want := range []string{
		`api_http_requests_total{method="POST",route="/v1/jobs/{id}/cancel",status="202"} 1`,
		`api_http_request_duration_seconds_count{method="POST",route="/v1/jobs/{id}/cancel"} 1`,
	} {
		if !strings.Contains(body, want) {
			t.Errorf("metrics missing %q\n%s", want, body)
		}
	}
}

func TestHTTPMetricsRecordsImplicitStatus(t *testing.T) {
	metrics := observability.NewMetrics()
	handler := HTTPMetrics(metrics)(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("ok"))
	}))
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/health/live", nil))

	if !strings.Contains(exposition(t, metrics), `api_http_requests_total{method="GET",route="/health/live",status="200"} 1`) {
		t.Error("implicit 200 status was not recorded")
	}
}

func TestRouteLabel(t *testing.T) {
	tests := map[string]string{
		"/v1/jobs":             "/v1/jobs",
		"/v1/jobs/123":         "/v1/jobs/{id}",
		"/v1/jobs/123/cancel":  "/v1/jobs/{id}/cancel",
		"/v1/jobs/123/replay":  "/v1/jobs/{id}/replay",
		"/health/live":         "/health/live",
		"/health/ready":        "/health/ready",
		"/v1/jobs/123/unknown": "other",
		"/v1/jobs/00000000-0000-0000-0000-000000000000": "/v1/jobs/{id}",
	}
	for path, want := range tests {
		if got := routeLabel(path); got != want {
			t.Errorf("routeLabel(%q) = %q, want %q", path, got, want)
		}
	}
}

func TestRateLimitWithMetricsCountsRejections(t *testing.T) {
	metrics := observability.NewMetrics()
	limiter := NewRateLimiter(1, time.Minute)
	handler := RateLimitWithMetrics(limiter, nil, metrics, "submission")(http.HandlerFunc(
		func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) }))

	tenantID := uuid.New()
	request := func() int {
		req := httptest.NewRequest(http.MethodPost, "/v1/jobs", nil)
		req = req.WithContext(context.WithValue(req.Context(), identityContextKey{}, auth.Identity{TenantID: tenantID}))
		recorder := httptest.NewRecorder()
		handler.ServeHTTP(recorder, req)
		return recorder.Code
	}

	if got := request(); got != http.StatusOK {
		t.Fatalf("first request status = %d, want 200", got)
	}
	if got := request(); got != http.StatusTooManyRequests {
		t.Fatalf("second request status = %d, want 429", got)
	}
	if !strings.Contains(exposition(t, metrics), `api_rate_limited_total{scope="submission"} 1`) {
		t.Error("rate limit rejection was not counted")
	}
}

func TestRateLimitStoreErrorIsCountedAsUnavailable(t *testing.T) {
	metrics := observability.NewMetrics()
	store := &fakeRateLimitStore{
		allow: func(context.Context, string, uuid.UUID, int, time.Duration) (bool, time.Duration, error) {
			return false, 0, errors.New("database unavailable")
		},
	}
	limiter := NewStoreRateLimiter(store, "request", 5, time.Minute)
	handler := RateLimitWithMetrics(limiter, nil, metrics)(http.HandlerFunc(
		func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) }))

	req := httptest.NewRequest(http.MethodGet, "/v1/jobs", nil)
	req = req.WithContext(context.WithValue(req.Context(), identityContextKey{},
		auth.Identity{TenantID: uuid.New()}))
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, req)

	if recorder.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503; body=%s", recorder.Code, recorder.Body.String())
	}
	body := exposition(t, metrics)
	if !strings.Contains(body, `api_rate_limiter_unavailable_total{scope="request"} 1`) {
		t.Errorf("unavailable store error was not counted\n%s", body)
	}
	if strings.Contains(body, `api_rate_limited_total{scope="request"}`) {
		t.Error("store error was incorrectly counted as a rate limit rejection")
	}
}
