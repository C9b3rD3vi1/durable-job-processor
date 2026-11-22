package observability

import (
	"context"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestMetricsExposition(t *testing.T) {
	m := NewMetrics()
	m.Inc(MetricJobsSubmittedTotal, L("job_type", "webhook.deliver"), L("result", "created"))
	m.Inc(MetricJobsSubmittedTotal, L("job_type", "webhook.deliver"), L("result", "created"))
	m.Set(MetricWorkerActive, 3)
	m.Observe(MetricJobExecutionDuration, 0.02, L("job_type", "report.generate"))
	m.Observe(MetricJobExecutionDuration, 0.25, L("job_type", "report.generate"))

	body := m.render()
	for _, want := range []string{
		"# HELP jobs_submitted_total Job submissions, by result (created, replayed, rejected, error).",
		"# TYPE jobs_submitted_total counter",
		`jobs_submitted_total{job_type="webhook.deliver",result="created"} 2`,
		"# TYPE worker_active_count gauge",
		"worker_active_count 3",
		"# TYPE job_execution_duration_seconds histogram",
		`job_execution_duration_seconds_bucket{job_type="report.generate",le="0.025"} 1`,
		`job_execution_duration_seconds_bucket{job_type="report.generate",le="0.25"} 2`,
		`job_execution_duration_seconds_bucket{job_type="report.generate",le="+Inf"} 2`,
		`job_execution_duration_seconds_sum{job_type="report.generate"} 0.27`,
		`job_execution_duration_seconds_count{job_type="report.generate"} 2`,
	} {
		if !strings.Contains(body, want) {
			t.Errorf("exposition missing %q\n%s", want, body)
		}
	}
}

func TestMetricsLabelEscapingAndOrdering(t *testing.T) {
	m := NewMetrics()
	m.Inc(MetricRateLimitedTotal, L("scope", `sub"mission\test`+"\nnext"), L("alpha", "z"))
	body := m.render()
	for _, want := range []string{
		`api_rate_limited_total{alpha="z",scope="sub\"mission\\test\nnext"} 1`,
	} {
		if !strings.Contains(body, want) {
			t.Errorf("exposition missing %q\n%s", want, body)
		}
	}
}

func TestMetricsConcurrentRecording(t *testing.T) {
	m := NewMetrics()
	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				m.Inc(MetricJobAttemptsTotal, L("job_type", "report.generate"))
				m.Observe(MetricJobWaitDuration, 0.5)
			}
		}()
	}
	wg.Wait()

	if !strings.Contains(m.render(), `job_attempts_total{job_type="report.generate"} 1600`) {
		t.Error("counter lost concurrent increments")
	}
	if !strings.Contains(m.render(), "job_wait_duration_seconds_count 1600") {
		t.Error("histogram lost concurrent observations")
	}
}

func TestNilMetricsAreSafe(t *testing.T) {
	var m *Metrics
	m.Inc(MetricJobsSubmittedTotal)
	m.Add(MetricWorkerActive, -1)
	m.Set(MetricDBPoolInUse, 2)
	m.Observe(MetricJobExecutionDuration, 1)
	if got := m.render(); got != "" {
		t.Errorf("render() = %q, want empty", got)
	}
}

func TestMetricsHandlerAuthentication(t *testing.T) {
	m := NewMetrics()
	m.Inc(MetricJobsCompletedTotal, L("job_type", "email.send"))

	protected := m.Handler("secret-token")
	tests := []struct {
		name       string
		method     string
		auth       string
		wantStatus int
	}{
		{"missing token", http.MethodGet, "", http.StatusUnauthorized},
		{"wrong token", http.MethodGet, "Bearer wrong", http.StatusUnauthorized},
		{"malformed header", http.MethodGet, "secret-token", http.StatusUnauthorized},
		{"valid token", http.MethodGet, "Bearer secret-token", http.StatusOK},
		{"case insensitive scheme", http.MethodGet, "bearer secret-token", http.StatusOK},
		{"post not allowed", http.MethodPost, "Bearer secret-token", http.StatusMethodNotAllowed},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest(tt.method, "/metrics", nil)
			if tt.auth != "" {
				req.Header.Set("Authorization", tt.auth)
			}
			recorder := httptest.NewRecorder()
			protected.ServeHTTP(recorder, req)
			if recorder.Code != tt.wantStatus {
				t.Fatalf("status = %d, want %d", recorder.Code, tt.wantStatus)
			}
			if tt.wantStatus == http.StatusOK &&
				!strings.Contains(recorder.Body.String(), `jobs_completed_total{job_type="email.send"} 1`) {
				t.Errorf("unexpected body: %s", recorder.Body.String())
			}
		})
	}

	public := m.Handler("")
	recorder := httptest.NewRecorder()
	public.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	if recorder.Code != http.StatusOK {
		t.Fatalf("public status = %d, want 200", recorder.Code)
	}
}

func TestServeMetricsRejectsUnauthenticatedPublicBinding(t *testing.T) {
	if err := ServeMetrics(context.Background(), ":19099", "", NewMetrics()); err == nil {
		t.Fatal("ServeMetrics() error = nil, want token requirement")
	}
	if err := ServeMetrics(context.Background(), "not-an-address", "token", NewMetrics()); err == nil {
		t.Fatal("ServeMetrics() error = nil, want invalid address error")
	}
}

func TestServeMetricsReturnsBindError(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserve port: %v", err)
	}
	defer listener.Close()

	if err := ServeMetrics(context.Background(), listener.Addr().String(), "", NewMetrics()); err == nil {
		t.Fatal("ServeMetrics() error = nil, want bind conflict error")
	}
}

func TestServeMetricsShutsDownWithContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	done := make(chan error, 1)
	go func() {
		done <- ServeMetrics(ctx, "127.0.0.1:0", "", NewMetrics())
	}()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("ServeMetrics() error = %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("ServeMetrics() did not return after cancellation")
	}
}

func TestPollPoolStats(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	m := NewMetrics()
	done := make(chan struct{})
	go func() {
		PollPoolStats(ctx, time.Millisecond, func() PoolStats {
			return PoolStats{InUse: 4, Idle: 6, WaitCount: 2, MaxConns: 10}
		}, m)
		close(done)
	}()

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		body := m.render()
		if strings.Contains(body, "db_pool_in_use 4") &&
			strings.Contains(body, "db_pool_idle 6") &&
			strings.Contains(body, "db_pool_wait_count 2") &&
			strings.Contains(body, "db_pool_max_conns 10") {
			cancel()
			select {
			case <-done:
			case <-time.After(time.Second):
				t.Fatal("PollPoolStats() did not stop after cancellation")
			}
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("pool gauges were not published")
}
