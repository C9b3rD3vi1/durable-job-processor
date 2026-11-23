package middleware

import (
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/C9b3rD3vi1/durable-job-processor/internal/observability"
)

// metricsStatusRecorder captures the final status code without changing response
// behaviour. Unwrap keeps http.ResponseController features available.
type metricsStatusRecorder struct {
	http.ResponseWriter
	status int
	wrote  bool
}

func (w *metricsStatusRecorder) WriteHeader(code int) {
	if w.wrote {
		return
	}
	w.status = code
	w.wrote = true
	w.ResponseWriter.WriteHeader(code)
}

func (w *metricsStatusRecorder) Write(body []byte) (int, error) {
	if !w.wrote {
		w.WriteHeader(http.StatusOK)
	}
	return w.ResponseWriter.Write(body)
}

func (w *metricsStatusRecorder) Unwrap() http.ResponseWriter {
	return w.ResponseWriter
}

// HTTPMetrics records request count, status and duration for every response.
// Route labels are normalized so job IDs never become label values.
func HTTPMetrics(metrics *observability.Metrics) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			started := time.Now()
			recorder := &metricsStatusRecorder{ResponseWriter: w, status: http.StatusOK}
			// Record with defer so a recovered panic is counted as a 500 even if
			// an inner recoverer writes the response before returning.
			defer func() {
				route := routeLabel(r.URL.Path)
				methodLabels := []observability.Label{observability.L("method", r.Method), observability.L("route", route)}
				metrics.Inc(observability.MetricHTTPRequestsTotal,
					append(methodLabels, observability.L("status", strconv.Itoa(recorder.status)))...,
				)
				metrics.Observe(observability.MetricHTTPRequestDuration, time.Since(started).Seconds(), methodLabels...)
			}()
			next.ServeHTTP(recorder, r)
		})
	}
}

// routeLabel collapses parameterized paths to bounded label values. Unknown
// subpaths are deliberately grouped as "other" instead of being treated as a
// known route.
func routeLabel(path string) string {
	if path == "/health/live" || path == "/health/ready" {
		return path
	}
	parts := strings.Split(strings.Trim(path, "/"), "/")
	if len(parts) < 2 || parts[0] != "v1" || parts[1] != "jobs" {
		return "other"
	}
	switch len(parts) {
	case 2:
		return "/v1/jobs"
	case 3:
		return "/v1/jobs/{id}"
	case 4:
		switch parts[3] {
		case "cancel":
			return "/v1/jobs/{id}/cancel"
		case "replay":
			return "/v1/jobs/{id}/replay"
		}
	}
	return "other"
}
