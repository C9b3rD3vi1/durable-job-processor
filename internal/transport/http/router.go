// Package httpapi wires the HTTP transport for the job API.
package httpapi

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"time"

	"github.com/C9b3rD3vi1/durable-job-processor/internal/auth"
	"github.com/C9b3rD3vi1/durable-job-processor/internal/observability"
	"github.com/C9b3rD3vi1/durable-job-processor/internal/transport/http/dto"
	"github.com/C9b3rD3vi1/durable-job-processor/internal/transport/http/handlers"
	"github.com/C9b3rD3vi1/durable-job-processor/internal/transport/http/middleware"
)

// Pinger is the bounded dependency check used by readiness.
type Pinger interface {
	Ping(ctx context.Context) error
}

// Options contains the router dependencies.
type Options struct {
	JobService               handlers.JobService
	Pinger                   Pinger
	Authenticator            auth.TokenAuthenticator
	Logger                   *slog.Logger
	Metrics                  *observability.Metrics
	MaxBodyBytes             int64
	RateLimitPerMinute       int
	SubmitRateLimitPerMinute int

	// RateLimitStore, when provided, shares counters across API processes.
	// Without it the router falls back to process-local in-memory limiters.
	RateLimitStore middleware.RateLimitStore
}

// NewRouter builds the HTTP handler tree.
func NewRouter(opts Options) http.Handler {
	logger := opts.Logger
	if logger == nil {
		logger = slog.Default()
	}

	jobsHandler := handlers.NewJobsHandler(opts.JobService, logger, opts.MaxBodyBytes)
	authenticated := middleware.RequireAuth(opts.Authenticator, logger)

	// A shared store makes both budgets global across API replicas. The
	// in-memory fallback keeps unit tests and single-process deployments
	// independent of PostgreSQL.
	newLimiter := func(scope string, limit int) middleware.Limiter {
		if opts.RateLimitStore != nil {
			return middleware.NewStoreRateLimiter(opts.RateLimitStore, scope, limit, time.Minute)
		}
		return middleware.NewRateLimiter(limit, time.Minute)
	}
	rateLimited := middleware.RateLimitWithMetrics(
		newLimiter("request", opts.RateLimitPerMinute), logger, opts.Metrics)
	submitRateLimited := middleware.RateLimitWithMetrics(
		newLimiter("submission", opts.SubmitRateLimitPerMinute), logger, opts.Metrics, "submission")
	// Authentication runs before the per-tenant rate limiter so the tenant is
	// always a verified identity and unknown keys never consume a budget.
	authorized := func(next http.Handler) http.Handler {
		return authenticated(rateLimited(next))
	}
	// Job submissions share the general budget and also carry their own,
	// tighter budget. The submission budget is checked first so an exhausted
	// submission quota does not consume general request capacity.
	submissionAuthorized := func(next http.Handler) http.Handler {
		return authenticated(submitRateLimited(rateLimited(next)))
	}
	mux := http.NewServeMux()

	mux.Handle("POST /v1/jobs", submissionAuthorized(http.HandlerFunc(jobsHandler.CreateJob)))
	mux.Handle("GET /v1/jobs", authorized(http.HandlerFunc(jobsHandler.ListJobs)))
	mux.Handle("GET /v1/jobs/{id}", authorized(http.HandlerFunc(jobsHandler.GetJob)))
	mux.Handle("POST /v1/jobs/{id}/cancel", authorized(http.HandlerFunc(jobsHandler.CancelJob)))
	mux.Handle("POST /v1/jobs/{id}/replay", authorized(
		middleware.RequireRole(auth.RoleOperator)(http.HandlerFunc(jobsHandler.ReplayJob)),
	))

	mux.HandleFunc("GET /health/live", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})
	mux.HandleFunc("GET /health/ready", func(w http.ResponseWriter, r *http.Request) {
		if opts.Pinger == nil {
			writeJSON(w, http.StatusServiceUnavailable, map[string]string{"status": "unavailable"})
			return
		}
		ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
		defer cancel()
		if err := opts.Pinger.Ping(ctx); err != nil {
			logger.Warn("readiness check failed", "error", err)
			writeJSON(w, http.StatusServiceUnavailable, map[string]string{"status": "unavailable"})
			return
		}
		writeJSON(w, http.StatusOK, map[string]string{"status": "ready"})
	})

	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusNotFound, dto.ErrorResponse{
			Error: dto.ErrorBody{Code: "not_found", Message: "route not found"},
		})
	})

	handler := middleware.SecurityHeaders(mux)
	handler = middleware.RequestLogger(logger)(handler)
	handler = middleware.HTTPMetrics(opts.Metrics)(handler)
	handler = middleware.Recoverer(logger)(handler)
	return handler
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}
