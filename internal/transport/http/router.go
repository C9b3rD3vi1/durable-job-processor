// Package httpapi wires the HTTP transport for the job API.
package httpapi

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"time"

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
	JobService   handlers.JobService
	Pinger       Pinger
	Logger       *slog.Logger
	MaxBodyBytes int64
}

// NewRouter builds the HTTP handler tree.
func NewRouter(opts Options) http.Handler {
	logger := opts.Logger
	if logger == nil {
		logger = slog.Default()
	}

	jobsHandler := handlers.NewJobsHandler(opts.JobService, logger, opts.MaxBodyBytes)
	mux := http.NewServeMux()

	mux.Handle("POST /v1/jobs", middleware.RequireTenant(http.HandlerFunc(jobsHandler.CreateJob)))
	mux.Handle("GET /v1/jobs/{id}", middleware.RequireTenant(http.HandlerFunc(jobsHandler.GetJob)))
	mux.Handle("POST /v1/jobs/{id}/replay", middleware.RequireTenant(http.HandlerFunc(jobsHandler.ReplayJob)))

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
	handler = middleware.Recoverer(logger)(handler)
	return handler
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}
