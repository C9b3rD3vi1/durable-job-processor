package handlers

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"strings"

	"github.com/C9b3rD3vi1/durable-job-processor/internal/domain"
	"github.com/C9b3rD3vi1/durable-job-processor/internal/jobs"
	"github.com/C9b3rD3vi1/durable-job-processor/internal/transport/http/dto"
	"github.com/C9b3rD3vi1/durable-job-processor/internal/transport/http/middleware"
	"github.com/google/uuid"
)

// JobService is the HTTP-facing subset of the job service.
type JobService interface {
	Submit(ctx context.Context, in jobs.SubmitInput) (*domain.Job, bool, error)
	Get(ctx context.Context, tenantID, id uuid.UUID) (*domain.Job, error)
}

// JobsHandler implements the job submission and lookup endpoints.
type JobsHandler struct {
	service      JobService
	logger       *slog.Logger
	maxBodyBytes int64
}

// NewJobsHandler creates a handler with a hard request-body limit.
func NewJobsHandler(service JobService, logger *slog.Logger, maxBodyBytes int64) *JobsHandler {
	if logger == nil {
		logger = slog.Default()
	}
	if maxBodyBytes <= 0 {
		maxBodyBytes = 1 << 20
	}
	return &JobsHandler{service: service, logger: logger, maxBodyBytes: maxBodyBytes}
}

// CreateJob handles POST /v1/jobs.
func (h *JobsHandler) CreateJob(w http.ResponseWriter, r *http.Request) {
	tenantID, ok := middleware.TenantID(r.Context())
	if !ok {
		writeError(w, http.StatusUnauthorized, "tenant_required", "tenant context is missing")
		return
	}

	var req dto.CreateJobRequest
	if err := h.decodeJSON(w, r, &req); err != nil {
		var maxBytesErr *http.MaxBytesError
		if errors.As(err, &maxBytesErr) {
			writeError(w, http.StatusRequestEntityTooLarge, "request_too_large", "request body is too large")
			return
		}
		writeError(w, http.StatusBadRequest, "invalid_json", "request body must be a single valid JSON object")
		return
	}

	idempotencyKey := strings.TrimSpace(r.Header.Get("Idempotency-Key"))
	var keyPtr *string
	if idempotencyKey != "" {
		keyPtr = &idempotencyKey
	}

	job, created, err := h.service.Submit(r.Context(), jobs.SubmitInput{
		TenantID:       tenantID,
		JobType:        req.JobType,
		Payload:        req.Payload,
		RunAt:          req.ScheduledAt,
		Priority:       req.Priority,
		MaxAttempts:    req.MaxAttempts,
		IdempotencyKey: keyPtr,
	})
	if err != nil {
		h.handleServiceError(w, r, tenantID, uuid.Nil, err)
		return
	}

	w.Header().Set("Location", "/v1/jobs/"+job.ID.String())
	if !created {
		w.Header().Set("Idempotent-Replayed", "true")
	}
	status := http.StatusAccepted
	if !created {
		status = http.StatusOK
	}
	writeJSON(w, status, dto.NewJobResponse(job))
}

// GetJob handles GET /v1/jobs/{id}.
func (h *JobsHandler) GetJob(w http.ResponseWriter, r *http.Request) {
	tenantID, ok := middleware.TenantID(r.Context())
	if !ok {
		writeError(w, http.StatusUnauthorized, "tenant_required", "tenant context is missing")
		return
	}

	id, err := uuid.Parse(strings.TrimSpace(r.PathValue("id")))
	if err != nil || id == uuid.Nil {
		writeError(w, http.StatusBadRequest, "invalid_job_id", "job id must be a valid UUID")
		return
	}

	job, err := h.service.Get(r.Context(), tenantID, id)
	if err != nil {
		h.handleServiceError(w, r, tenantID, id, err)
		return
	}
	writeJSON(w, http.StatusOK, dto.NewJobResponse(job))
}

func (h *JobsHandler) decodeJSON(w http.ResponseWriter, r *http.Request, dst any) error {
	r.Body = http.MaxBytesReader(w, r.Body, h.maxBodyBytes)
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(dst); err != nil {
		return err
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return errors.New("request body must contain exactly one JSON value")
	}
	return nil
}

func (h *JobsHandler) handleServiceError(w http.ResponseWriter, r *http.Request, tenantID, jobID uuid.UUID, err error) {
	switch {
	case errors.Is(err, domain.ErrInvalidJob), errors.Is(err, domain.ErrInvalidArgument):
		writeError(w, http.StatusBadRequest, "invalid_job", err.Error())
	case errors.Is(err, domain.ErrNotFound):
		writeError(w, http.StatusNotFound, "job_not_found", "job not found")
	case errors.Is(err, context.Canceled):
		writeError(w, http.StatusRequestTimeout, "request_canceled", "request was canceled")
	default:
		h.logger.Error("job request failed",
			"error", err,
			"tenant_id", tenantID,
			"job_id", jobID,
			"method", r.Method,
			"path", r.URL.Path,
		)
		writeError(w, http.StatusInternalServerError, "internal_error", "an internal error occurred")
	}
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

func writeError(w http.ResponseWriter, status int, code, message string) {
	writeJSON(w, status, dto.ErrorResponse{
		Error: dto.ErrorBody{Code: code, Message: message},
	})
}
