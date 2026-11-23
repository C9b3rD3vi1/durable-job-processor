package handlers

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/C9b3rD3vi1/durable-job-processor/internal/auth"
	"github.com/C9b3rD3vi1/durable-job-processor/internal/domain"
	"github.com/C9b3rD3vi1/durable-job-processor/internal/jobs"
	"github.com/C9b3rD3vi1/durable-job-processor/internal/transport/http/dto"
	"github.com/C9b3rD3vi1/durable-job-processor/internal/transport/http/middleware"
	"github.com/google/uuid"
)

// JobService is the HTTP-facing subset of the job service.
type JobService interface {
	Submit(ctx context.Context, in jobs.SubmitInput) (*domain.Job, bool, error)
	Get(ctx context.Context, actor jobs.Actor, id uuid.UUID) (*domain.Job, error)
	List(ctx context.Context, actor jobs.Actor, in jobs.ListInput) (*jobs.ListResult, error)
	Cancel(ctx context.Context, actor jobs.Actor, id uuid.UUID) (*domain.Job, bool, error)
	Replay(ctx context.Context, tenantID, id uuid.UUID, replayedBy string) (*domain.Job, error)
}

// actorFromIdentity carries verified API key identity into the service layer.
// No client-controlled field can influence tenant, key or operator status.
func actorFromIdentity(identity auth.Identity) jobs.Actor {
	return jobs.Actor{
		TenantID:   identity.TenantID,
		KeyID:      identity.KeyID,
		IsOperator: identity.IsOperator(),
	}
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
	identity, ok := middleware.IdentityFrom(r.Context())
	if !ok {
		writeError(w, http.StatusUnauthorized, "authentication_required", "authentication is required")
		return
	}
	tenantID := identity.TenantID

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
		CreatedByKeyID: identity.KeyID,
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
	identity, ok := middleware.IdentityFrom(r.Context())
	if !ok {
		writeError(w, http.StatusUnauthorized, "authentication_required", "authentication is required")
		return
	}
	tenantID := identity.TenantID

	id, err := uuid.Parse(strings.TrimSpace(r.PathValue("id")))
	if err != nil || id == uuid.Nil {
		writeError(w, http.StatusBadRequest, "invalid_job_id", "job id must be a valid UUID")
		return
	}

	job, err := h.service.Get(r.Context(), actorFromIdentity(identity), id)
	if err != nil {
		h.handleServiceError(w, r, tenantID, id, err)
		return
	}
	writeJSON(w, http.StatusOK, dto.NewJobResponse(job))
}

// ListJobs handles GET /v1/jobs with cursor pagination and optional filters.
func (h *JobsHandler) ListJobs(w http.ResponseWriter, r *http.Request) {
	identity, ok := middleware.IdentityFrom(r.Context())
	if !ok {
		writeError(w, http.StatusUnauthorized, "authentication_required", "authentication is required")
		return
	}

	query := r.URL.Query()
	in := jobs.ListInput{
		JobType: strings.TrimSpace(query.Get("job_type")),
		Cursor:  strings.TrimSpace(query.Get("cursor")),
	}

	if raw := strings.TrimSpace(query.Get("status")); raw != "" {
		status := domain.JobStatus(raw)
		if !status.Valid() {
			writeError(w, http.StatusBadRequest, "invalid_query", "status must be a known job status")
			return
		}
		in.Status = &status
	}

	var err error
	if in.CreatedAfter, err = parseTimeQuery(query.Get("created_after"), "created_after"); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_query", err.Error())
		return
	}
	if in.CreatedBefore, err = parseTimeQuery(query.Get("created_before"), "created_before"); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_query", err.Error())
		return
	}
	if in.CreatedAfter != nil && in.CreatedBefore != nil && in.CreatedAfter.After(*in.CreatedBefore) {
		writeError(w, http.StatusBadRequest, "invalid_query", "created_after must not be later than created_before")
		return
	}
	if raw := strings.TrimSpace(query.Get("limit")); raw != "" {
		limit, convErr := strconv.Atoi(raw)
		if convErr != nil {
			writeError(w, http.StatusBadRequest, "invalid_query", "limit must be an integer")
			return
		}
		if limit < 1 || limit > jobs.MaxListLimit {
			writeError(w, http.StatusBadRequest, "invalid_query",
				fmt.Sprintf("limit must be between 1 and %d", jobs.MaxListLimit))
			return
		}
		in.Limit = limit
	}

	result, err := h.service.List(r.Context(), actorFromIdentity(identity), in)
	if err != nil {
		h.handleServiceError(w, r, identity.TenantID, uuid.Nil, err)
		return
	}
	writeJSON(w, http.StatusOK, dto.NewListJobsResponse(result.Jobs, result.NextCursor))
}

// CancelJob handles POST /v1/jobs/{id}/cancel. Queued and retry_wait jobs are
// cancelled immediately; a running job receives a durable cooperative
// cancellation request and is finalized by its worker.
func (h *JobsHandler) CancelJob(w http.ResponseWriter, r *http.Request) {
	identity, ok := middleware.IdentityFrom(r.Context())
	if !ok {
		writeError(w, http.StatusUnauthorized, "authentication_required", "authentication is required")
		return
	}

	id, err := uuid.Parse(strings.TrimSpace(r.PathValue("id")))
	if err != nil || id == uuid.Nil {
		writeError(w, http.StatusBadRequest, "invalid_job_id", "job id must be a valid UUID")
		return
	}

	job, cooperative, err := h.service.Cancel(r.Context(), actorFromIdentity(identity), id)
	if err != nil {
		h.handleServiceError(w, r, identity.TenantID, id, err)
		return
	}

	h.logger.Info("job cancellation requested",
		"event", "job.cancelled",
		"tenant_id", identity.TenantID,
		"job_id", job.ID,
		"status", job.Status,
		"cooperative", cooperative,
		"api_key_id", identity.KeyID,
	)

	if cooperative {
		w.Header().Set("Cancel-Requested", "true")
		writeJSON(w, http.StatusAccepted, dto.NewJobResponse(job))
		return
	}
	writeJSON(w, http.StatusOK, dto.NewJobResponse(job))
}

func parseTimeQuery(raw, name string) (*time.Time, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil, nil
	}
	value, err := time.Parse(time.RFC3339, raw)
	if err != nil {
		return nil, fmt.Errorf("%s must be an RFC3339 timestamp", name)
	}
	value = value.UTC()
	return &value, nil
}

// ReplayJob handles POST /v1/jobs/{id}/replay. Only operators may replay, and
// only dead jobs owned by the operator's tenant are eligible.
func (h *JobsHandler) ReplayJob(w http.ResponseWriter, r *http.Request) {
	identity, ok := middleware.IdentityFrom(r.Context())
	if !ok {
		writeError(w, http.StatusUnauthorized, "authentication_required", "authentication is required")
		return
	}
	if !identity.IsOperator() {
		writeError(w, http.StatusForbidden, "forbidden", "operator role is required to replay dead jobs")
		return
	}
	tenantID := identity.TenantID

	id, err := uuid.Parse(strings.TrimSpace(r.PathValue("id")))
	if err != nil || id == uuid.Nil {
		writeError(w, http.StatusBadRequest, "invalid_job_id", "job id must be a valid UUID")
		return
	}

	// The authenticated operator key is the auditable replay actor.
	replayedBy := identity.KeyName
	job, err := h.service.Replay(r.Context(), tenantID, id, replayedBy)
	if err != nil {
		h.handleServiceError(w, r, tenantID, id, err)
		return
	}

	h.logger.Info("job replayed",
		"event", "job.replayed",
		"tenant_id", tenantID,
		"job_id", job.ID,
		"replayed_by", replayedBy,
		"api_key_id", identity.KeyID,
		"replay_count", job.ReplayCount,
	)
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
	case errors.Is(err, domain.ErrInvalidJob):
		writeError(w, http.StatusBadRequest, "invalid_job", err.Error())
	case errors.Is(err, domain.ErrInvalidArgument):
		writeError(w, http.StatusBadRequest, "invalid_argument", err.Error())
	case errors.Is(err, domain.ErrNotFound):
		writeError(w, http.StatusNotFound, "job_not_found", "job not found")
	case errors.Is(err, domain.ErrQuotaExceeded):
		writeError(w, http.StatusTooManyRequests, "quota_exceeded", err.Error())
	case errors.Is(err, domain.ErrInvalidTransition):
		writeError(w, http.StatusConflict, "invalid_transition", "job state does not allow this operation")
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
