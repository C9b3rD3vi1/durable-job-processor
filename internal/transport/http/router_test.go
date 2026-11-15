package httpapi

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/C9b3rD3vi1/durable-job-processor/internal/domain"
	"github.com/C9b3rD3vi1/durable-job-processor/internal/jobs"
	"github.com/C9b3rD3vi1/durable-job-processor/internal/transport/http/dto"
	"github.com/google/uuid"
)

type fakeService struct {
	submitJob     *domain.Job
	submitCreated bool
	submitErr     error
	gotInput      jobs.SubmitInput

	getJob    *domain.Job
	getErr    error
	gotTenant uuid.UUID
	gotID     uuid.UUID
}

func (f *fakeService) Submit(_ context.Context, in jobs.SubmitInput) (*domain.Job, bool, error) {
	f.gotInput = in
	return f.submitJob, f.submitCreated, f.submitErr
}

func (f *fakeService) Get(_ context.Context, tenantID, id uuid.UUID) (*domain.Job, error) {
	f.gotTenant = tenantID
	f.gotID = id
	return f.getJob, f.getErr
}

type fakePinger struct{ err error }

func (p fakePinger) Ping(context.Context) error { return p.err }

func sampleJob(tenantID uuid.UUID) *domain.Job {
	now := time.Date(2026, 11, 15, 9, 0, 0, 0, time.UTC)
	return &domain.Job{
		ID:          uuid.New(),
		TenantID:    tenantID,
		JobType:     "report.generate",
		Payload:     json.RawMessage(`{"report_id":"r-1"}`),
		Status:      domain.StatusQueued,
		Priority:    0,
		RunAt:       now,
		Attempts:    0,
		MaxAttempts: 5,
		CreatedAt:   now,
		UpdatedAt:   now,
	}
}

func testRouter(service *fakeService, pinger Pinger, maxBody int64) http.Handler {
	return NewRouter(Options{
		JobService:   service,
		Pinger:       pinger,
		Logger:       slog.New(slog.NewTextHandler(io.Discard, nil)),
		MaxBodyBytes: maxBody,
	})
}

func TestCreateJob(t *testing.T) {
	tenantID := uuid.New()
	job := sampleJob(tenantID)
	service := &fakeService{submitJob: job, submitCreated: true}
	router := testRouter(service, fakePinger{}, 1<<20)

	body := `{"job_type":"report.generate","payload":{"report_id":"r-1"},"priority":2,"max_attempts":3}`
	req := httptest.NewRequest(http.MethodPost, "/v1/jobs", bytes.NewBufferString(body))
	req.Header.Set("X-Tenant-ID", tenantID.String())
	req.Header.Set("Idempotency-Key", "client-key")
	rec := httptest.NewRecorder()

	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want 202; body=%s", rec.Code, rec.Body.String())
	}
	if location := rec.Header().Get("Location"); location != "/v1/jobs/"+job.ID.String() {
		t.Errorf("Location = %q, want /v1/jobs/%s", location, job.ID)
	}
	if service.gotInput.TenantID != tenantID {
		t.Errorf("tenant = %v, want %v", service.gotInput.TenantID, tenantID)
	}
	if service.gotInput.IdempotencyKey == nil || *service.gotInput.IdempotencyKey != "client-key" {
		t.Errorf("IdempotencyKey = %v, want client-key", service.gotInput.IdempotencyKey)
	}

	var response dto.JobResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &response); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if response.ID != job.ID || response.Status != domain.StatusQueued {
		t.Errorf("response = %+v, want id %v queued", response, job.ID)
	}
}

func TestCreateJobIdempotentReplay(t *testing.T) {
	tenantID := uuid.New()
	job := sampleJob(tenantID)
	service := &fakeService{submitJob: job, submitCreated: false}
	router := testRouter(service, fakePinger{}, 1<<20)

	req := httptest.NewRequest(http.MethodPost, "/v1/jobs", bytes.NewBufferString(
		`{"job_type":"report.generate","payload":{"report_id":"r-1"}}`,
	))
	req.Header.Set("X-Tenant-ID", tenantID.String())
	req.Header.Set("Idempotency-Key", "client-key")
	rec := httptest.NewRecorder()

	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	if got := rec.Header().Get("Idempotent-Replayed"); got != "true" {
		t.Errorf("Idempotent-Replayed = %q, want true", got)
	}
}

func TestGetJob(t *testing.T) {
	tenantID := uuid.New()
	job := sampleJob(tenantID)
	service := &fakeService{getJob: job}
	router := testRouter(service, fakePinger{}, 1<<20)

	req := httptest.NewRequest(http.MethodGet, "/v1/jobs/"+job.ID.String(), nil)
	req.Header.Set("X-Tenant-ID", tenantID.String())
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body=%s", rec.Code, rec.Body.String())
	}
	if service.gotTenant != tenantID || service.gotID != job.ID {
		t.Errorf("service got %v/%v, want %v/%v", service.gotTenant, service.gotID, tenantID, job.ID)
	}
}

func TestGetJobNotFound(t *testing.T) {
	tenantID := uuid.New()
	service := &fakeService{getErr: domain.ErrNotFound}
	router := testRouter(service, fakePinger{}, 1<<20)

	req := httptest.NewRequest(http.MethodGet, "/v1/jobs/"+uuid.New().String(), nil)
	req.Header.Set("X-Tenant-ID", tenantID.String())
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", rec.Code)
	}
}

func TestRequiresTenant(t *testing.T) {
	router := testRouter(&fakeService{}, fakePinger{}, 1<<20)
	req := httptest.NewRequest(http.MethodGet, "/v1/jobs/"+uuid.New().String(), nil)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want 401", rec.Code)
	}
}

func TestRejectsInvalidTenant(t *testing.T) {
	router := testRouter(&fakeService{}, fakePinger{}, 1<<20)
	req := httptest.NewRequest(http.MethodGet, "/v1/jobs/"+uuid.New().String(), nil)
	req.Header.Set("X-Tenant-ID", "not-a-uuid")
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", rec.Code)
	}
}

func TestRejectsInvalidJobID(t *testing.T) {
	router := testRouter(&fakeService{}, fakePinger{}, 1<<20)
	req := httptest.NewRequest(http.MethodGet, "/v1/jobs/not-a-uuid", nil)
	req.Header.Set("X-Tenant-ID", uuid.New().String())
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", rec.Code)
	}
}

func TestRejectsInvalidJSON(t *testing.T) {
	router := testRouter(&fakeService{}, fakePinger{}, 1<<20)
	req := httptest.NewRequest(http.MethodPost, "/v1/jobs", bytes.NewBufferString(`{`))
	req.Header.Set("X-Tenant-ID", uuid.New().String())
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", rec.Code)
	}
}

func TestRejectsUnknownJSONField(t *testing.T) {
	router := testRouter(&fakeService{}, fakePinger{}, 1<<20)
	req := httptest.NewRequest(http.MethodPost, "/v1/jobs", bytes.NewBufferString(
		`{"job_type":"report.generate","payload":{},"tenant_id":"attacker"}`,
	))
	req.Header.Set("X-Tenant-ID", uuid.New().String())
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", rec.Code)
	}
}

func TestRejectsOversizedBody(t *testing.T) {
	router := testRouter(&fakeService{}, fakePinger{}, 16)
	req := httptest.NewRequest(http.MethodPost, "/v1/jobs", bytes.NewBufferString(
		`{"job_type":"report.generate","payload":{"report_id":"r-1"}}`,
	))
	req.Header.Set("X-Tenant-ID", uuid.New().String())
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("status = %d, want 413", rec.Code)
	}
}

func TestHealthEndpoints(t *testing.T) {
	router := testRouter(&fakeService{}, fakePinger{}, 1<<20)

	live := httptest.NewRecorder()
	router.ServeHTTP(live, httptest.NewRequest(http.MethodGet, "/health/live", nil))
	if live.Code != http.StatusOK {
		t.Errorf("live status = %d, want 200", live.Code)
	}

	ready := httptest.NewRecorder()
	router.ServeHTTP(ready, httptest.NewRequest(http.MethodGet, "/health/ready", nil))
	if ready.Code != http.StatusOK {
		t.Errorf("ready status = %d, want 200", ready.Code)
	}
}

func TestReadinessFailure(t *testing.T) {
	router := testRouter(&fakeService{}, fakePinger{err: errors.New("db down")}, 1<<20)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/health/ready", nil))
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503", rec.Code)
	}
}

func TestNotFound(t *testing.T) {
	router := testRouter(&fakeService{}, fakePinger{}, 1<<20)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/nope", nil))
	if rec.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", rec.Code)
	}
}
