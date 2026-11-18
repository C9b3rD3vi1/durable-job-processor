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
	"strings"
	"testing"
	"time"

	"github.com/C9b3rD3vi1/durable-job-processor/internal/auth"
	"github.com/C9b3rD3vi1/durable-job-processor/internal/domain"
	"github.com/C9b3rD3vi1/durable-job-processor/internal/jobs"
	"github.com/C9b3rD3vi1/durable-job-processor/internal/transport/http/dto"
	"github.com/google/uuid"
)

const (
	clientToken   = "djp_client-token"
	operatorToken = "djp_operator-token"
)

type fakeService struct {
	submitJob     *domain.Job
	submitCreated bool
	submitErr     error
	gotInput      jobs.SubmitInput

	getJob    *domain.Job
	getErr    error
	gotTenant uuid.UUID
	gotActor  jobs.Actor
	gotID     uuid.UUID

	replayJob     *domain.Job
	replayErr     error
	gotReplayedBy string

	listResult *jobs.ListResult
	listErr    error
	gotList    jobs.ListInput

	cancelJob     *domain.Job
	cancelRequest bool
	cancelErr     error
}

func (f *fakeService) Submit(_ context.Context, in jobs.SubmitInput) (*domain.Job, bool, error) {
	f.gotInput = in
	return f.submitJob, f.submitCreated, f.submitErr
}

func (f *fakeService) Get(_ context.Context, actor jobs.Actor, id uuid.UUID) (*domain.Job, error) {
	f.gotTenant = actor.TenantID
	f.gotActor = actor
	f.gotID = id
	return f.getJob, f.getErr
}

func (f *fakeService) Replay(_ context.Context, tenantID, id uuid.UUID, replayedBy string) (*domain.Job, error) {
	f.gotTenant = tenantID
	f.gotID = id
	f.gotReplayedBy = replayedBy
	return f.replayJob, f.replayErr
}

func (f *fakeService) List(_ context.Context, actor jobs.Actor, in jobs.ListInput) (*jobs.ListResult, error) {
	f.gotTenant = actor.TenantID
	f.gotActor = actor
	f.gotList = in
	return f.listResult, f.listErr
}

func (f *fakeService) Cancel(_ context.Context, actor jobs.Actor, id uuid.UUID) (*domain.Job, bool, error) {
	f.gotTenant = actor.TenantID
	f.gotActor = actor
	f.gotID = id
	return f.cancelJob, f.cancelRequest, f.cancelErr
}

type fakePinger struct{ err error }

func (p fakePinger) Ping(context.Context) error { return p.err }

type fakeAuthenticator struct {
	identities map[string]auth.Identity
	err        error
}

func (f *fakeAuthenticator) Authenticate(_ context.Context, token string) (auth.Identity, error) {
	if f.err != nil {
		return auth.Identity{}, f.err
	}
	identity, ok := f.identities[token]
	if !ok {
		return auth.Identity{}, auth.ErrInvalidCredential
	}
	return identity, nil
}

func testAuthenticator(tenantID uuid.UUID) *fakeAuthenticator {
	return &fakeAuthenticator{identities: map[string]auth.Identity{
		clientToken: {
			TenantID: tenantID,
			KeyID:    uuid.New(),
			KeyName:  "client-key",
			Role:     auth.RoleClient,
		},
		operatorToken: {
			TenantID: tenantID,
			KeyID:    uuid.New(),
			KeyName:  "operator-key",
			Role:     auth.RoleOperator,
		},
	}}
}

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

func testRouter(service *fakeService, pinger Pinger, maxBody int64, authenticator auth.TokenAuthenticator) http.Handler {
	return NewRouter(Options{
		JobService:    service,
		Pinger:        pinger,
		Authenticator: authenticator,
		Logger:        slog.New(slog.NewTextHandler(io.Discard, nil)),
		MaxBodyBytes:  maxBody,
	})
}

func authenticate(req *http.Request, token string) {
	req.Header.Set("Authorization", "Bearer "+token)
}

func TestCreateJob(t *testing.T) {
	tenantID := uuid.New()
	job := sampleJob(tenantID)
	service := &fakeService{submitJob: job, submitCreated: true}
	router := testRouter(service, fakePinger{}, 1<<20, testAuthenticator(tenantID))

	body := `{"job_type":"report.generate","payload":{"report_id":"r-1"},"priority":2,"max_attempts":3}`
	req := httptest.NewRequest(http.MethodPost, "/v1/jobs", bytes.NewBufferString(body))
	authenticate(req, clientToken)
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
	router := testRouter(service, fakePinger{}, 1<<20, testAuthenticator(tenantID))

	req := httptest.NewRequest(http.MethodPost, "/v1/jobs", bytes.NewBufferString(
		`{"job_type":"report.generate","payload":{"report_id":"r-1"}}`,
	))
	authenticate(req, clientToken)
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

func TestTenantHeaderIsIgnored(t *testing.T) {
	tenantID := uuid.New()
	otherTenantID := uuid.New()
	job := sampleJob(tenantID)
	service := &fakeService{getJob: job}
	router := testRouter(service, fakePinger{}, 1<<20, testAuthenticator(tenantID))

	req := httptest.NewRequest(http.MethodGet, "/v1/jobs/"+job.ID.String(), nil)
	authenticate(req, clientToken)
	// A caller-supplied tenant header must never override verified identity.
	req.Header.Set("X-Tenant-ID", otherTenantID.String())
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body=%s", rec.Code, rec.Body.String())
	}
	if service.gotTenant != tenantID {
		t.Errorf("tenant = %v, want verified identity tenant %v", service.gotTenant, tenantID)
	}
}

func TestGetJob(t *testing.T) {
	tenantID := uuid.New()
	job := sampleJob(tenantID)
	service := &fakeService{getJob: job}
	router := testRouter(service, fakePinger{}, 1<<20, testAuthenticator(tenantID))

	req := httptest.NewRequest(http.MethodGet, "/v1/jobs/"+job.ID.String(), nil)
	authenticate(req, clientToken)
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
	router := testRouter(service, fakePinger{}, 1<<20, testAuthenticator(tenantID))

	req := httptest.NewRequest(http.MethodGet, "/v1/jobs/"+uuid.New().String(), nil)
	authenticate(req, clientToken)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", rec.Code)
	}
}

func TestListJobs(t *testing.T) {
	tenantID := uuid.New()
	first := sampleJob(tenantID)
	second := sampleJob(tenantID)
	service := &fakeService{listResult: &jobs.ListResult{
		Jobs:       []*domain.Job{first, second},
		NextCursor: "next-page",
	}}
	router := testRouter(service, fakePinger{}, 1<<20, testAuthenticator(tenantID))

	req := httptest.NewRequest(http.MethodGet,
		"/v1/jobs?status=queued&job_type=report.generate&limit=2&created_after=2026-11-01T00:00:00Z&cursor=abc",
		nil,
	)
	authenticate(req, clientToken)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body=%s", rec.Code, rec.Body.String())
	}
	if service.gotTenant != tenantID {
		t.Errorf("tenant = %v, want %v", service.gotTenant, tenantID)
	}
	if service.gotList.Status == nil || *service.gotList.Status != domain.StatusQueued {
		t.Errorf("status filter = %v, want queued", service.gotList.Status)
	}
	if service.gotList.JobType != "report.generate" {
		t.Errorf("job_type filter = %q, want report.generate", service.gotList.JobType)
	}
	if service.gotList.Limit != 2 {
		t.Errorf("limit = %d, want 2", service.gotList.Limit)
	}
	if service.gotList.CreatedAfter == nil {
		t.Error("created_after was not parsed")
	}
	if service.gotList.Cursor != "abc" {
		t.Errorf("cursor = %q, want abc", service.gotList.Cursor)
	}

	var response dto.ListJobsResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &response); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if len(response.Jobs) != 2 || response.NextCursor != "next-page" {
		t.Errorf("response = %+v, want 2 jobs and next-page cursor", response)
	}
}

func TestListJobsRejectsInvalidQuery(t *testing.T) {
	tenantID := uuid.New()
	router := testRouter(&fakeService{}, fakePinger{}, 1<<20, testAuthenticator(tenantID))

	for _, query := range []string{
		"limit=not-a-number",
		"created_after=yesterday",
		"created_before=2026-11-16",
	} {
		req := httptest.NewRequest(http.MethodGet, "/v1/jobs?"+query, nil)
		authenticate(req, clientToken)
		rec := httptest.NewRecorder()
		router.ServeHTTP(rec, req)

		if rec.Code != http.StatusBadRequest {
			t.Errorf("query %q status = %d, want 400", query, rec.Code)
		}
		var response dto.ErrorResponse
		if err := json.Unmarshal(rec.Body.Bytes(), &response); err != nil {
			t.Fatalf("decode error response: %v", err)
		}
		if response.Error.Code != "invalid_query" {
			t.Errorf("query %q error code = %q, want invalid_query", query, response.Error.Code)
		}
	}
}

func TestCancelQueuedJob(t *testing.T) {
	tenantID := uuid.New()
	job := sampleJob(tenantID)
	job.Status = domain.StatusCancelled
	service := &fakeService{cancelJob: job}
	router := testRouter(service, fakePinger{}, 1<<20, testAuthenticator(tenantID))

	req := httptest.NewRequest(http.MethodPost, "/v1/jobs/"+job.ID.String()+"/cancel", nil)
	authenticate(req, clientToken)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body=%s", rec.Code, rec.Body.String())
	}
	if service.gotTenant != tenantID || service.gotID != job.ID {
		t.Errorf("service got %v/%v, want %v/%v", service.gotTenant, service.gotID, tenantID, job.ID)
	}
}

func TestCancelRunningJobIsCooperative(t *testing.T) {
	tenantID := uuid.New()
	job := sampleJob(tenantID)
	job.Status = domain.StatusRunning
	service := &fakeService{cancelJob: job, cancelRequest: true}
	router := testRouter(service, fakePinger{}, 1<<20, testAuthenticator(tenantID))

	req := httptest.NewRequest(http.MethodPost, "/v1/jobs/"+job.ID.String()+"/cancel", nil)
	authenticate(req, clientToken)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want 202; body=%s", rec.Code, rec.Body.String())
	}
	if got := rec.Header().Get("Cancel-Requested"); got != "true" {
		t.Errorf("Cancel-Requested = %q, want true", got)
	}
}

func TestCancelTerminalJobConflicts(t *testing.T) {
	tenantID := uuid.New()
	service := &fakeService{cancelErr: domain.ErrInvalidTransition}
	router := testRouter(service, fakePinger{}, 1<<20, testAuthenticator(tenantID))

	req := httptest.NewRequest(http.MethodPost, "/v1/jobs/"+uuid.New().String()+"/cancel", nil)
	authenticate(req, clientToken)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusConflict {
		t.Fatalf("status = %d, want 409", rec.Code)
	}
}

func TestSubmitQuotaExceeded(t *testing.T) {
	tenantID := uuid.New()
	service := &fakeService{submitErr: domain.ErrQuotaExceeded}
	router := testRouter(service, fakePinger{}, 1<<20, testAuthenticator(tenantID))

	req := httptest.NewRequest(http.MethodPost, "/v1/jobs", bytes.NewBufferString(
		`{"job_type":"report.generate","payload":{"report_id":"r-1"}}`,
	))
	authenticate(req, clientToken)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusTooManyRequests {
		t.Fatalf("status = %d, want 429; body=%s", rec.Code, rec.Body.String())
	}
	var response dto.ErrorResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &response); err != nil {
		t.Fatalf("decode error response: %v", err)
	}
	if response.Error.Code != "quota_exceeded" {
		t.Errorf("error code = %q, want quota_exceeded", response.Error.Code)
	}
}

func TestRateLimitIsPerTenant(t *testing.T) {
	tenantID := uuid.New()
	service := &fakeService{listResult: &jobs.ListResult{}}
	router := NewRouter(Options{
		JobService:         service,
		Pinger:             fakePinger{},
		Authenticator:      testAuthenticator(tenantID),
		Logger:             slog.New(slog.NewTextHandler(io.Discard, nil)),
		MaxBodyBytes:       1 << 20,
		RateLimitPerMinute: 2,
	})

	for i := 0; i < 2; i++ {
		req := httptest.NewRequest(http.MethodGet, "/v1/jobs", nil)
		authenticate(req, clientToken)
		rec := httptest.NewRecorder()
		router.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("request %d status = %d, want 200", i, rec.Code)
		}
	}

	req := httptest.NewRequest(http.MethodGet, "/v1/jobs", nil)
	authenticate(req, clientToken)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusTooManyRequests {
		t.Fatalf("over-limit status = %d, want 429; body=%s", rec.Code, rec.Body.String())
	}
	if rec.Header().Get("Retry-After") == "" {
		t.Error("Retry-After header is empty")
	}
	var response dto.ErrorResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &response); err != nil {
		t.Fatalf("decode error response: %v", err)
	}
	if response.Error.Code != "rate_limited" {
		t.Errorf("error code = %q, want rate_limited", response.Error.Code)
	}
}

func TestSubmissionRateLimitIsSeparateFromGeneralLimit(t *testing.T) {
	tenantID := uuid.New()
	service := &fakeService{
		submitJob:     sampleJob(tenantID),
		submitCreated: true,
		listResult:    &jobs.ListResult{},
	}
	router := NewRouter(Options{
		JobService:               service,
		Pinger:                   fakePinger{},
		Authenticator:            testAuthenticator(tenantID),
		Logger:                   slog.New(slog.NewTextHandler(io.Discard, nil)),
		MaxBodyBytes:             1 << 20,
		RateLimitPerMinute:       0, // disabled: only the submission budget applies
		SubmitRateLimitPerMinute: 2,
	})

	body := `{"job_type":"report.generate","payload":{"report_id":"r-1"}}`
	for i := 0; i < 2; i++ {
		req := httptest.NewRequest(http.MethodPost, "/v1/jobs", bytes.NewBufferString(body))
		authenticate(req, clientToken)
		rec := httptest.NewRecorder()
		router.ServeHTTP(rec, req)
		if rec.Code != http.StatusAccepted {
			t.Fatalf("submit %d status = %d, want 202; body=%s", i, rec.Code, rec.Body.String())
		}
	}

	req := httptest.NewRequest(http.MethodPost, "/v1/jobs", bytes.NewBufferString(body))
	authenticate(req, clientToken)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusTooManyRequests {
		t.Fatalf("over-limit submit status = %d, want 429; body=%s", rec.Code, rec.Body.String())
	}
	if rec.Header().Get("Retry-After") == "" {
		t.Error("Retry-After header is empty on submission rejection")
	}
	var response dto.ErrorResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &response); err != nil {
		t.Fatalf("decode error response: %v", err)
	}
	if response.Error.Code != "rate_limited" {
		t.Errorf("error code = %q, want rate_limited", response.Error.Code)
	}
	if !strings.Contains(response.Error.Message, "submission") {
		t.Errorf("error message = %q, want it to identify the submission budget", response.Error.Message)
	}

	// The exhausted submission budget must not block ordinary reads.
	readReq := httptest.NewRequest(http.MethodGet, "/v1/jobs", nil)
	authenticate(readReq, clientToken)
	readRec := httptest.NewRecorder()
	router.ServeHTTP(readRec, readReq)
	if readRec.Code != http.StatusOK {
		t.Errorf("GET after exhausted submit budget status = %d, want 200", readRec.Code)
	}
}

func TestSubmissionStillSharesGeneralLimit(t *testing.T) {
	tenantID := uuid.New()
	service := &fakeService{
		submitJob:     sampleJob(tenantID),
		submitCreated: true,
	}
	router := NewRouter(Options{
		JobService:               service,
		Pinger:                   fakePinger{},
		Authenticator:            testAuthenticator(tenantID),
		Logger:                   slog.New(slog.NewTextHandler(io.Discard, nil)),
		MaxBodyBytes:             1 << 20,
		RateLimitPerMinute:       1,
		SubmitRateLimitPerMinute: 100,
	})

	body := `{"job_type":"report.generate","payload":{"report_id":"r-1"}}`
	req := httptest.NewRequest(http.MethodPost, "/v1/jobs", bytes.NewBufferString(body))
	authenticate(req, clientToken)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusAccepted {
		t.Fatalf("first submit status = %d, want 202; body=%s", rec.Code, rec.Body.String())
	}

	req = httptest.NewRequest(http.MethodPost, "/v1/jobs", bytes.NewBufferString(body))
	authenticate(req, clientToken)
	rec = httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusTooManyRequests {
		t.Fatalf("second submit status = %d, want 429 from the general budget; body=%s", rec.Code, rec.Body.String())
	}
}

func TestReplayJob(t *testing.T) {
	tenantID := uuid.New()
	job := sampleJob(tenantID)
	job.Status = domain.StatusRetryWait
	job.Attempts = 0
	job.ReplayCount = 1
	service := &fakeService{replayJob: job}
	router := testRouter(service, fakePinger{}, 1<<20, testAuthenticator(tenantID))

	req := httptest.NewRequest(http.MethodPost, "/v1/jobs/"+job.ID.String()+"/replay", nil)
	authenticate(req, operatorToken)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body=%s", rec.Code, rec.Body.String())
	}
	if service.gotTenant != tenantID || service.gotID != job.ID {
		t.Errorf("service got %v/%v, want %v/%v", service.gotTenant, service.gotID, tenantID, job.ID)
	}
	if service.gotReplayedBy != "operator-key" {
		t.Errorf("replayed_by = %q, want the operator key name", service.gotReplayedBy)
	}

	var response dto.JobResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &response); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if response.Status != domain.StatusRetryWait || response.ReplayCount != 1 {
		t.Errorf("response = %+v, want retry_wait with replay_count 1", response)
	}
}

func TestReplayRequiresOperator(t *testing.T) {
	tenantID := uuid.New()
	job := sampleJob(tenantID)
	service := &fakeService{replayJob: job}
	router := testRouter(service, fakePinger{}, 1<<20, testAuthenticator(tenantID))

	req := httptest.NewRequest(http.MethodPost, "/v1/jobs/"+job.ID.String()+"/replay", nil)
	authenticate(req, clientToken)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusForbidden {
		t.Fatalf("status = %d, want 403; body=%s", rec.Code, rec.Body.String())
	}
	if service.gotID != uuid.Nil {
		t.Errorf("service was called for a forbidden replay: job %v", service.gotID)
	}

	var response dto.ErrorResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &response); err != nil {
		t.Fatalf("decode error response: %v", err)
	}
	if response.Error.Code != "forbidden" {
		t.Errorf("error code = %q, want forbidden", response.Error.Code)
	}
}

func TestReplayJobRejectsNonDeadJob(t *testing.T) {
	tenantID := uuid.New()
	job := sampleJob(tenantID)
	service := &fakeService{replayErr: domain.ErrInvalidTransition}
	router := testRouter(service, fakePinger{}, 1<<20, testAuthenticator(tenantID))

	req := httptest.NewRequest(http.MethodPost, "/v1/jobs/"+job.ID.String()+"/replay", nil)
	authenticate(req, operatorToken)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusConflict {
		t.Fatalf("status = %d, want 409; body=%s", rec.Code, rec.Body.String())
	}
	var response dto.ErrorResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &response); err != nil {
		t.Fatalf("decode error response: %v", err)
	}
	if response.Error.Code != "invalid_transition" {
		t.Errorf("error code = %q, want invalid_transition", response.Error.Code)
	}
}

func TestReplayJobNotFound(t *testing.T) {
	tenantID := uuid.New()
	service := &fakeService{replayErr: domain.ErrNotFound}
	router := testRouter(service, fakePinger{}, 1<<20, testAuthenticator(tenantID))

	req := httptest.NewRequest(http.MethodPost, "/v1/jobs/"+uuid.New().String()+"/replay", nil)
	authenticate(req, operatorToken)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", rec.Code)
	}
}

func TestReplayJobRejectsInvalidID(t *testing.T) {
	tenantID := uuid.New()
	router := testRouter(&fakeService{}, fakePinger{}, 1<<20, testAuthenticator(tenantID))
	req := httptest.NewRequest(http.MethodPost, "/v1/jobs/not-a-uuid/replay", nil)
	authenticate(req, operatorToken)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", rec.Code)
	}
}

func TestRequiresAuthentication(t *testing.T) {
	tenantID := uuid.New()
	router := testRouter(&fakeService{}, fakePinger{}, 1<<20, testAuthenticator(tenantID))
	req := httptest.NewRequest(http.MethodGet, "/v1/jobs/"+uuid.New().String(), nil)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want 401", rec.Code)
	}
	var response dto.ErrorResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &response); err != nil {
		t.Fatalf("decode error response: %v", err)
	}
	if response.Error.Code != "authentication_required" {
		t.Errorf("error code = %q, want authentication_required", response.Error.Code)
	}
}

func TestRejectsMalformedAuthorization(t *testing.T) {
	tenantID := uuid.New()
	router := testRouter(&fakeService{}, fakePinger{}, 1<<20, testAuthenticator(tenantID))

	for _, header := range []string{
		"Basic dXNlcjpwYXNz",
		"Bearer",
		"Bearer   ",
		"Token " + clientToken,
	} {
		req := httptest.NewRequest(http.MethodGet, "/v1/jobs/"+uuid.New().String(), nil)
		req.Header.Set("Authorization", header)
		rec := httptest.NewRecorder()
		router.ServeHTTP(rec, req)

		if rec.Code != http.StatusUnauthorized {
			t.Errorf("Authorization %q status = %d, want 401", header, rec.Code)
		}
	}
}

func TestRejectsInvalidAPIKey(t *testing.T) {
	tenantID := uuid.New()
	router := testRouter(&fakeService{}, fakePinger{}, 1<<20, testAuthenticator(tenantID))

	req := httptest.NewRequest(http.MethodGet, "/v1/jobs/"+uuid.New().String(), nil)
	authenticate(req, "djp_not-a-real-key")
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want 401; body=%s", rec.Code, rec.Body.String())
	}
	var response dto.ErrorResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &response); err != nil {
		t.Fatalf("decode error response: %v", err)
	}
	if response.Error.Code != "invalid_api_key" {
		t.Errorf("error code = %q, want invalid_api_key", response.Error.Code)
	}
}

func TestAuthenticationFailureReturnsInternalError(t *testing.T) {
	tenantID := uuid.New()
	authenticator := testAuthenticator(tenantID)
	authenticator.err = errors.New("database unavailable")
	router := testRouter(&fakeService{}, fakePinger{}, 1<<20, authenticator)

	req := httptest.NewRequest(http.MethodGet, "/v1/jobs/"+uuid.New().String(), nil)
	authenticate(req, clientToken)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want 500", rec.Code)
	}
}

func TestAuthenticationUnavailableFailsClosed(t *testing.T) {
	router := testRouter(&fakeService{}, fakePinger{}, 1<<20, nil)
	req := httptest.NewRequest(http.MethodGet, "/v1/jobs/"+uuid.New().String(), nil)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503", rec.Code)
	}
}

func TestRejectsInvalidJobID(t *testing.T) {
	tenantID := uuid.New()
	router := testRouter(&fakeService{}, fakePinger{}, 1<<20, testAuthenticator(tenantID))
	req := httptest.NewRequest(http.MethodGet, "/v1/jobs/not-a-uuid", nil)
	authenticate(req, clientToken)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", rec.Code)
	}
}

func TestRejectsInvalidJSON(t *testing.T) {
	tenantID := uuid.New()
	router := testRouter(&fakeService{}, fakePinger{}, 1<<20, testAuthenticator(tenantID))
	req := httptest.NewRequest(http.MethodPost, "/v1/jobs", bytes.NewBufferString(`{`))
	authenticate(req, clientToken)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", rec.Code)
	}
}

func TestRejectsUnknownJSONField(t *testing.T) {
	tenantID := uuid.New()
	router := testRouter(&fakeService{}, fakePinger{}, 1<<20, testAuthenticator(tenantID))
	req := httptest.NewRequest(http.MethodPost, "/v1/jobs", bytes.NewBufferString(
		`{"job_type":"report.generate","payload":{},"tenant_id":"attacker"}`,
	))
	authenticate(req, clientToken)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", rec.Code)
	}
}

func TestRejectsOversizedBody(t *testing.T) {
	tenantID := uuid.New()
	router := testRouter(&fakeService{}, fakePinger{}, 16, testAuthenticator(tenantID))
	req := httptest.NewRequest(http.MethodPost, "/v1/jobs", bytes.NewBufferString(
		`{"job_type":"report.generate","payload":{"report_id":"r-1"}}`,
	))
	authenticate(req, clientToken)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("status = %d, want 413", rec.Code)
	}
}

func TestHealthEndpointsArePublic(t *testing.T) {
	router := testRouter(&fakeService{}, fakePinger{}, 1<<20, testAuthenticator(uuid.New()))

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
	router := testRouter(&fakeService{}, fakePinger{err: errors.New("db down")}, 1<<20, testAuthenticator(uuid.New()))
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/health/ready", nil))
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503", rec.Code)
	}
}

func TestNotFound(t *testing.T) {
	router := testRouter(&fakeService{}, fakePinger{}, 1<<20, testAuthenticator(uuid.New()))
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/nope", nil))
	if rec.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", rec.Code)
	}
}
