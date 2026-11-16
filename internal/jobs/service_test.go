package jobs

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/C9b3rD3vi1/durable-job-processor/internal/domain"
	"github.com/google/uuid"
)

type fakeRepository struct {
	createCalls    int
	createJob      *domain.Job
	createNew      bool
	createErr      error
	gotJob         *domain.Job
	gotMaxOutstand int

	getCalls  int
	getJob    *domain.Job
	getErr    error
	gotTenant uuid.UUID
	gotID     uuid.UUID

	listCalls  int
	listJobs   []*domain.Job
	listErr    error
	gotFilter  ListFilter
	gotLimit   int
	gotListTen uuid.UUID

	cancelCalls  int
	cancelJob    *domain.Job
	cancelReq    bool
	cancelErr    error
	gotCancelTan uuid.UUID
	gotCancelID  uuid.UUID

	replayCalls   int
	replayJob     *domain.Job
	replayErr     error
	gotReplayedBy string
}

func (f *fakeRepository) CreateJob(_ context.Context, job *domain.Job, maxOutstanding int) (*domain.Job, bool, error) {
	f.createCalls++
	f.gotJob = job
	f.gotMaxOutstand = maxOutstanding
	if f.createErr != nil {
		return nil, false, f.createErr
	}
	if f.createJob != nil {
		return f.createJob, f.createNew, nil
	}
	return job, true, nil
}

func (f *fakeRepository) GetJobByID(_ context.Context, tenantID, id uuid.UUID) (*domain.Job, error) {
	f.getCalls++
	f.gotTenant = tenantID
	f.gotID = id
	return f.getJob, f.getErr
}

func (f *fakeRepository) ListJobs(_ context.Context, tenantID uuid.UUID, filter ListFilter, limit int) ([]*domain.Job, error) {
	f.listCalls++
	f.gotListTen = tenantID
	f.gotFilter = filter
	f.gotLimit = limit
	return f.listJobs, f.listErr
}

func (f *fakeRepository) CancelJob(_ context.Context, tenantID, id uuid.UUID) (*domain.Job, bool, error) {
	f.cancelCalls++
	f.gotCancelTan = tenantID
	f.gotCancelID = id
	return f.cancelJob, f.cancelReq, f.cancelErr
}

func (f *fakeRepository) ReplayJob(_ context.Context, tenantID, id uuid.UUID, replayedBy string) (*domain.Job, error) {
	f.replayCalls++
	f.gotTenant = tenantID
	f.gotID = id
	f.gotReplayedBy = replayedBy
	return f.replayJob, f.replayErr
}

func TestServiceSubmit(t *testing.T) {
	now := time.Date(2026, 11, 15, 12, 0, 0, 0, time.UTC)
	repo := &fakeRepository{}
	service := NewService(repo, testValidator())
	service.now = func() time.Time { return now }

	job, created, err := service.Submit(context.Background(), validInput())
	if err != nil {
		t.Fatalf("Submit() error = %v", err)
	}
	if !created {
		t.Error("created = false, want true")
	}
	if repo.createCalls != 1 {
		t.Errorf("CreateJob calls = %d, want 1", repo.createCalls)
	}
	if repo.gotJob == nil || repo.gotJob.Status != domain.StatusQueued {
		t.Errorf("stored job = %+v, want queued job", repo.gotJob)
	}
	if !job.RunAt.Equal(now) {
		t.Errorf("RunAt = %v, want %v", job.RunAt, now)
	}
}

func TestServiceSubmitIdempotentReplay(t *testing.T) {
	existing := &domain.Job{ID: uuid.New()}
	repo := &fakeRepository{createJob: existing, createNew: false}
	service := NewService(repo, testValidator())

	job, created, err := service.Submit(context.Background(), validInput())
	if err != nil {
		t.Fatalf("Submit() error = %v", err)
	}
	if created {
		t.Error("created = true, want false")
	}
	if job != existing {
		t.Error("Submit() should return the existing job")
	}
}

func TestServiceSubmitValidatesBeforeRepository(t *testing.T) {
	repo := &fakeRepository{}
	service := NewService(repo, testValidator())

	input := validInput()
	input.JobType = "unknown.job"
	_, _, err := service.Submit(context.Background(), input)
	if !errors.Is(err, domain.ErrInvalidJob) {
		t.Fatalf("Submit() error = %v, want ErrInvalidJob", err)
	}
	if repo.createCalls != 0 {
		t.Errorf("CreateJob calls = %d, want 0", repo.createCalls)
	}
}

func TestServiceSubmitPropagatesRepositoryError(t *testing.T) {
	wantErr := errors.New("database unavailable")
	repo := &fakeRepository{createErr: wantErr}
	service := NewService(repo, testValidator())

	_, _, err := service.Submit(context.Background(), validInput())
	if !errors.Is(err, wantErr) {
		t.Fatalf("Submit() error = %v, want %v", err, wantErr)
	}
}

func TestServiceGet(t *testing.T) {
	tenantID := uuid.New()
	jobID := uuid.New()
	want := &domain.Job{ID: jobID, TenantID: tenantID, Payload: json.RawMessage(`{}`)}
	repo := &fakeRepository{getJob: want}
	service := NewService(repo, testValidator())

	got, err := service.Get(context.Background(), tenantID, jobID)
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}
	if got != want {
		t.Error("Get() returned unexpected job")
	}
	if repo.gotTenant != tenantID || repo.gotID != jobID {
		t.Errorf("repository got tenant/id = %v/%v, want %v/%v", repo.gotTenant, repo.gotID, tenantID, jobID)
	}
}

func TestServiceGetRejectsInvalidIDs(t *testing.T) {
	service := NewService(&fakeRepository{}, testValidator())

	if _, err := service.Get(context.Background(), uuid.Nil, uuid.New()); !errors.Is(err, domain.ErrInvalidArgument) {
		t.Errorf("nil tenant error = %v, want ErrInvalidArgument", err)
	}
	if _, err := service.Get(context.Background(), uuid.New(), uuid.Nil); !errors.Is(err, domain.ErrInvalidArgument) {
		t.Errorf("nil job error = %v, want ErrInvalidArgument", err)
	}
}

func TestServiceGetPropagatesNotFound(t *testing.T) {
	repo := &fakeRepository{getErr: domain.ErrNotFound}
	service := NewService(repo, testValidator())

	_, err := service.Get(context.Background(), uuid.New(), uuid.New())
	if !errors.Is(err, domain.ErrNotFound) {
		t.Fatalf("Get() error = %v, want ErrNotFound", err)
	}
}

func TestServiceReplay(t *testing.T) {
	tenantID := uuid.New()
	jobID := uuid.New()
	want := &domain.Job{
		ID:          jobID,
		TenantID:    tenantID,
		Status:      domain.StatusRetryWait,
		ReplayCount: 1,
	}
	repo := &fakeRepository{replayJob: want}
	service := NewService(repo, testValidator())

	got, err := service.Replay(context.Background(), tenantID, jobID, "operator-1")
	if err != nil {
		t.Fatalf("Replay() error = %v", err)
	}
	if got != want {
		t.Error("Replay() returned unexpected job")
	}
	if repo.replayCalls != 1 {
		t.Errorf("ReplayJob calls = %d, want 1", repo.replayCalls)
	}
	if repo.gotTenant != tenantID || repo.gotID != jobID {
		t.Errorf("repository got tenant/id = %v/%v, want %v/%v", repo.gotTenant, repo.gotID, tenantID, jobID)
	}
	if repo.gotReplayedBy != "operator-1" {
		t.Errorf("replayed_by = %q, want operator-1", repo.gotReplayedBy)
	}
}

func TestServiceReplayRejectsInvalidInput(t *testing.T) {
	repo := &fakeRepository{}
	service := NewService(repo, testValidator())

	tests := []struct {
		name       string
		tenantID   uuid.UUID
		id         uuid.UUID
		replayedBy string
	}{
		{"nil tenant", uuid.Nil, uuid.New(), "operator"},
		{"nil job", uuid.New(), uuid.Nil, "operator"},
		{"blank actor", uuid.New(), uuid.New(), "   "},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := service.Replay(context.Background(), tt.tenantID, tt.id, tt.replayedBy); !errors.Is(err, domain.ErrInvalidArgument) {
				t.Errorf("Replay() error = %v, want ErrInvalidArgument", err)
			}
		})
	}
	if repo.replayCalls != 0 {
		t.Errorf("invalid calls reached the repository %d times", repo.replayCalls)
	}
}

func TestServiceReplayPropagatesInvalidTransition(t *testing.T) {
	repo := &fakeRepository{replayErr: domain.ErrInvalidTransition}
	service := NewService(repo, testValidator())

	_, err := service.Replay(context.Background(), uuid.New(), uuid.New(), "operator")
	if !errors.Is(err, domain.ErrInvalidTransition) {
		t.Fatalf("Replay() error = %v, want ErrInvalidTransition", err)
	}
}
