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
	createCalls int
	createJob   *domain.Job
	createNew   bool
	createErr   error
	gotJob      *domain.Job

	getCalls  int
	getJob    *domain.Job
	getErr    error
	gotTenant uuid.UUID
	gotID     uuid.UUID
}

func (f *fakeRepository) CreateJob(_ context.Context, job *domain.Job) (*domain.Job, bool, error) {
	f.createCalls++
	f.gotJob = job
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
