package jobs

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/C9b3rD3vi1/durable-job-processor/internal/domain"
	"github.com/google/uuid"
)

func TestServiceListDefaultsLimitAndCursor(t *testing.T) {
	tenantID := uuid.New()
	now := time.Date(2026, 11, 16, 12, 0, 0, 0, time.UTC)
	first := &domain.Job{ID: uuid.New(), CreatedAt: now}
	second := &domain.Job{ID: uuid.New(), CreatedAt: now.Add(-time.Minute)}
	third := &domain.Job{ID: uuid.New(), CreatedAt: now.Add(-2 * time.Minute)}

	repo := &fakeRepository{listJobs: []*domain.Job{first, second, third}}
	service := NewService(repo, testValidator())
	actor := Actor{TenantID: tenantID, KeyID: uuid.New()}

	result, err := service.List(context.Background(), actor, ListInput{})
	if err != nil {
		t.Fatalf("List() error = %v", err)
	}
	if repo.gotLimit != DefaultListLimit+1 {
		t.Errorf("repository limit = %d, want %d", repo.gotLimit, DefaultListLimit+1)
	}
	if len(result.Jobs) != 3 || result.NextCursor != "" {
		t.Errorf("result = %d jobs, cursor %q; want 3 jobs and no cursor", len(result.Jobs), result.NextCursor)
	}
	if repo.gotListTen != tenantID {
		t.Errorf("tenant = %v, want %v", repo.gotListTen, tenantID)
	}

	repo.listJobs = []*domain.Job{first, second, third}
	result, err = service.List(context.Background(), actor, ListInput{Limit: 2})
	if err != nil {
		t.Fatalf("List() error = %v", err)
	}
	if len(result.Jobs) != 2 || result.NextCursor == "" {
		t.Fatalf("result = %d jobs, cursor %q; want 2 jobs and a cursor", len(result.Jobs), result.NextCursor)
	}
	if repo.gotLimit != 3 {
		t.Errorf("repository limit = %d, want 3", repo.gotLimit)
	}

	cursor, err := decodeCursor(result.NextCursor)
	if err != nil {
		t.Fatalf("decode cursor: %v", err)
	}
	if !cursor.CreatedAt.Equal(second.CreatedAt) || cursor.ID != second.ID {
		t.Errorf("cursor = %+v, want second job %v", cursor, second.ID)
	}
}

func TestServiceListPassesFiltersAndCursor(t *testing.T) {
	tenantID := uuid.New()
	status := domain.StatusDead
	after := time.Date(2026, 11, 1, 0, 0, 0, 0, time.UTC)
	before := time.Date(2026, 11, 16, 0, 0, 0, 0, time.UTC)
	cursorTime := time.Date(2026, 11, 10, 0, 0, 0, 0, time.UTC)
	cursorID := uuid.New()

	repo := &fakeRepository{}
	service := NewService(repo, testValidator())

	_, err := service.List(context.Background(), Actor{TenantID: tenantID, KeyID: uuid.New()}, ListInput{
		Status:        &status,
		JobType:       " report.generate ",
		CreatedAfter:  &after,
		CreatedBefore: &before,
		Cursor:        encodeCursor(cursorTime, cursorID),
		Limit:         25,
	})
	if err != nil {
		t.Fatalf("List() error = %v", err)
	}
	if repo.gotFilter.Status == nil || *repo.gotFilter.Status != status {
		t.Errorf("status filter = %v, want %v", repo.gotFilter.Status, status)
	}
	if repo.gotFilter.JobType != "report.generate" {
		t.Errorf("job_type filter = %q, want report.generate", repo.gotFilter.JobType)
	}
	if repo.gotFilter.CreatedAfter == nil || !repo.gotFilter.CreatedAfter.Equal(after) {
		t.Errorf("created_after = %v, want %v", repo.gotFilter.CreatedAfter, after)
	}
	if repo.gotFilter.CreatedBefore == nil || !repo.gotFilter.CreatedBefore.Equal(before) {
		t.Errorf("created_before = %v, want %v", repo.gotFilter.CreatedBefore, before)
	}
	if repo.gotFilter.AfterID == nil || *repo.gotFilter.AfterID != cursorID {
		t.Errorf("cursor id = %v, want %v", repo.gotFilter.AfterID, cursorID)
	}
	if repo.gotFilter.AfterCreated == nil || !repo.gotFilter.AfterCreated.Equal(cursorTime) {
		t.Errorf("cursor time = %v, want %v", repo.gotFilter.AfterCreated, cursorTime)
	}
	if repo.gotLimit != 26 {
		t.Errorf("repository limit = %d, want 26", repo.gotLimit)
	}
}

func TestServiceListRejectsInvalidInput(t *testing.T) {
	service := NewService(&fakeRepository{}, testValidator())
	tenantID := uuid.New()
	now := time.Now()
	status := domain.JobStatus("bogus")

	tests := []struct {
		name   string
		tenant uuid.UUID
		in     ListInput
	}{
		{"nil tenant", uuid.Nil, ListInput{}},
		{"limit too small", tenantID, ListInput{Limit: -1}},
		{"limit too large", tenantID, ListInput{Limit: MaxListLimit + 1}},
		{"invalid status", tenantID, ListInput{Status: &status}},
		{"invalid cursor", tenantID, ListInput{Cursor: "not-a-cursor"}},
		{"reversed time range", tenantID, ListInput{CreatedAfter: &now, CreatedBefore: timePointer(now.Add(-time.Minute))}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			actor := Actor{TenantID: tt.tenant, KeyID: uuid.New()}
			if _, err := service.List(context.Background(), actor, tt.in); !errors.Is(err, domain.ErrInvalidArgument) {
				t.Errorf("List() error = %v, want ErrInvalidArgument", err)
			}
		})
	}
}

func TestCursorRoundTrip(t *testing.T) {
	createdAt := time.Date(2026, 11, 16, 12, 34, 56, 123456789, time.FixedZone("UTC+2", 2*60*60))
	id := uuid.New()

	decoded, err := decodeCursor(encodeCursor(createdAt, id))
	if err != nil {
		t.Fatalf("decodeCursor() error = %v", err)
	}
	if !decoded.CreatedAt.Equal(createdAt) {
		t.Errorf("created_at = %v, want %v", decoded.CreatedAt, createdAt)
	}
	if decoded.ID != id {
		t.Errorf("id = %v, want %v", decoded.ID, id)
	}
}

func TestServiceCancel(t *testing.T) {
	tenantID := uuid.New()
	jobID := uuid.New()
	owner := uuid.New()
	want := &domain.Job{ID: jobID, TenantID: tenantID, Status: domain.StatusRunning, CreatedByKeyID: &owner}
	repo := &fakeRepository{cancelJob: want, cancelReq: true, getJob: want}
	service := NewService(repo, testValidator())

	got, requested, err := service.Cancel(context.Background(), Actor{TenantID: tenantID, KeyID: owner}, jobID)
	if err != nil {
		t.Fatalf("Cancel() error = %v", err)
	}
	if got != want || !requested {
		t.Errorf("Cancel() = %v, %v; want job and cooperative request", got, requested)
	}
	if repo.cancelCalls != 1 {
		t.Errorf("CancelJob calls = %d, want 1", repo.cancelCalls)
	}
	if repo.gotCancelTan != tenantID || repo.gotCancelID != jobID {
		t.Errorf("repository got tenant/id = %v/%v, want %v/%v", repo.gotCancelTan, repo.gotCancelID, tenantID, jobID)
	}
}

func TestServiceCancelRejectsInvalidIDs(t *testing.T) {
	repo := &fakeRepository{}
	service := NewService(repo, testValidator())

	if _, _, err := service.Cancel(context.Background(), Actor{KeyID: uuid.New()}, uuid.New()); !errors.Is(err, domain.ErrInvalidArgument) {
		t.Errorf("nil tenant error = %v, want ErrInvalidArgument", err)
	}
	if _, _, err := service.Cancel(context.Background(), Actor{TenantID: uuid.New()}, uuid.New()); !errors.Is(err, domain.ErrInvalidArgument) {
		t.Errorf("nil key error = %v, want ErrInvalidArgument", err)
	}
	if _, _, err := service.Cancel(context.Background(), Actor{TenantID: uuid.New(), KeyID: uuid.New()}, uuid.Nil); !errors.Is(err, domain.ErrInvalidArgument) {
		t.Errorf("nil job error = %v, want ErrInvalidArgument", err)
	}
	if repo.cancelCalls != 0 {
		t.Errorf("invalid calls reached the repository %d times", repo.cancelCalls)
	}
}

func TestServiceSubmitPassesQuota(t *testing.T) {
	repo := &fakeRepository{}
	service := NewService(repo, testValidator(), WithMaxOutstandingJobs(7))

	if _, _, err := service.Submit(context.Background(), validInput()); err != nil {
		t.Fatalf("Submit() error = %v", err)
	}
	if repo.gotMaxOutstand != 7 {
		t.Errorf("max outstanding = %d, want 7", repo.gotMaxOutstand)
	}
}

func TestServiceSubmitPropagatesQuotaExceeded(t *testing.T) {
	repo := &fakeRepository{createErr: domain.ErrQuotaExceeded}
	service := NewService(repo, testValidator())

	_, _, err := service.Submit(context.Background(), validInput())
	if !errors.Is(err, domain.ErrQuotaExceeded) {
		t.Fatalf("Submit() error = %v, want ErrQuotaExceeded", err)
	}
}

func TestServiceGetEnforcesOwnerOrOperator(t *testing.T) {
	tenantID := uuid.New()
	owner := uuid.New()
	job := &domain.Job{ID: uuid.New(), TenantID: tenantID, CreatedByKeyID: &owner}
	service := NewService(&fakeRepository{getJob: job}, testValidator())

	if _, err := service.Get(context.Background(), Actor{TenantID: tenantID, KeyID: owner}, job.ID); err != nil {
		t.Errorf("owner Get() error = %v, want nil", err)
	}
	if _, err := service.Get(context.Background(), Actor{TenantID: tenantID, KeyID: uuid.New(), IsOperator: true}, job.ID); err != nil {
		t.Errorf("operator Get() error = %v, want nil", err)
	}
	_, err := service.Get(context.Background(), Actor{TenantID: tenantID, KeyID: uuid.New()}, job.ID)
	if !errors.Is(err, domain.ErrNotFound) {
		t.Errorf("other client Get() error = %v, want ErrNotFound", err)
	}
}

func TestServiceListSetsOwnerFilter(t *testing.T) {
	tenantID := uuid.New()
	owner := uuid.New()
	repo := &fakeRepository{}
	service := NewService(repo, testValidator())

	if _, err := service.List(context.Background(), Actor{TenantID: tenantID, KeyID: owner}, ListInput{}); err != nil {
		t.Fatalf("client List() error = %v", err)
	}
	if repo.gotFilter.OwnerKeyID == nil || *repo.gotFilter.OwnerKeyID != owner || repo.gotFilter.AnyOwner {
		t.Errorf("client filter = %+v, want owner %v", repo.gotFilter, owner)
	}

	if _, err := service.List(context.Background(), Actor{TenantID: tenantID, KeyID: uuid.New(), IsOperator: true}, ListInput{}); err != nil {
		t.Fatalf("operator List() error = %v", err)
	}
	if !repo.gotFilter.AnyOwner || repo.gotFilter.OwnerKeyID != nil {
		t.Errorf("operator filter = %+v, want unfiltered owner", repo.gotFilter)
	}
}

func TestServiceCancelEnforcesOwnerOrOperator(t *testing.T) {
	tenantID := uuid.New()
	owner := uuid.New()
	job := &domain.Job{ID: uuid.New(), TenantID: tenantID, Status: domain.StatusQueued, CreatedByKeyID: &owner}
	repo := &fakeRepository{getJob: job, cancelJob: job}
	service := NewService(repo, testValidator())
	other := Actor{TenantID: tenantID, KeyID: uuid.New()}

	if _, _, err := service.Cancel(context.Background(), other, job.ID); !errors.Is(err, domain.ErrNotFound) {
		t.Errorf("other client Cancel() error = %v, want ErrNotFound", err)
	}
	if repo.cancelCalls != 0 {
		t.Errorf("unauthorized cancel reached the repository %d times", repo.cancelCalls)
	}

	operator := Actor{TenantID: tenantID, KeyID: uuid.New(), IsOperator: true}
	if _, _, err := service.Cancel(context.Background(), operator, job.ID); err != nil {
		t.Errorf("operator Cancel() error = %v", err)
	}
	if repo.cancelCalls != 1 {
		t.Errorf("CancelJob calls = %d, want 1", repo.cancelCalls)
	}
}

func timePointer(value time.Time) *time.Time { return &value }
