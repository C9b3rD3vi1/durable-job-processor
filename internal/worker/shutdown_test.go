package worker

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/C9b3rD3vi1/durable-job-processor/internal/domain"
	"github.com/C9b3rD3vi1/durable-job-processor/internal/handlers"
	"github.com/C9b3rD3vi1/durable-job-processor/internal/observability"
	"github.com/google/uuid"
)

func TestInvokeHandlerBoundedReturnsWhenHandlerIgnoresCancellation(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	defer close(release)

	handler := handlers.HandlerFunc(func(context.Context, domain.Job) (json.RawMessage, error) {
		close(started)
		<-release
		return json.RawMessage(`{"unexpected":true}`), nil
	})

	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() {
		_, err := invokeHandlerBounded(ctx, handler, domain.Job{})
		result <- err
	}()

	<-started
	cancel()

	select {
	case err := <-result:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("invokeHandlerBounded() error = %v, want context.Canceled", err)
		}
	case <-time.After(time.Second):
		t.Fatal("invokeHandlerBounded() blocked on an uncooperative handler")
	}
}

func TestInvokeHandlerBoundedDistinguishesHandlerDeadlineFromRunnerTimeout(t *testing.T) {
	handler := handlers.HandlerFunc(func(context.Context, domain.Job) (json.RawMessage, error) {
		return nil, context.DeadlineExceeded
	})

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	_, err, timedOut := invokeHandlerBoundedResult(ctx, handler, domain.Job{})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("invokeHandlerBoundedResult() error = %v, want DeadlineExceeded", err)
	}
	if timedOut {
		t.Error("timedOut = true, want false when the handler returned before the runner deadline")
	}
}

func TestRunHardShutdownDoesNotWaitForUncooperativeHandler(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	defer close(release)

	registry := handlers.New()
	if err := registry.Register("blocking", handlers.HandlerFunc(func(context.Context, domain.Job) (json.RawMessage, error) {
		close(started)
		<-release
		return json.RawMessage(`{}`), nil
	})); err != nil {
		t.Fatalf("register handler: %v", err)
	}

	job := &domain.Job{
		ID:          uuid.New(),
		TenantID:    uuid.New(),
		JobType:     "blocking",
		Payload:     json.RawMessage(`{}`),
		Status:      domain.StatusQueued,
		Attempts:    0,
		MaxAttempts: 5,
		CreatedAt:   time.Now(),
	}
	store := newFakeStore(job)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	runner, err := New(Config{
		Store:           store,
		Registry:        registry,
		Logger:          logger,
		Metrics:         observability.NewMetrics(),
		WorkerID:        "hard-shutdown-test",
		Concurrency:     1,
		PollInterval:    10 * time.Millisecond,
		ShutdownTimeout: 50 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- runner.Run(ctx) }()

	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("handler was not started")
	}
	cancel()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run() error = %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Run() blocked during forced shutdown")
	}

	store.mu.Lock()
	defer store.mu.Unlock()
	record, ok := store.failed[job.ID]
	if !ok {
		t.Fatal("interrupted job was not recorded for retry")
	}
	if !strings.Contains(record.cause, "context canceled") {
		t.Errorf("failure cause = %q, want context cancellation", record.cause)
	}
}

type blockingFailStore struct {
	*fakeStore
	job     *domain.Job
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func (s *blockingFailStore) ClaimJob(context.Context, string, time.Duration) (*domain.Job, error) {
	return s.job, nil
}

func (s *blockingFailStore) FailJob(context.Context, uuid.UUID, uuid.UUID, string, time.Duration) (bool, error) {
	s.once.Do(func() { close(s.entered) })
	<-s.release
	return true, nil
}

func TestRunFinalGraceExpiryReportsIncompleteShutdown(t *testing.T) {
	started := make(chan struct{})
	releaseHandler := make(chan struct{})
	defer close(releaseHandler)

	registry := handlers.New()
	if err := registry.Register("blocking", handlers.HandlerFunc(func(context.Context, domain.Job) (json.RawMessage, error) {
		close(started)
		<-releaseHandler
		return json.RawMessage(`{}`), nil
	})); err != nil {
		t.Fatalf("register handler: %v", err)
	}

	leaseToken := uuid.New()
	job := &domain.Job{
		ID:          uuid.New(),
		TenantID:    uuid.New(),
		JobType:     "blocking",
		Payload:     json.RawMessage(`{}`),
		Status:      domain.StatusQueued,
		Attempts:    1,
		MaxAttempts: 5,
		CreatedAt:   time.Now(),
		LeaseToken:  &leaseToken,
	}
	store := &blockingFailStore{
		fakeStore: newFakeStore(job),
		job:       job,
		entered:   make(chan struct{}),
		release:   make(chan struct{}),
	}
	defer close(store.release)

	runner, err := New(Config{
		Store:           store,
		Registry:        registry,
		Logger:          slog.New(slog.NewTextHandler(io.Discard, nil)),
		Metrics:         observability.NewMetrics(),
		WorkerID:        "final-grace-test",
		Concurrency:     1,
		PollInterval:    10 * time.Millisecond,
		ShutdownTimeout: 25 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	runner.persistTimeout = 100 * time.Millisecond

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- runner.Run(ctx) }()

	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("handler was not started")
	}
	cancel()

	select {
	case err := <-done:
		if !errors.Is(err, ErrShutdownIncomplete) {
			t.Fatalf("Run() error = %v, want ErrShutdownIncomplete", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Run() blocked after the final grace expired")
	}

	select {
	case <-store.entered:
	default:
		t.Fatal("failure persistence was not attempted")
	}
}
