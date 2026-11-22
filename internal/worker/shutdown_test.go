package worker

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"strings"
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
