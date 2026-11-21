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
	"github.com/C9b3rD3vi1/durable-job-processor/internal/queue"
	"github.com/C9b3rD3vi1/durable-job-processor/internal/retry"
	"github.com/google/uuid"
)

type failureRecord struct {
	cause string
	delay time.Duration
}

type fakeStore struct {
	mu sync.Mutex

	jobs       []*domain.Job
	claimCalls int
	claimErr   error
	claimed    []uuid.UUID

	completed   map[uuid.UUID]json.RawMessage
	failed      map[uuid.UUID]failureRecord
	completeOK  bool
	failOK      bool
	completeErr error
	failErr     error

	heartbeatCalls  int
	heartbeatOK     bool
	heartbeatCancel bool
	heartbeatErr    error

	cancelOK      bool
	cancelCalls   int
	cancelErr     error
	cancelledJobs map[uuid.UUID]bool

	recoveryCalls  int
	recoveryResult queue.RecoveryResult
	recoveryErr    error
}

func newFakeStore(jobs ...*domain.Job) *fakeStore {
	return &fakeStore{
		jobs:          jobs,
		completed:     make(map[uuid.UUID]json.RawMessage),
		failed:        make(map[uuid.UUID]failureRecord),
		cancelledJobs: make(map[uuid.UUID]bool),
		completeOK:    true,
		failOK:        true,
		heartbeatOK:   true,
		cancelOK:      true,
	}
}

func (s *fakeStore) ClaimJob(_ context.Context, _ string, _ time.Duration) (*domain.Job, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.claimCalls++
	if s.claimErr != nil {
		return nil, s.claimErr
	}
	if len(s.jobs) == 0 {
		return nil, nil
	}

	job := s.jobs[0]
	s.jobs = s.jobs[1:]
	token := uuid.New()
	job.LeaseToken = &token
	job.Status = domain.StatusRunning
	job.Attempts++
	s.claimed = append(s.claimed, job.ID)
	return job, nil
}

func (s *fakeStore) CompleteJob(_ context.Context, id, _ uuid.UUID, result json.RawMessage) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.completeErr != nil {
		return false, s.completeErr
	}
	if !s.completeOK {
		return false, nil
	}
	s.completed[id] = append(json.RawMessage(nil), result...)
	return true, nil
}

func (s *fakeStore) FailJob(_ context.Context, id, _ uuid.UUID, cause string, delay time.Duration) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.failErr != nil {
		return false, s.failErr
	}
	if !s.failOK {
		return false, nil
	}
	s.failed[id] = failureRecord{cause: cause, delay: delay}
	return true, nil
}

func (s *fakeStore) HeartbeatJob(_ context.Context, _, _ uuid.UUID, _ time.Duration) (queue.HeartbeatResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.heartbeatErr != nil {
		return queue.HeartbeatResult{}, s.heartbeatErr
	}
	s.heartbeatCalls++
	if !s.heartbeatOK {
		return queue.HeartbeatResult{Renewed: false}, nil
	}
	return queue.HeartbeatResult{Renewed: true, CancelRequested: s.heartbeatCancel}, nil
}

func (s *fakeStore) CancelRunningJob(_ context.Context, id, _ uuid.UUID) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.cancelCalls++
	if s.cancelErr != nil {
		return false, s.cancelErr
	}
	if !s.cancelOK {
		return false, nil
	}
	s.cancelledJobs[id] = true
	return true, nil
}

func (s *fakeStore) RecoverExpiredJobs(_ context.Context, _ int) (queue.RecoveryResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.recoveryCalls++
	if s.recoveryErr != nil {
		return queue.RecoveryResult{}, s.recoveryErr
	}
	return s.recoveryResult, nil
}

func (s *fakeStore) QueueStats(context.Context) (queue.Stats, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return queue.Stats{Depth: int64(len(s.jobs)), OldestDueSeconds: 1.5}, nil
}

func (s *fakeStore) heartbeatCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.heartbeatCalls
}

func (s *fakeStore) recoveryCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.recoveryCalls
}

func (s *fakeStore) completedCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.completed)
}

func (s *fakeStore) claimedCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.claimed)
}

func (s *fakeStore) firstClaimed() uuid.UUID {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.claimed) == 0 {
		return uuid.Nil
	}
	return s.claimed[0]
}

func (s *fakeStore) failedCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.failed)
}

func (s *fakeStore) cancelledCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.cancelledJobs)
}

func (s *fakeStore) cancelCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.cancelCalls
}

func (s *fakeStore) claimCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.claimCalls
}

func (s *fakeStore) completion(id uuid.UUID) (json.RawMessage, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	result, ok := s.completed[id]
	return result, ok
}

func (s *fakeStore) failure(id uuid.UUID) (failureRecord, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	record, ok := s.failed[id]
	return record, ok
}

func testJob(jobType string, attempts, maxAttempts int) *domain.Job {
	token := uuid.New()
	return &domain.Job{
		ID:          uuid.New(),
		TenantID:    uuid.New(),
		JobType:     jobType,
		Payload:     json.RawMessage(`{"value":"test"}`),
		Status:      domain.StatusRunning,
		Attempts:    attempts,
		MaxAttempts: maxAttempts,
		LeaseToken:  &token,
	}
}

func registryWith(t *testing.T, jobType string, handler handlers.Handler) *handlers.Registry {
	t.Helper()
	registry := handlers.New()
	if err := registry.Register(jobType, handler); err != nil {
		t.Fatalf("register handler: %v", err)
	}
	return registry
}

func newRunner(t *testing.T, store queue.Store, registry *handlers.Registry, mutate func(*Config)) *Runner {
	t.Helper()
	cfg := Config{
		Store:           store,
		Registry:        registry,
		Logger:          slog.New(slog.NewTextHandler(io.Discard, nil)),
		WorkerID:        "test-worker",
		Concurrency:     1,
		PollInterval:    time.Millisecond,
		LeaseDuration:   time.Minute,
		HandlerTimeout:  time.Second,
		RetryBaseDelay:  time.Second,
		ShutdownTimeout: time.Second,
	}
	if mutate != nil {
		mutate(&cfg)
	}
	runner, err := New(cfg)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	return runner
}

func waitFor(t *testing.T, timeout time.Duration, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("condition was not met before timeout")
}

func stopRunner(t *testing.T, cancel context.CancelFunc, done <-chan error) {
	t.Helper()
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run() error = %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run() did not return after cancellation")
	}
}

func TestNewDefaultsAndValidation(t *testing.T) {
	store := newFakeStore()
	registry := handlers.DefaultRegistry()

	runner, err := New(Config{Store: store, Registry: registry, WorkerID: "w-1"})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	if runner.concurrency != defaultConcurrency {
		t.Errorf("concurrency = %d, want %d", runner.concurrency, defaultConcurrency)
	}
	if runner.pollInterval != defaultPollInterval {
		t.Errorf("poll interval = %v, want %v", runner.pollInterval, defaultPollInterval)
	}
	if runner.handlerTimeout != defaultHandlerTimeout {
		t.Errorf("handler timeout = %v, want %v", runner.handlerTimeout, defaultHandlerTimeout)
	}
	if runner.heartbeatInterval != defaultHeartbeatInterval {
		t.Errorf("heartbeat interval = %v, want %v", runner.heartbeatInterval, defaultHeartbeatInterval)
	}
	if runner.retryBaseDelay != defaultRetryBaseDelay {
		t.Errorf("retry base delay = %v, want %v", runner.retryBaseDelay, defaultRetryBaseDelay)
	}
	if runner.retryPolicy.MaxDelay != defaultRetryMaxDelay {
		t.Errorf("retry max delay = %v, want %v", runner.retryPolicy.MaxDelay, defaultRetryMaxDelay)
	}
	if runner.recoveryInterval != defaultRecoveryInterval {
		t.Errorf("recovery interval = %v, want %v", runner.recoveryInterval, defaultRecoveryInterval)
	}

	tests := []struct {
		name string
		cfg  Config
	}{
		{"missing store", Config{Registry: registry, WorkerID: "w"}},
		{"missing registry", Config{Store: store, WorkerID: "w"}},
		{"missing worker id", Config{Store: store, Registry: registry}},
		{
			"heartbeat interval not less than lease",
			Config{
				Store: store, Registry: registry, WorkerID: "w",
				LeaseDuration: time.Second, HeartbeatInterval: time.Second,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := New(tt.cfg); err == nil {
				t.Fatal("New() error = nil, want error")
			}
		})
	}
}

func TestProcessSuccess(t *testing.T) {
	store := newFakeStore()
	registry := registryWith(t, "test.ok", handlers.HandlerFunc(func(context.Context, domain.Job) (json.RawMessage, error) {
		return json.RawMessage(`{"status":"done"}`), nil
	}))
	runner := newRunner(t, store, registry, nil)
	job := testJob("test.ok", 1, 3)

	runner.process(context.Background(), job)

	result, ok := store.completion(job.ID)
	if !ok {
		t.Fatal("job was not completed")
	}
	if string(result) != `{"status":"done"}` {
		t.Errorf("result = %s, want {\"status\":\"done\"}", result)
	}
	if _, failed := store.failure(job.ID); failed {
		t.Error("successful job was recorded as failed")
	}
}

func TestProcessHandlerErrorSchedulesRetry(t *testing.T) {
	store := newFakeStore()
	registry := registryWith(t, "test.fail", handlers.HandlerFunc(func(context.Context, domain.Job) (json.RawMessage, error) {
		return nil, errors.New("boom")
	}))
	runner := newRunner(t, store, registry, func(cfg *Config) {
		cfg.RetryBaseDelay = 2 * time.Second
	})
	runner.jitter = func() float64 { return 1 }
	job := testJob("test.fail", 1, 3)

	runner.process(context.Background(), job)

	record, ok := store.failure(job.ID)
	if !ok {
		t.Fatal("job failure was not recorded")
	}
	if record.delay != 2*time.Second {
		t.Errorf("retry delay = %v, want 2s", record.delay)
	}
	if record.cause != "boom" {
		t.Errorf("cause = %q, want boom", record.cause)
	}
	if _, completed := store.completion(job.ID); completed {
		t.Error("failed job was recorded as completed")
	}
}

func TestProcessRenewsLeaseWhileHandlerRuns(t *testing.T) {
	store := newFakeStore()
	registry := registryWith(t, "test.slow", handlers.HandlerFunc(func(context.Context, domain.Job) (json.RawMessage, error) {
		time.Sleep(40 * time.Millisecond)
		return json.RawMessage(`{"ok":true}`), nil
	}))
	runner := newRunner(t, store, registry, func(cfg *Config) {
		cfg.LeaseDuration = time.Second
		cfg.HeartbeatInterval = 5 * time.Millisecond
		cfg.HandlerTimeout = time.Second
	})

	runner.process(context.Background(), testJob("test.slow", 1, 3))

	if store.heartbeatCount() == 0 {
		t.Error("running handler did not renew its lease")
	}
	if store.completedCount() != 1 {
		t.Errorf("completed %d jobs, want 1", store.completedCount())
	}
}

func TestProcessStopsWhenLeaseIsLost(t *testing.T) {
	store := newFakeStore()
	store.heartbeatOK = false

	started := make(chan struct{})
	registry := registryWith(t, "test.lost", handlers.HandlerFunc(func(ctx context.Context, _ domain.Job) (json.RawMessage, error) {
		close(started)
		<-ctx.Done()
		return nil, ctx.Err()
	}))
	runner := newRunner(t, store, registry, func(cfg *Config) {
		cfg.LeaseDuration = time.Second
		cfg.HeartbeatInterval = 5 * time.Millisecond
		cfg.HandlerTimeout = 2 * time.Second
	})

	done := make(chan struct{})
	go func() {
		runner.process(context.Background(), testJob("test.lost", 1, 3))
		close(done)
	}()

	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("handler did not start")
	}
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("process did not stop after losing the lease")
	}

	if store.heartbeatCount() == 0 {
		t.Error("lease ownership was never checked")
	}
	if store.completedCount() != 0 || store.failedCount() != 0 {
		t.Error("a worker that lost its lease wrote job state")
	}
}

func TestProcessHonorsCancellationRequest(t *testing.T) {
	store := newFakeStore()
	store.heartbeatCancel = true

	started := make(chan struct{})
	registry := registryWith(t, "test.cancel", handlers.HandlerFunc(func(ctx context.Context, _ domain.Job) (json.RawMessage, error) {
		close(started)
		<-ctx.Done()
		return nil, ctx.Err()
	}))
	runner := newRunner(t, store, registry, func(cfg *Config) {
		cfg.LeaseDuration = time.Second
		cfg.HeartbeatInterval = 5 * time.Millisecond
		cfg.HandlerTimeout = 2 * time.Second
	})

	done := make(chan struct{})
	go func() {
		runner.process(context.Background(), testJob("test.cancel", 1, 3))
		close(done)
	}()

	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("handler did not start")
	}
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("process did not stop after cancellation was requested")
	}

	if store.cancelledCount() != 1 {
		t.Errorf("cancelled %d jobs, want 1", store.cancelledCount())
	}
	if store.completedCount() != 0 || store.failedCount() != 0 {
		t.Error("a cancelled job was recorded as completed or failed")
	}
}

func TestProcessFinalizesCancellationWhenCompletionIsRejected(t *testing.T) {
	store := newFakeStore()
	store.completeOK = false

	registry := registryWith(t, "test.cancel", handlers.HandlerFunc(func(context.Context, domain.Job) (json.RawMessage, error) {
		return json.RawMessage(`{"ok":true}`), nil
	}))
	runner := newRunner(t, store, registry, nil)

	runner.process(context.Background(), testJob("test.cancel", 1, 3))

	if store.cancelledCount() != 1 {
		t.Errorf("cancelled %d jobs, want 1", store.cancelledCount())
	}
	if store.completedCount() != 0 {
		t.Error("rejected completion was recorded")
	}
	if store.cancelCount() == 0 {
		t.Error("cancellation finalization was never attempted")
	}
}

func TestProcessCancellationWriteIsFenced(t *testing.T) {
	store := newFakeStore()
	store.cancelOK = false
	store.heartbeatCancel = true

	registry := registryWith(t, "test.cancel", handlers.HandlerFunc(func(ctx context.Context, _ domain.Job) (json.RawMessage, error) {
		<-ctx.Done()
		return nil, ctx.Err()
	}))
	runner := newRunner(t, store, registry, func(cfg *Config) {
		cfg.LeaseDuration = time.Second
		cfg.HeartbeatInterval = 5 * time.Millisecond
		cfg.HandlerTimeout = 2 * time.Second
	})

	runner.process(context.Background(), testJob("test.cancel", 1, 3))

	if store.cancelledCount() != 0 {
		t.Error("fenced cancellation was recorded")
	}
	if store.completedCount() != 0 || store.failedCount() != 0 {
		t.Error("a cancelled job was recorded as completed or failed")
	}
}

func TestProcessPermanentFailureDeadLetters(t *testing.T) {
	store := newFakeStore()
	registry := registryWith(t, "test.permanent", handlers.HandlerFunc(func(context.Context, domain.Job) (json.RawMessage, error) {
		return nil, retry.Permanent(errors.New("bad input"))
	}))
	runner := newRunner(t, store, registry, nil)
	job := testJob("test.permanent", 1, 5)

	runner.process(context.Background(), job)

	record, ok := store.failure(job.ID)
	if !ok {
		t.Fatal("permanent failure was not recorded")
	}
	if record.delay != 0 {
		t.Errorf("retry delay = %v, want 0 (dead-letter)", record.delay)
	}
	if record.cause != "bad input" {
		t.Errorf("cause = %q, want bad input", record.cause)
	}
}

func TestProcessExhaustedAttemptsDeadLetters(t *testing.T) {
	store := newFakeStore()
	registry := registryWith(t, "test.exhausted", handlers.HandlerFunc(func(context.Context, domain.Job) (json.RawMessage, error) {
		return nil, errors.New("still failing")
	}))
	runner := newRunner(t, store, registry, nil)
	job := testJob("test.exhausted", 3, 3)

	runner.process(context.Background(), job)

	record, ok := store.failure(job.ID)
	if !ok {
		t.Fatal("exhausted failure was not recorded")
	}
	if record.delay != 0 {
		t.Errorf("retry delay = %v, want 0 (dead-letter)", record.delay)
	}
}

func TestRetryDelayUsesFullJitterAndCap(t *testing.T) {
	runner := newRunner(t, newFakeStore(), handlers.New(), func(cfg *Config) {
		cfg.RetryBaseDelay = time.Second
		cfg.RetryMaxDelay = 3 * time.Second
	})
	runner.jitter = func() float64 { return 1 }

	for attempt, want := range map[int]time.Duration{
		1:  1 * time.Second,
		2:  2 * time.Second,
		3:  3 * time.Second,
		10: 3 * time.Second,
	} {
		if got := runner.retryDelay(attempt); got != want {
			t.Errorf("retryDelay(%d) = %v, want %v", attempt, got, want)
		}
	}

	runner.jitter = func() float64 { return 0 }
	if got := runner.retryDelay(2); got != 0 {
		t.Errorf("zero jitter retryDelay(2) = %v, want 0", got)
	}
}

func TestRecoveryLoopRunsPeriodically(t *testing.T) {
	store := newFakeStore()
	store.recoveryResult = queue.RecoveryResult{Requeued: 1, Dead: 1}
	runner := newRunner(t, store, handlers.New(), func(cfg *Config) {
		cfg.RecoveryInterval = 5 * time.Millisecond
	})

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- runner.Run(ctx) }()

	waitFor(t, time.Second, func() bool { return store.recoveryCount() >= 2 })
	stopRunner(t, cancel, done)
}

func TestRecoveryLoopSurvivesErrors(t *testing.T) {
	store := newFakeStore()
	store.recoveryErr = errors.New("database unavailable")
	runner := newRunner(t, store, handlers.New(), func(cfg *Config) {
		cfg.RecoveryInterval = 5 * time.Millisecond
	})

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- runner.Run(ctx) }()

	waitFor(t, time.Second, func() bool { return store.recoveryCount() >= 2 })
	stopRunner(t, cancel, done)
}

func TestProcessUnknownHandlerDeadLetters(t *testing.T) {
	store := newFakeStore()
	runner := newRunner(t, store, handlers.New(), nil)
	job := testJob("test.missing", 1, 3)

	runner.process(context.Background(), job)

	record, ok := store.failure(job.ID)
	if !ok {
		t.Fatal("job failure was not recorded")
	}
	if record.delay != 0 {
		t.Errorf("retry delay = %v, want 0 (dead-letter)", record.delay)
	}
	if record.cause == "" {
		t.Error("failure cause is empty")
	}
}

func TestProcessRecoversPanic(t *testing.T) {
	store := newFakeStore()
	registry := registryWith(t, "test.panic", handlers.HandlerFunc(func(context.Context, domain.Job) (json.RawMessage, error) {
		panic("boom")
	}))
	runner := newRunner(t, store, registry, nil)
	job := testJob("test.panic", 1, 3)

	runner.process(context.Background(), job)

	record, ok := store.failure(job.ID)
	if !ok {
		t.Fatal("panicking handler did not record a failure")
	}
	if record.cause != "handler panicked: boom" {
		t.Errorf("cause = %q, want handler panicked: boom", record.cause)
	}
}

func TestProcessHandlerTimeout(t *testing.T) {
	store := newFakeStore()
	registry := registryWith(t, "test.slow", handlers.HandlerFunc(func(ctx context.Context, _ domain.Job) (json.RawMessage, error) {
		<-ctx.Done()
		return nil, ctx.Err()
	}))
	runner := newRunner(t, store, registry, func(cfg *Config) {
		cfg.HandlerTimeout = 20 * time.Millisecond
	})
	job := testJob("test.slow", 1, 3)

	runner.process(context.Background(), job)

	record, ok := store.failure(job.ID)
	if !ok {
		t.Fatal("timed-out handler did not record a failure")
	}
	if record.cause == "" || !strings.Contains(record.cause, "timed out") {
		t.Errorf("cause = %q, want timeout message", record.cause)
	}
}

func TestProcessCompletionIsFenced(t *testing.T) {
	store := newFakeStore()
	store.completeOK = false
	registry := registryWith(t, "test.ok", handlers.HandlerFunc(func(context.Context, domain.Job) (json.RawMessage, error) {
		return json.RawMessage(`{"ok":true}`), nil
	}))
	runner := newRunner(t, store, registry, nil)

	runner.process(context.Background(), testJob("test.ok", 1, 3))

	if store.completedCount() != 0 {
		t.Error("fenced completion was recorded")
	}
}

func TestProcessFailureIsFenced(t *testing.T) {
	store := newFakeStore()
	store.failOK = false
	registry := registryWith(t, "test.fail", handlers.HandlerFunc(func(context.Context, domain.Job) (json.RawMessage, error) {
		return nil, errors.New("boom")
	}))
	runner := newRunner(t, store, registry, nil)

	runner.process(context.Background(), testJob("test.fail", 1, 3))

	if store.failedCount() != 0 {
		t.Error("fenced failure was recorded")
	}
}

func TestRunClaimsAndCompletesJobs(t *testing.T) {
	jobs := []*domain.Job{
		testJob("test.ok", 1, 3),
		testJob("test.ok", 1, 3),
		testJob("test.ok", 1, 3),
	}
	store := newFakeStore(jobs...)
	registry := registryWith(t, "test.ok", handlers.HandlerFunc(func(context.Context, domain.Job) (json.RawMessage, error) {
		return json.RawMessage(`{"ok":true}`), nil
	}))
	runner := newRunner(t, store, registry, func(cfg *Config) {
		cfg.Concurrency = 2
	})

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- runner.Run(ctx) }()

	waitFor(t, 2*time.Second, func() bool { return store.completedCount() == len(jobs) })
	stopRunner(t, cancel, done)

	if store.claimedCount() != len(jobs) {
		t.Errorf("claimed %d jobs, want %d", store.claimedCount(), len(jobs))
	}
}

func TestRunBoundsConcurrency(t *testing.T) {
	const (
		jobCount    = 10
		concurrency = 3
	)
	jobs := make([]*domain.Job, jobCount)
	for i := range jobs {
		jobs[i] = testJob("test.slow", 1, 3)
	}

	var (
		mu        sync.Mutex
		active    int
		maxActive int
	)
	registry := registryWith(t, "test.slow", handlers.HandlerFunc(func(context.Context, domain.Job) (json.RawMessage, error) {
		mu.Lock()
		active++
		if active > maxActive {
			maxActive = active
		}
		mu.Unlock()

		time.Sleep(15 * time.Millisecond)

		mu.Lock()
		active--
		mu.Unlock()
		return json.RawMessage(`{"ok":true}`), nil
	}))

	store := newFakeStore(jobs...)
	runner := newRunner(t, store, registry, func(cfg *Config) {
		cfg.Concurrency = concurrency
	})

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- runner.Run(ctx) }()

	waitFor(t, 5*time.Second, func() bool { return store.completedCount() == jobCount })
	stopRunner(t, cancel, done)

	mu.Lock()
	peak := maxActive
	mu.Unlock()
	if peak > concurrency {
		t.Errorf("peak concurrency = %d, want at most %d", peak, concurrency)
	}
	if peak < 2 {
		t.Errorf("peak concurrency = %d, want at least 2", peak)
	}
}

func TestRunGracefulShutdownDrainsInFlight(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	registry := registryWith(t, "test.block", handlers.HandlerFunc(func(ctx context.Context, _ domain.Job) (json.RawMessage, error) {
		close(started)
		select {
		case <-release:
			return json.RawMessage(`{"ok":true}`), nil
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}))

	store := newFakeStore(testJob("test.block", 1, 3))
	runner := newRunner(t, store, registry, func(cfg *Config) {
		cfg.HandlerTimeout = 5 * time.Second
		cfg.ShutdownTimeout = time.Second
	})

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- runner.Run(ctx) }()

	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("handler did not start")
	}

	cancel()
	select {
	case <-done:
		t.Fatal("Run() returned before the in-flight handler drained")
	case <-time.After(50 * time.Millisecond):
	}

	close(release)
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run() error = %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Run() did not return after the handler finished")
	}

	if store.completedCount() != 1 {
		t.Errorf("completed %d jobs, want 1", store.completedCount())
	}
}

func TestRunShutdownTimeoutCancelsHandlers(t *testing.T) {
	started := make(chan struct{})
	registry := registryWith(t, "test.block", handlers.HandlerFunc(func(ctx context.Context, _ domain.Job) (json.RawMessage, error) {
		close(started)
		<-ctx.Done()
		return nil, ctx.Err()
	}))

	store := newFakeStore(testJob("test.block", 0, 3))
	runner := newRunner(t, store, registry, func(cfg *Config) {
		cfg.HandlerTimeout = 5 * time.Second
		cfg.ShutdownTimeout = 30 * time.Millisecond
		cfg.RetryBaseDelay = 2 * time.Second
	})
	runner.jitter = func() float64 { return 1 }

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- runner.Run(ctx) }()

	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("handler did not start")
	}

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run() error = %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Run() did not return after forced cancellation")
	}

	jobID := store.firstClaimed()
	record, ok := store.failure(jobID)
	if !ok {
		t.Fatal("cancelled handler did not record a retry")
	}
	if record.delay != 2*time.Second {
		t.Errorf("retry delay = %v, want 2s", record.delay)
	}
}

func TestRunSurvivesClaimErrors(t *testing.T) {
	store := newFakeStore()
	store.claimErr = errors.New("database unavailable")
	runner := newRunner(t, store, handlers.DefaultRegistry(), nil)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- runner.Run(ctx) }()

	waitFor(t, 2*time.Second, func() bool { return store.claimCount() >= 3 })
	stopRunner(t, cancel, done)
}
