// Package worker implements the durable job execution runtime. A runner starts
// a bounded set of claim loops; each loop atomically claims and executes one job
// at a time, so the configured concurrency is also the maximum number of
// in-flight handlers.
package worker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand"
	"strings"
	"time"

	"github.com/C9b3rD3vi1/durable-job-processor/internal/domain"
	"github.com/C9b3rD3vi1/durable-job-processor/internal/handlers"
	"github.com/C9b3rD3vi1/durable-job-processor/internal/observability"
	"github.com/C9b3rD3vi1/durable-job-processor/internal/queue"
	"github.com/C9b3rD3vi1/durable-job-processor/internal/retry"
)

const (
	defaultConcurrency       = 1
	defaultPollInterval      = time.Second
	defaultLeaseDuration     = 60 * time.Second
	defaultHeartbeatInterval = 15 * time.Second
	defaultHandlerTimeout    = 30 * time.Second
	defaultRetryBaseDelay    = 5 * time.Second
	defaultRetryMaxDelay     = 15 * time.Minute
	defaultRecoveryInterval  = 30 * time.Second
	defaultShutdownTimeout   = 15 * time.Second

	claimTimeout   = 10 * time.Second
	persistTimeout = 10 * time.Second
	maxErrorLength = 2000
	recoveryBatch  = 100
)

// Config configures a Runner. Store, Registry and WorkerID are required; the
// timing fields receive safe defaults when left at zero.
type Config struct {
	Store             queue.Store
	Registry          *handlers.Registry
	Logger            *slog.Logger
	Metrics           *observability.Metrics
	WorkerID          string
	Concurrency       int
	PollInterval      time.Duration
	LeaseDuration     time.Duration
	HeartbeatInterval time.Duration
	HandlerTimeout    time.Duration
	RetryBaseDelay    time.Duration
	RetryMaxDelay     time.Duration
	RecoveryInterval  time.Duration
	ShutdownTimeout   time.Duration
}

// Runner claims and executes jobs from a queue.Store.
type Runner struct {
	store             queue.Store
	registry          *handlers.Registry
	logger            *slog.Logger
	metrics           *observability.Metrics
	workerID          string
	concurrency       int
	pollInterval      time.Duration
	leaseDuration     time.Duration
	heartbeatInterval time.Duration
	handlerTimeout    time.Duration
	retryBaseDelay    time.Duration
	retryPolicy       retry.Policy
	recoveryInterval  time.Duration
	shutdownTimeout   time.Duration
	jitter            func() float64
}

// New validates cfg and builds a Runner.
func New(cfg Config) (*Runner, error) {
	if cfg.Store == nil {
		return nil, errors.New("worker store is required")
	}
	if cfg.Registry == nil {
		return nil, errors.New("worker handler registry is required")
	}
	if strings.TrimSpace(cfg.WorkerID) == "" {
		return nil, errors.New("worker id is required")
	}

	if cfg.Concurrency < 1 {
		cfg.Concurrency = defaultConcurrency
	}
	if cfg.PollInterval <= 0 {
		cfg.PollInterval = defaultPollInterval
	}
	if cfg.LeaseDuration <= 0 {
		cfg.LeaseDuration = defaultLeaseDuration
	}
	if cfg.HeartbeatInterval <= 0 {
		cfg.HeartbeatInterval = defaultHeartbeatInterval
	}
	if cfg.HeartbeatInterval >= cfg.LeaseDuration {
		return nil, fmt.Errorf("heartbeat interval %s must be less than lease duration %s",
			cfg.HeartbeatInterval, cfg.LeaseDuration)
	}
	if cfg.HandlerTimeout <= 0 {
		cfg.HandlerTimeout = defaultHandlerTimeout
	}
	if cfg.RetryBaseDelay <= 0 {
		cfg.RetryBaseDelay = defaultRetryBaseDelay
	}
	if cfg.RetryMaxDelay <= 0 {
		cfg.RetryMaxDelay = defaultRetryMaxDelay
	}
	if cfg.RecoveryInterval <= 0 {
		cfg.RecoveryInterval = defaultRecoveryInterval
	}
	if cfg.ShutdownTimeout <= 0 {
		cfg.ShutdownTimeout = defaultShutdownTimeout
	}

	logger := cfg.Logger
	if logger == nil {
		logger = slog.Default()
	}

	return &Runner{
		store:             cfg.Store,
		registry:          cfg.Registry,
		logger:            logger,
		metrics:           cfg.Metrics,
		workerID:          strings.TrimSpace(cfg.WorkerID),
		concurrency:       cfg.Concurrency,
		pollInterval:      cfg.PollInterval,
		leaseDuration:     cfg.LeaseDuration,
		heartbeatInterval: cfg.HeartbeatInterval,
		handlerTimeout:    cfg.HandlerTimeout,
		retryBaseDelay:    cfg.RetryBaseDelay,
		retryPolicy:       retry.NewPolicy(cfg.RetryBaseDelay, cfg.RetryMaxDelay),
		recoveryInterval:  cfg.RecoveryInterval,
		shutdownTimeout:   cfg.ShutdownTimeout,
		jitter:            rand.Float64,
	}, nil
}

// retryDelay returns the full-jitter backoff for the attempt that just failed.
// attempt is 1-based, matching jobs.attempts after a claim.
func (r *Runner) retryDelay(attempt int) time.Duration {
	return r.retryPolicy.DelayWithJitter(attempt, r.jitter())
}

// claim asks PostgreSQL for one due job. The claim statement is bounded so a
// stalled database cannot pin a loop forever.
func (r *Runner) claim(ctx context.Context) (*domain.Job, error) {
	claimCtx, cancel := context.WithTimeout(ctx, claimTimeout)
	defer cancel()
	return r.store.ClaimJob(claimCtx, r.workerID, r.leaseDuration)
}
