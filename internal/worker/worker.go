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
	"strings"
	"time"

	"github.com/C9b3rD3vi1/durable-job-processor/internal/domain"
	"github.com/C9b3rD3vi1/durable-job-processor/internal/handlers"
	"github.com/C9b3rD3vi1/durable-job-processor/internal/queue"
)

const (
	defaultConcurrency     = 1
	defaultPollInterval    = time.Second
	defaultLeaseDuration   = 60 * time.Second
	defaultHandlerTimeout  = 30 * time.Second
	defaultRetryBaseDelay  = 5 * time.Second
	defaultShutdownTimeout = 15 * time.Second

	claimTimeout   = 10 * time.Second
	persistTimeout = 10 * time.Second
	maxErrorLength = 2000
)

// Config configures a Runner. Store, Registry and WorkerID are required; the
// timing fields receive safe defaults when left at zero.
type Config struct {
	Store           queue.Store
	Registry        *handlers.Registry
	Logger          *slog.Logger
	WorkerID        string
	Concurrency     int
	PollInterval    time.Duration
	LeaseDuration   time.Duration
	HandlerTimeout  time.Duration
	RetryBaseDelay  time.Duration
	ShutdownTimeout time.Duration
}

// Runner claims and executes jobs from a queue.Store.
type Runner struct {
	store           queue.Store
	registry        *handlers.Registry
	logger          *slog.Logger
	workerID        string
	concurrency     int
	pollInterval    time.Duration
	leaseDuration   time.Duration
	handlerTimeout  time.Duration
	retryBaseDelay  time.Duration
	shutdownTimeout time.Duration
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
	if cfg.HandlerTimeout <= 0 {
		cfg.HandlerTimeout = defaultHandlerTimeout
	}
	if cfg.HandlerTimeout > cfg.LeaseDuration {
		return nil, fmt.Errorf("handler timeout %s exceeds lease duration %s", cfg.HandlerTimeout, cfg.LeaseDuration)
	}
	if cfg.RetryBaseDelay <= 0 {
		cfg.RetryBaseDelay = defaultRetryBaseDelay
	}
	if cfg.ShutdownTimeout <= 0 {
		cfg.ShutdownTimeout = defaultShutdownTimeout
	}

	logger := cfg.Logger
	if logger == nil {
		logger = slog.Default()
	}

	return &Runner{
		store:           cfg.Store,
		registry:        cfg.Registry,
		logger:          logger,
		workerID:        strings.TrimSpace(cfg.WorkerID),
		concurrency:     cfg.Concurrency,
		pollInterval:    cfg.PollInterval,
		leaseDuration:   cfg.LeaseDuration,
		handlerTimeout:  cfg.HandlerTimeout,
		retryBaseDelay:  cfg.RetryBaseDelay,
		shutdownTimeout: cfg.ShutdownTimeout,
	}, nil
}

// claim asks PostgreSQL for one due job. The claim statement is bounded so a
// stalled database cannot pin a loop forever.
func (r *Runner) claim(ctx context.Context) (*domain.Job, error) {
	claimCtx, cancel := context.WithTimeout(ctx, claimTimeout)
	defer cancel()
	return r.store.ClaimJob(claimCtx, r.workerID, r.leaseDuration)
}
