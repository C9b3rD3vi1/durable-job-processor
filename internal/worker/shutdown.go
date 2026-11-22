package worker

import (
	"context"
	"sync"
	"time"
)

// Run starts the configured number of claim loops and blocks until ctx is
// cancelled. Loops stop claiming immediately; in-flight handlers may finish for
// up to ShutdownTimeout. After that their contexts are cancelled and the runner
// waits one final bounded grace period for fenced persistence writes. Handlers
// that ignore cancellation are abandoned rather than blocking shutdown.
func (r *Runner) Run(ctx context.Context) error {
	// Handler contexts deliberately outlive the signal context so a graceful
	// shutdown can drain work already in progress.
	handlerCtx, cancelHandlers := context.WithCancel(context.WithoutCancel(ctx))
	defer cancelHandlers()

	var wg sync.WaitGroup
	for slot := 0; slot < r.concurrency; slot++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			r.loop(ctx, handlerCtx)
		}()
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		r.recoveryLoop(ctx)
	}()
	wg.Add(1)
	go func() {
		defer wg.Done()
		r.statsLoop(ctx)
	}()

	r.logger.Info("worker started",
		"worker_id", r.workerID,
		"concurrency", r.concurrency,
		"poll_interval", r.pollInterval.String(),
		"lease_duration", r.leaseDuration.String(),
		"heartbeat_interval", r.heartbeatInterval.String(),
		"handler_timeout", r.handlerTimeout.String(),
		"retry_base_delay", r.retryBaseDelay.String(),
		"retry_max_delay", r.retryPolicy.MaxDelay.String(),
		"recovery_interval", r.recoveryInterval.String(),
	)

	<-ctx.Done()
	r.logger.Info("worker stopping; draining in-flight jobs",
		"worker_id", r.workerID,
		"shutdown_timeout", r.shutdownTimeout.String(),
	)

	drained := make(chan struct{})
	go func() {
		wg.Wait()
		close(drained)
	}()

	timer := time.NewTimer(r.shutdownTimeout)
	defer timer.Stop()

	select {
	case <-drained:
		r.logger.Info("worker stopped", "worker_id", r.workerID)
		return nil
	case <-timer.C:
		r.logger.Warn("worker shutdown timeout exceeded; cancelling in-flight handlers",
			"worker_id", r.workerID,
		)
		cancelHandlers()

		// Give fenced persistence writes a bounded final grace period. A
		// handler that ignores cancellation has already been abandoned by
		// invokeHandlerBounded, so waiting forever would serve no purpose.
		finalTimer := time.NewTimer(persistTimeout)
		defer finalTimer.Stop()
		select {
		case <-drained:
			r.logger.Info("worker stopped after forced cancellation", "worker_id", r.workerID)
		case <-finalTimer.C:
			r.logger.Warn("worker shutdown grace exceeded; abandoning in-flight handlers",
				"worker_id", r.workerID,
				"grace", persistTimeout.String(),
			)
		}
		return nil
	}
}
