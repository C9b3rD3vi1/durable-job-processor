package worker

import (
	"context"
	"time"

	"github.com/C9b3rD3vi1/durable-job-processor/internal/observability"
)

// queueStatsInterval bounds how often a worker refreshes the shared backlog
// gauges. Every worker reads the same PostgreSQL view, so concurrent workers
// publish the same values rather than per-worker estimates.
const queueStatsInterval = 15 * time.Second

func (r *Runner) statsLoop(ctx context.Context) {
	ticker := time.NewTicker(queueStatsInterval)
	defer ticker.Stop()

	r.publishQueueStats(ctx)
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			r.publishQueueStats(ctx)
		}
	}
}

func (r *Runner) publishQueueStats(ctx context.Context) {
	statsCtx, cancel := context.WithTimeout(ctx, r.persistTimeout)
	defer cancel()

	stats, err := r.store.QueueStats(statsCtx)
	if err != nil {
		if ctx.Err() == nil {
			r.logger.Warn("queue stats", "worker_id", r.workerID, "error", err)
		}
		return
	}
	r.metrics.Set(observability.MetricQueueDepth, float64(stats.Depth))
	r.metrics.Set(observability.MetricQueueOldestDue, stats.OldestDueSeconds)
}
