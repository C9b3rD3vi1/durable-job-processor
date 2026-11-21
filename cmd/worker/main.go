package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/C9b3rD3vi1/durable-job-processor/internal/config"
	"github.com/C9b3rD3vi1/durable-job-processor/internal/handlers"
	"github.com/C9b3rD3vi1/durable-job-processor/internal/observability"
	"github.com/C9b3rD3vi1/durable-job-processor/internal/postgres"
	"github.com/C9b3rD3vi1/durable-job-processor/internal/worker"
	"github.com/C9b3rD3vi1/durable-job-processor/migrations"
)

func main() {
	if err := run(); err != nil {
		slog.Error("worker stopped", "error", err)
		os.Exit(1)
	}
}

func run() error {
	cfg, err := config.Load()
	if err != nil {
		return fmt.Errorf("load configuration: %w", err)
	}

	logger, err := observability.NewLogger(cfg.Environment, cfg.LogLevel)
	if err != nil {
		return fmt.Errorf("create logger: %w", err)
	}
	metrics := observability.NewMetrics()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	pool, err := postgres.Connect(ctx, postgres.Config{
		URL:            cfg.DatabaseURL,
		MaxConns:       cfg.DBMaxConns,
		MinConns:       cfg.DBMinConns,
		ConnectTimeout: cfg.DBConnectTimeout,
	})
	if err != nil {
		return fmt.Errorf("connect to PostgreSQL: %w", err)
	}
	defer pool.Close()
	logger.Info("database connected", "max_conns", cfg.DBMaxConns)

	if cfg.AutoMigrate {
		applied, err := postgres.Migrate(ctx, pool, migrations.FS)
		if err != nil {
			return fmt.Errorf("run migrations: %w", err)
		}
		logger.Info("migrations checked", "applied", applied)
	}

	runner, err := worker.New(worker.Config{
		Store: postgres.NewJobRepository(pool),
		Registry: handlers.DefaultRegistryWithWebhookPolicy(handlers.WebhookEgressPolicy{
			AllowedHosts:    cfg.WebhookAllowedHosts,
			AllowPrivateIPs: cfg.WebhookAllowPrivateIPs,
		}),
		Logger:            logger,
		Metrics:           metrics,
		WorkerID:          workerID(),
		Concurrency:       cfg.WorkerConcurrency,
		PollInterval:      cfg.PollInterval,
		LeaseDuration:     cfg.LeaseDuration,
		HeartbeatInterval: cfg.HeartbeatInterval,
		HandlerTimeout:    cfg.HandlerTimeout,
		RetryBaseDelay:    cfg.RetryBaseDelay,
		RetryMaxDelay:     cfg.RetryMaxDelay,
		RecoveryInterval:  cfg.RecoveryInterval,
		ShutdownTimeout:   cfg.ShutdownTimeout,
	})
	if err != nil {
		return fmt.Errorf("create worker: %w", err)
	}

	if cfg.MetricsEnabled {
		go func() {
			if err := observability.ServeMetrics(ctx, cfg.MetricsAddr, cfg.MetricsToken, metrics); err != nil {
				logger.Error("metrics server stopped", "error", err)
			}
		}()
		go observability.PollPoolStats(ctx, 15*time.Second, func() observability.PoolStats {
			stat := pool.Stat()
			return observability.PoolStats{
				InUse:     int64(stat.AcquiredConns()),
				Idle:      int64(stat.IdleConns()),
				WaitCount: stat.EmptyAcquireCount(),
				MaxConns:  int64(stat.MaxConns()),
			}
		}, metrics)
		logger.Info("metrics endpoint enabled", "addr", cfg.MetricsAddr)
	}

	return runner.Run(ctx)
}

// workerID identifies this process in jobs.locked_by.
func workerID() string {
	hostname, err := os.Hostname()
	hostname = strings.TrimSpace(hostname)
	if err != nil || hostname == "" {
		hostname = "worker"
	}
	return fmt.Sprintf("%s-%d", hostname, os.Getpid())
}
