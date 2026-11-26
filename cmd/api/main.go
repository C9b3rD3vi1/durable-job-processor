package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/C9b3rD3vi1/durable-job-processor/internal/auth"
	"github.com/C9b3rD3vi1/durable-job-processor/internal/config"
	"github.com/C9b3rD3vi1/durable-job-processor/internal/handlers"
	"github.com/C9b3rD3vi1/durable-job-processor/internal/jobs"
	"github.com/C9b3rD3vi1/durable-job-processor/internal/observability"
	"github.com/C9b3rD3vi1/durable-job-processor/internal/postgres"
	httpapi "github.com/C9b3rD3vi1/durable-job-processor/internal/transport/http"
	"github.com/C9b3rD3vi1/durable-job-processor/migrations"
)

func main() {
	if err := run(); err != nil {
		slog.Error("api stopped", "error", err)
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

	rateLimitStore := postgres.NewRateLimitRepository(pool)
	logger.Info("rate limiter configured", "backend", "postgres", "api_replicas", cfg.APIReplicas)

	if cfg.AutoMigrate {
		applied, err := postgres.Migrate(ctx, pool, migrations.FS)
		if err != nil {
			return fmt.Errorf("run migrations: %w", err)
		}
		logger.Info("migrations checked", "applied", applied)
	}

	repository := postgres.NewJobRepository(pool)
	registry := handlers.DefaultRegistryWithWebhookPolicy(handlers.WebhookEgressPolicy{
		AllowedHosts:    cfg.WebhookAllowedHosts,
		AllowPrivateIPs: cfg.WebhookAllowPrivateIPs,
	})
	validator := jobs.NewValidator(registry, cfg.MaxPayloadBytes, cfg.MaxAttempts)
	service := jobs.NewService(repository, validator,
		jobs.WithMaxOutstandingJobs(cfg.TenantMaxOutstandingJobs),
		jobs.WithMetrics(metrics))
	authenticator := auth.NewAuthenticator(postgres.NewAPIKeyRepository(pool))

	router := httpapi.NewRouter(httpapi.Options{
		JobService:               service,
		Pinger:                   pool,
		Authenticator:            authenticator,
		Logger:                   logger,
		Metrics:                  metrics,
		MaxBodyBytes:             cfg.RequestBodyLimit(),
		RateLimitPerMinute:       cfg.TenantRateLimitPerMinute,
		SubmitRateLimitPerMinute: cfg.TenantSubmitRateLimitPerMinute,
		RateLimitStore:           rateLimitStore,
	})

	server := &http.Server{
		Addr:              cfg.HTTPAddr,
		Handler:           router,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       15 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	metricsEnabled := cfg.MetricsEnabled
	metricsErr := make(chan error, 1)
	var metricsCh <-chan error
	if metricsEnabled {
		metricsCh = metricsErr
		go func() {
			metricsErr <- observability.ServeMetrics(ctx, cfg.MetricsAddr, cfg.MetricsToken, metrics)
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

	serverErr := make(chan error, 1)
	go func() {
		logger.Info("api listening",
			"addr", cfg.HTTPAddr,
			"environment", cfg.Environment,
			"auto_migrate", cfg.AutoMigrate,
		)
		serverErr <- server.ListenAndServe()
	}()

	metricsStopped := false
	serverStopped := false
	var runErr error
	addErr := func(err error) {
		if err != nil {
			runErr = errors.Join(runErr, err)
		}
	}

	select {
	case err := <-serverErr:
		serverStopped = true
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			addErr(fmt.Errorf("serve HTTP: %w", err))
		}
		stop()
	case err := <-metricsCh:
		metricsStopped = true
		if err != nil {
			addErr(fmt.Errorf("metrics server: %w", err))
		}
		stop()
	case <-ctx.Done():
		logger.Info("shutdown requested", "timeout", cfg.ShutdownTimeout.String())
		stop()
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
	defer cancel()
	if err := server.Shutdown(shutdownCtx); err != nil {
		addErr(fmt.Errorf("graceful shutdown: %w", err))
	}

	if !serverStopped {
		select {
		case err := <-serverErr:
			serverStopped = true
			if err != nil && !errors.Is(err, http.ErrServerClosed) {
				addErr(fmt.Errorf("serve HTTP: %w", err))
			}
		case <-shutdownCtx.Done():
			addErr(fmt.Errorf("wait for HTTP server: %w", shutdownCtx.Err()))
		}
	}
	if metricsEnabled && !metricsStopped {
		select {
		case err := <-metricsCh:
			metricsStopped = true
			if err != nil {
				addErr(fmt.Errorf("metrics server: %w", err))
			} else {
				logger.Info("metrics listener stopped", "addr", cfg.MetricsAddr)
			}
		case <-shutdownCtx.Done():
			addErr(fmt.Errorf("wait for metrics server: %w", shutdownCtx.Err()))
		}
	}
	if runErr != nil {
		return runErr
	}
	logger.Info("api stopped")
	return nil
}
