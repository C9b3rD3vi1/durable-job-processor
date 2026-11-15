package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/C9b3rD3vi1/durable-job-processor/internal/config"
	"github.com/C9b3rD3vi1/durable-job-processor/internal/observability"
	"github.com/C9b3rD3vi1/durable-job-processor/internal/postgres"
	"github.com/C9b3rD3vi1/durable-job-processor/migrations"
)

func main() {
	if err := run(); err != nil {
		slog.Error("migration failed", "error", err)
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

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

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

	applied, err := postgres.Migrate(ctx, pool, migrations.FS)
	if err != nil {
		return err
	}
	logger.Info("migrations applied", "count", applied)
	return nil
}
