package postgres

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Config controls the PostgreSQL connection pool.
type Config struct {
	URL            string
	MaxConns       int32
	MinConns       int32
	ConnectTimeout time.Duration
}

// Connect creates a pool, verifies it with a ping, and closes it if the ping
// fails. The caller owns the returned pool and must call Close.
func Connect(ctx context.Context, cfg Config) (*pgxpool.Pool, error) {
	if cfg.URL == "" {
		return nil, fmt.Errorf("database URL is required")
	}

	poolConfig, err := pgxpool.ParseConfig(cfg.URL)
	if err != nil {
		return nil, fmt.Errorf("parse database URL: %w", err)
	}
	if cfg.MaxConns > 0 {
		poolConfig.MaxConns = cfg.MaxConns
	}
	if cfg.MinConns >= 0 {
		poolConfig.MinConns = cfg.MinConns
	}
	if cfg.ConnectTimeout > 0 {
		poolConfig.ConnConfig.ConnectTimeout = cfg.ConnectTimeout
	}

	pool, err := pgxpool.NewWithConfig(ctx, poolConfig)
	if err != nil {
		return nil, fmt.Errorf("create database pool: %w", err)
	}

	pingCtx := ctx
	cancel := func() {}
	if cfg.ConnectTimeout > 0 {
		pingCtx, cancel = context.WithTimeout(ctx, cfg.ConnectTimeout)
	}
	defer cancel()

	if err := pool.Ping(pingCtx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ping database: %w", err)
	}

	return pool, nil
}
