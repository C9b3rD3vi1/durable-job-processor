package config_test

import (
	"testing"
	"time"

	"github.com/C9b3rD3vi1/durable-job-processor/internal/config"
)

func setBaseEnv(t *testing.T) {
	t.Helper()
	t.Setenv("APP_ENV", "test")
	t.Setenv("LOG_LEVEL", "debug")
	t.Setenv("HTTP_ADDR", ":9090")
	t.Setenv("PORT", "")
	t.Setenv("DATABASE_URL", "postgres://jobs:jobs@localhost:5432/jobs?sslmode=disable")
	t.Setenv("DB_MAX_CONNS", "20")
	t.Setenv("DB_MIN_CONNS", "2")
	t.Setenv("DB_CONNECT_TIMEOUT", "3s")
	t.Setenv("AUTO_MIGRATE", "true")
	t.Setenv("SHUTDOWN_TIMEOUT", "20s")
	t.Setenv("WORKER_CONCURRENCY", "8")
	t.Setenv("POLL_INTERVAL", "750ms")
	t.Setenv("LEASE_DURATION", "90s")
	t.Setenv("HEARTBEAT_INTERVAL", "10s")
	t.Setenv("HANDLER_TIMEOUT", "45s")
	t.Setenv("RETRY_BASE_DELAY", "4s")
	t.Setenv("MAX_ATTEMPTS", "7")
	t.Setenv("MAX_PAYLOAD_BYTES", "1024")
}

func TestLoad(t *testing.T) {
	setBaseEnv(t)

	cfg, err := config.Load()
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}

	if cfg.Environment != "test" {
		t.Errorf("Environment = %q, want test", cfg.Environment)
	}
	if cfg.HTTPAddr != ":9090" {
		t.Errorf("HTTPAddr = %q, want :9090", cfg.HTTPAddr)
	}
	if cfg.DBMaxConns != 20 || cfg.DBMinConns != 2 {
		t.Errorf("DB pool = %d/%d, want 20/2", cfg.DBMaxConns, cfg.DBMinConns)
	}
	if cfg.DBConnectTimeout != 3*time.Second {
		t.Errorf("DBConnectTimeout = %v, want 3s", cfg.DBConnectTimeout)
	}
	if !cfg.AutoMigrate {
		t.Error("AutoMigrate = false, want true")
	}
	if cfg.WorkerConcurrency != 8 || cfg.PollInterval != 750*time.Millisecond {
		t.Errorf("worker config = %d/%v, want 8/750ms", cfg.WorkerConcurrency, cfg.PollInterval)
	}
	if cfg.LeaseDuration != 90*time.Second || cfg.HeartbeatInterval != 10*time.Second {
		t.Errorf("lease config = %v/%v, want 90s/10s", cfg.LeaseDuration, cfg.HeartbeatInterval)
	}
	if cfg.HandlerTimeout != 45*time.Second || cfg.RetryBaseDelay != 4*time.Second {
		t.Errorf("handler config = %v/%v, want 45s/4s", cfg.HandlerTimeout, cfg.RetryBaseDelay)
	}
	if cfg.MaxAttempts != 7 || cfg.MaxPayloadBytes != 1024 {
		t.Errorf("limits = %d/%d, want 7/1024", cfg.MaxAttempts, cfg.MaxPayloadBytes)
	}
	if got, want := cfg.RequestBodyLimit(), int64(1024+64*1024); got != want {
		t.Errorf("RequestBodyLimit() = %d, want %d", got, want)
	}
}

func TestLoadFromPort(t *testing.T) {
	setBaseEnv(t)
	t.Setenv("HTTP_ADDR", "")
	t.Setenv("PORT", "8181")

	cfg, err := config.Load()
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if cfg.HTTPAddr != ":8181" {
		t.Errorf("HTTPAddr = %q, want :8181", cfg.HTTPAddr)
	}
}

func TestLoadRequiresDatabaseURL(t *testing.T) {
	setBaseEnv(t)
	t.Setenv("DATABASE_URL", "")

	if _, err := config.Load(); err == nil {
		t.Fatal("Load() error = nil, want error")
	}
}

func TestLoadRejectsUnsafeLease(t *testing.T) {
	setBaseEnv(t)
	t.Setenv("LEASE_DURATION", "10s")
	t.Setenv("HEARTBEAT_INTERVAL", "10s")

	if _, err := config.Load(); err == nil {
		t.Fatal("Load() error = nil, want error")
	}
}

func TestLoadRejectsHandlerTimeoutBeyondLease(t *testing.T) {
	setBaseEnv(t)
	t.Setenv("HANDLER_TIMEOUT", "91s")

	if _, err := config.Load(); err == nil {
		t.Fatal("Load() error = nil, want error")
	}
}

func TestLoadRejectsNonPositiveRetryDelay(t *testing.T) {
	setBaseEnv(t)
	t.Setenv("RETRY_BASE_DELAY", "0s")

	if _, err := config.Load(); err == nil {
		t.Fatal("Load() error = nil, want error")
	}
}

func TestLoadRejectsInvalidLogLevel(t *testing.T) {
	setBaseEnv(t)
	t.Setenv("LOG_LEVEL", "verbose")

	if _, err := config.Load(); err == nil {
		t.Fatal("Load() error = nil, want error")
	}
}

func TestLoadRejectsInvalidPort(t *testing.T) {
	setBaseEnv(t)
	t.Setenv("HTTP_ADDR", "")
	t.Setenv("PORT", "not-a-port")

	if _, err := config.Load(); err == nil {
		t.Fatal("Load() error = nil, want error")
	}
}
