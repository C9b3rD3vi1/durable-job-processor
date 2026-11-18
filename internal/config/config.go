package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/C9b3rD3vi1/durable-job-processor/internal/domain"
)

const (
	defaultHTTPAddr          = ":8080"
	defaultDBMaxConns        = 10
	defaultDBConnectTimeout  = 5 * time.Second
	defaultShutdownTimeout   = 15 * time.Second
	defaultWorkerConcurrency = 4
	defaultPollInterval      = time.Second
	defaultLeaseDuration     = 60 * time.Second
	defaultHeartbeatInterval = 15 * time.Second
	defaultHandlerTimeout    = 30 * time.Second
	defaultRetryBaseDelay    = 5 * time.Second
	defaultRetryMaxDelay     = 15 * time.Minute
	defaultRecoveryInterval  = 30 * time.Second
	defaultMaxPayloadBytes   = 256 * 1024

	defaultTenantMaxOutstandingJobs       = 1000
	defaultTenantRateLimitPerMinute       = 600
	defaultTenantSubmitRateLimitPerMinute = 120
)

// Config is the complete runtime configuration for the API and worker binaries.
// The API consumes the database, HTTP and logging settings; the worker also
// consumes the lease, heartbeat, retry and recovery settings.
type Config struct {
	Environment string
	LogLevel    string
	HTTPAddr    string

	DatabaseURL      string
	DBMaxConns       int32
	DBMinConns       int32
	DBConnectTimeout time.Duration
	AutoMigrate      bool

	ShutdownTimeout   time.Duration
	WorkerConcurrency int
	PollInterval      time.Duration
	LeaseDuration     time.Duration
	HeartbeatInterval time.Duration
	HandlerTimeout    time.Duration
	RetryBaseDelay    time.Duration
	RetryMaxDelay     time.Duration
	RecoveryInterval  time.Duration
	MaxAttempts       int
	MaxPayloadBytes   int64

	TenantMaxOutstandingJobs       int
	TenantRateLimitPerMinute       int
	TenantSubmitRateLimitPerMinute int
}

// Load reads configuration from environment variables and validates it.
func Load() (Config, error) {
	cfg := Config{
		Environment: envString("APP_ENV", "development"),
		LogLevel:    envString("LOG_LEVEL", "info"),
		DatabaseURL: strings.TrimSpace(os.Getenv("DATABASE_URL")),
	}

	if cfg.DatabaseURL == "" {
		return Config{}, fmt.Errorf("DATABASE_URL is required")
	}

	httpAddr := envString("HTTP_ADDR", "")
	if httpAddr == "" {
		port := envString("PORT", "8080")
		portNumber, err := strconv.Atoi(port)
		if err != nil || portNumber < 1 || portNumber > 65535 {
			return Config{}, fmt.Errorf("PORT must be a number between 1 and 65535")
		}
		httpAddr = ":" + port
	}
	cfg.HTTPAddr = httpAddr

	var err error
	if cfg.DBMaxConns, err = envInt32("DB_MAX_CONNS", defaultDBMaxConns); err != nil {
		return Config{}, err
	}
	if cfg.DBMinConns, err = envInt32("DB_MIN_CONNS", 0); err != nil {
		return Config{}, err
	}
	if cfg.DBConnectTimeout, err = envDuration("DB_CONNECT_TIMEOUT", defaultDBConnectTimeout); err != nil {
		return Config{}, err
	}
	if cfg.ShutdownTimeout, err = envDuration("SHUTDOWN_TIMEOUT", defaultShutdownTimeout); err != nil {
		return Config{}, err
	}
	if cfg.PollInterval, err = envDuration("POLL_INTERVAL", defaultPollInterval); err != nil {
		return Config{}, err
	}
	if cfg.LeaseDuration, err = envDuration("LEASE_DURATION", defaultLeaseDuration); err != nil {
		return Config{}, err
	}
	if cfg.HeartbeatInterval, err = envDuration("HEARTBEAT_INTERVAL", defaultHeartbeatInterval); err != nil {
		return Config{}, err
	}
	if cfg.HandlerTimeout, err = envDuration("HANDLER_TIMEOUT", defaultHandlerTimeout); err != nil {
		return Config{}, err
	}
	if cfg.RetryBaseDelay, err = envDuration("RETRY_BASE_DELAY", defaultRetryBaseDelay); err != nil {
		return Config{}, err
	}
	if cfg.RetryMaxDelay, err = envDuration("RETRY_MAX_DELAY", defaultRetryMaxDelay); err != nil {
		return Config{}, err
	}
	if cfg.RecoveryInterval, err = envDuration("RECOVERY_INTERVAL", defaultRecoveryInterval); err != nil {
		return Config{}, err
	}
	if cfg.WorkerConcurrency, err = envInt("WORKER_CONCURRENCY", defaultWorkerConcurrency); err != nil {
		return Config{}, err
	}
	if cfg.MaxAttempts, err = envInt("MAX_ATTEMPTS", domain.DefaultMaxAttempts); err != nil {
		return Config{}, err
	}
	if cfg.TenantMaxOutstandingJobs, err = envInt("TENANT_MAX_OUTSTANDING_JOBS", defaultTenantMaxOutstandingJobs); err != nil {
		return Config{}, err
	}
	if cfg.TenantRateLimitPerMinute, err = envInt("TENANT_RATE_LIMIT_PER_MINUTE", defaultTenantRateLimitPerMinute); err != nil {
		return Config{}, err
	}
	if cfg.TenantSubmitRateLimitPerMinute, err = envInt("TENANT_SUBMIT_RATE_LIMIT_PER_MINUTE", defaultTenantSubmitRateLimitPerMinute); err != nil {
		return Config{}, err
	}
	cfg.MaxPayloadBytes, err = envInt64("MAX_PAYLOAD_BYTES", defaultMaxPayloadBytes)
	if err != nil {
		return Config{}, err
	}
	if cfg.AutoMigrate, err = envBool("AUTO_MIGRATE", false); err != nil {
		return Config{}, err
	}

	if cfg.DBMaxConns < 1 {
		return Config{}, fmt.Errorf("DB_MAX_CONNS must be at least 1")
	}
	if cfg.DBMinConns < 0 || cfg.DBMinConns > cfg.DBMaxConns {
		return Config{}, fmt.Errorf("DB_MIN_CONNS must be between 0 and DB_MAX_CONNS")
	}
	if cfg.WorkerConcurrency < 1 {
		return Config{}, fmt.Errorf("WORKER_CONCURRENCY must be at least 1")
	}
	if cfg.PollInterval <= 0 {
		return Config{}, fmt.Errorf("POLL_INTERVAL must be positive")
	}
	if cfg.HeartbeatInterval <= 0 {
		return Config{}, fmt.Errorf("HEARTBEAT_INTERVAL must be positive")
	}
	if cfg.LeaseDuration <= cfg.HeartbeatInterval {
		return Config{}, fmt.Errorf("LEASE_DURATION must be greater than HEARTBEAT_INTERVAL")
	}
	if cfg.HandlerTimeout <= 0 {
		return Config{}, fmt.Errorf("HANDLER_TIMEOUT must be positive")
	}
	if cfg.RetryBaseDelay <= 0 {
		return Config{}, fmt.Errorf("RETRY_BASE_DELAY must be positive")
	}
	if cfg.RetryMaxDelay <= 0 {
		return Config{}, fmt.Errorf("RETRY_MAX_DELAY must be positive")
	}
	if cfg.RecoveryInterval <= 0 {
		return Config{}, fmt.Errorf("RECOVERY_INTERVAL must be positive")
	}
	if cfg.MaxAttempts < 1 || cfg.MaxAttempts > domain.MaxAttemptsLimit {
		return Config{}, fmt.Errorf("MAX_ATTEMPTS must be between 1 and %d", domain.MaxAttemptsLimit)
	}
	if cfg.MaxPayloadBytes < 1 {
		return Config{}, fmt.Errorf("MAX_PAYLOAD_BYTES must be positive")
	}
	if cfg.TenantMaxOutstandingJobs < 0 {
		return Config{}, fmt.Errorf("TENANT_MAX_OUTSTANDING_JOBS must not be negative")
	}
	if cfg.TenantRateLimitPerMinute < 0 {
		return Config{}, fmt.Errorf("TENANT_RATE_LIMIT_PER_MINUTE must not be negative")
	}
	if cfg.TenantSubmitRateLimitPerMinute < 0 {
		return Config{}, fmt.Errorf("TENANT_SUBMIT_RATE_LIMIT_PER_MINUTE must not be negative")
	}
	if cfg.ShutdownTimeout <= 0 {
		return Config{}, fmt.Errorf("SHUTDOWN_TIMEOUT must be positive")
	}
	switch strings.ToLower(cfg.LogLevel) {
	case "debug", "info", "warn", "error":
	default:
		return Config{}, fmt.Errorf("LOG_LEVEL must be one of debug, info, warn, error")
	}

	return cfg, nil
}

// RequestBodyLimit bounds the whole HTTP request, including JSON framing.
func (c Config) RequestBodyLimit() int64 {
	return c.MaxPayloadBytes + 64*1024
}

func envString(key, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(key)); value != "" {
		return value
	}
	return fallback
}

func envInt(key string, fallback int) (int, error) {
	raw := envString(key, "")
	if raw == "" {
		return fallback, nil
	}
	value, err := strconv.Atoi(raw)
	if err != nil {
		return 0, fmt.Errorf("%s must be an integer", key)
	}
	return value, nil
}

func envInt32(key string, fallback int) (int32, error) {
	raw := envString(key, "")
	if raw == "" {
		return int32(fallback), nil
	}
	value, err := strconv.ParseInt(raw, 10, 32)
	if err != nil {
		return 0, fmt.Errorf("%s must be a 32-bit integer", key)
	}
	return int32(value), nil
}

func envInt64(key string, fallback int64) (int64, error) {
	raw := envString(key, "")
	if raw == "" {
		return fallback, nil
	}
	value, err := strconv.ParseInt(raw, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("%s must be an integer", key)
	}
	return value, nil
}

func envDuration(key string, fallback time.Duration) (time.Duration, error) {
	raw := envString(key, "")
	if raw == "" {
		return fallback, nil
	}
	value, err := time.ParseDuration(raw)
	if err != nil {
		return 0, fmt.Errorf("%s must be a duration such as 500ms or 60s", key)
	}
	return value, nil
}

func envBool(key string, fallback bool) (bool, error) {
	raw := envString(key, "")
	if raw == "" {
		return fallback, nil
	}
	value, err := strconv.ParseBool(raw)
	if err != nil {
		return false, fmt.Errorf("%s must be a boolean", key)
	}
	return value, nil
}
