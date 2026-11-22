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
	t.Setenv("RETRY_MAX_DELAY", "2m")
	t.Setenv("RECOVERY_INTERVAL", "45s")
	t.Setenv("MAX_ATTEMPTS", "7")
	t.Setenv("MAX_PAYLOAD_BYTES", "1024")
	t.Setenv("TENANT_MAX_OUTSTANDING_JOBS", "50")
	t.Setenv("TENANT_RATE_LIMIT_PER_MINUTE", "70")
	t.Setenv("TENANT_SUBMIT_RATE_LIMIT_PER_MINUTE", "20")
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
	if cfg.RetryMaxDelay != 2*time.Minute || cfg.RecoveryInterval != 45*time.Second {
		t.Errorf("retry/recovery config = %v/%v, want 2m/45s", cfg.RetryMaxDelay, cfg.RecoveryInterval)
	}
	if cfg.MaxAttempts != 7 || cfg.MaxPayloadBytes != 1024 {
		t.Errorf("limits = %d/%d, want 7/1024", cfg.MaxAttempts, cfg.MaxPayloadBytes)
	}
	if cfg.TenantMaxOutstandingJobs != 50 || cfg.TenantRateLimitPerMinute != 70 ||
		cfg.TenantSubmitRateLimitPerMinute != 20 {
		t.Errorf("tenant limits = %d/%d/%d, want 50/70/20",
			cfg.TenantMaxOutstandingJobs, cfg.TenantRateLimitPerMinute,
			cfg.TenantSubmitRateLimitPerMinute)
	}
	if got, want := cfg.RequestBodyLimit(), int64(1024+64*1024); got != want {
		t.Errorf("RequestBodyLimit() = %d, want %d", got, want)
	}
}

func TestLoadRateLimitDefaults(t *testing.T) {
	setBaseEnv(t)
	t.Setenv("TENANT_MAX_OUTSTANDING_JOBS", "")
	t.Setenv("TENANT_RATE_LIMIT_PER_MINUTE", "")
	t.Setenv("TENANT_SUBMIT_RATE_LIMIT_PER_MINUTE", "")

	cfg, err := config.Load()
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if cfg.TenantMaxOutstandingJobs != 1000 {
		t.Errorf("TenantMaxOutstandingJobs = %d, want 1000", cfg.TenantMaxOutstandingJobs)
	}
	if cfg.TenantRateLimitPerMinute != 600 {
		t.Errorf("TenantRateLimitPerMinute = %d, want 600", cfg.TenantRateLimitPerMinute)
	}
	if cfg.TenantSubmitRateLimitPerMinute != 120 {
		t.Errorf("TenantSubmitRateLimitPerMinute = %d, want 120", cfg.TenantSubmitRateLimitPerMinute)
	}
}

func TestLoadRejectsNegativeRateLimits(t *testing.T) {
	for _, key := range []string{
		"TENANT_MAX_OUTSTANDING_JOBS",
		"TENANT_RATE_LIMIT_PER_MINUTE",
		"TENANT_SUBMIT_RATE_LIMIT_PER_MINUTE",
	} {
		t.Run(key, func(t *testing.T) {
			setBaseEnv(t)
			t.Setenv(key, "-1")

			if _, err := config.Load(); err == nil {
				t.Fatalf("Load() error = nil, want an error for %s=-1", key)
			}
		})
	}
}

func TestLoadRejectsMultipleAPIReplicasWithRateLimits(t *testing.T) {
	setBaseEnv(t)
	t.Setenv("API_REPLICAS", "2")

	if _, err := config.Load(); err == nil {
		t.Fatal("Load() error = nil, want multi-replica rate limit constraint")
	}
}

func TestLoadAllowsMultipleAPIReplicasWhenRateLimitsDisabled(t *testing.T) {
	setBaseEnv(t)
	t.Setenv("API_REPLICAS", "3")
	t.Setenv("TENANT_RATE_LIMIT_PER_MINUTE", "0")
	t.Setenv("TENANT_SUBMIT_RATE_LIMIT_PER_MINUTE", "0")

	cfg, err := config.Load()
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if cfg.APIReplicas != 3 {
		t.Errorf("APIReplicas = %d, want 3", cfg.APIReplicas)
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

func TestLoadAllowsHandlerTimeoutBeyondLease(t *testing.T) {
	setBaseEnv(t)
	// Heartbeats renew the lease while the handler runs, so handler runtime is
	// no longer bounded by the lease duration.
	t.Setenv("LEASE_DURATION", "30s")
	t.Setenv("HEARTBEAT_INTERVAL", "10s")
	t.Setenv("HANDLER_TIMEOUT", "2m")

	if _, err := config.Load(); err != nil {
		t.Fatalf("Load() error = %v, want nil", err)
	}
}

func TestLoadRejectsNonPositiveRetryDelay(t *testing.T) {
	setBaseEnv(t)
	t.Setenv("RETRY_BASE_DELAY", "0s")

	if _, err := config.Load(); err == nil {
		t.Fatal("Load() error = nil, want error")
	}
}

func TestLoadRejectsNonPositiveRetryMaxDelay(t *testing.T) {
	setBaseEnv(t)
	t.Setenv("RETRY_MAX_DELAY", "0s")

	if _, err := config.Load(); err == nil {
		t.Fatal("Load() error = nil, want error")
	}
}

func TestLoadRejectsNonPositiveRecoveryInterval(t *testing.T) {
	setBaseEnv(t)
	t.Setenv("RECOVERY_INTERVAL", "0s")

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

func TestLoadWebhookEgressConfig(t *testing.T) {
	setBaseEnv(t)
	t.Setenv("WEBHOOK_ALLOWED_HOSTS", " hooks.example.com, .internal.example.org, , ")
	t.Setenv("WEBHOOK_ALLOW_PRIVATE_IPS", "true")

	cfg, err := config.Load()
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	want := []string{"hooks.example.com", ".internal.example.org"}
	if len(cfg.WebhookAllowedHosts) != len(want) {
		t.Fatalf("WebhookAllowedHosts = %v, want %v", cfg.WebhookAllowedHosts, want)
	}
	for i := range want {
		if cfg.WebhookAllowedHosts[i] != want[i] {
			t.Errorf("WebhookAllowedHosts[%d] = %q, want %q", i, cfg.WebhookAllowedHosts[i], want[i])
		}
	}
	if !cfg.WebhookAllowPrivateIPs {
		t.Error("WebhookAllowPrivateIPs = false, want true")
	}
}

func TestLoadWebhookEgressDefaults(t *testing.T) {
	setBaseEnv(t)
	cfg, err := config.Load()
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if len(cfg.WebhookAllowedHosts) != 0 {
		t.Errorf("WebhookAllowedHosts = %v, want empty", cfg.WebhookAllowedHosts)
	}
	if cfg.WebhookAllowPrivateIPs {
		t.Error("WebhookAllowPrivateIPs = true, want false")
	}
}

func TestLoadRejectsPrivateWebhookIPsInProduction(t *testing.T) {
	for _, environment := range []string{"production", "PROD", " live "} {
		t.Run(environment, func(t *testing.T) {
			setBaseEnv(t)
			t.Setenv("APP_ENV", environment)
			t.Setenv("WEBHOOK_ALLOW_PRIVATE_IPS", "true")

			if _, err := config.Load(); err == nil {
				t.Fatal("Load() error = nil, want private webhook IPs rejected in production")
			}
		})
	}
}

func TestLoadAllowsPrivateWebhookIPsOutsideProduction(t *testing.T) {
	setBaseEnv(t)
	t.Setenv("APP_ENV", "development")
	t.Setenv("WEBHOOK_ALLOW_PRIVATE_IPS", "true")

	cfg, err := config.Load()
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if !cfg.WebhookAllowPrivateIPs {
		t.Error("WebhookAllowPrivateIPs = false, want true")
	}
}

func TestLoadMetricsConfig(t *testing.T) {
	setBaseEnv(t)
	t.Setenv("METRICS_ENABLED", "true")
	t.Setenv("METRICS_ADDR", ":19090")
	t.Setenv("METRICS_TOKEN", "scrape-secret")

	cfg, err := config.Load()
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if !cfg.MetricsEnabled {
		t.Error("MetricsEnabled = false, want true")
	}
	if cfg.MetricsAddr != ":19090" || cfg.MetricsToken != "scrape-secret" {
		t.Errorf("metrics config = %q/%q, want :19090/scrape-secret", cfg.MetricsAddr, cfg.MetricsToken)
	}
}

func TestLoadMetricsDefaults(t *testing.T) {
	setBaseEnv(t)
	cfg, err := config.Load()
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if !cfg.MetricsEnabled || cfg.MetricsAddr != "127.0.0.1:9090" {
		t.Errorf("metrics defaults = %v/%q, want true/127.0.0.1:9090", cfg.MetricsEnabled, cfg.MetricsAddr)
	}
}

func TestLoadRejectsUnauthenticatedPublicMetrics(t *testing.T) {
	setBaseEnv(t)
	t.Setenv("METRICS_ADDR", ":19090")
	t.Setenv("METRICS_TOKEN", "")

	if _, err := config.Load(); err == nil {
		t.Fatal("Load() error = nil, want public metrics endpoint rejected")
	}
}
